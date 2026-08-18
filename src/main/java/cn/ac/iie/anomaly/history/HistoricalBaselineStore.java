package cn.ac.iie.anomaly.history;

import cn.ac.iie.anomaly.config.AppConfig;
import com.tdunning.math.stats.TDigest;

import java.io.Serializable;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.TreeMap;

/**
 * 【导读：type=2 的“长期记忆”】
 *
 * 这个类不是数据库，而是 TaskManager JVM 里的内存状态；Flink Checkpoint 会把它序列化保存。
 * 它同时维护两种不同粒度的历史：
 * - Context History：很多 IP 对共享的分布画像，使用按天 t-digest 桶；
 * - Pair History：某个 srcIp+dstIp+protocol 自己的 EMA。
 *
 * 只有一个 5 分钟窗口完整分析成功后，Source 才调用 apply() 更新这里，所以失败窗口不会留下半截历史。
 *
 * Long-lived historical state used by anomalyType=2.
 *
 * Context history:
 *   contextKey -> date bucket -> t-digest(bytes/pkts), retained as time buckets.
 * Pair history:
 *   pairHash -> EMA(bytes/pkts), bounded by event-time TTL and a hard max-entry cap.
 *
 * The store is mutated only after a complete five-minute Doris window has been analyzed.
 */
public final class HistoricalBaselineStore {
    private final boolean contextEnabled;
    private final boolean pairEnabled;
    private final int contextRetentionDays;
    private final int contextMinDays;
    private final double contextCompression;
    private final int pairTtlDays;
    private final int pairMaxEntries;
    private final double pairEmaAlpha;
    private final int pairMinSamples;

    private final Map<String, NavigableMap<Long, ContextBucket>> contextBuckets = new HashMap<>();
    // insertion order is explicitly refreshed on update; plain get() does not mutate ordering.
    private final LinkedHashMap<Long, PairHistory> pairs = new LinkedHashMap<>();

    public HistoricalBaselineStore(AppConfig config) {
        this.contextEnabled = config.getBoolean("history.context.enabled", true);
        this.pairEnabled = config.getBoolean("history.pair.enabled", true);
        this.contextRetentionDays = Math.max(1, config.getInt("history.context.retention.days", 7));
        this.contextMinDays = Math.max(1, config.getInt("history.context.min.days", 2));
        this.contextCompression = Math.max(20d, config.getDouble("history.context.tdigest.compression", 100d));
        this.pairTtlDays = Math.max(1, config.getInt("history.pair.ttl.days", 7));
        this.pairMaxEntries = Math.max(1000, config.getInt("history.pair.max.entries", 200000));
        this.pairEmaAlpha = clampAlpha(config.getDouble("history.pair.ema.alpha", 0.10d));
        this.pairMinSamples = Math.max(1, config.getInt("history.pair.min.samples", 3));
    }

    public int getPairMinSamples() {
        return pairMinSamples;
    }

    /**
     * 合并最近若干“历史日期桶”的 t-digest，得到一个 Context 的 P50/P90/高分位。
     * currentDate 当天不参与，避免当前待检测窗口反过来影响自己的历史基线。
     */
    public ContextStats contextStats(String contextKey, LocalDate currentDate,
                                     double bytesQuantile, double pktsQuantile) {
        if (!contextEnabled) {
            return ContextStats.empty();
        }
        NavigableMap<Long, ContextBucket> byDay = contextBuckets.get(contextKey);
        if (byDay == null || byDay.isEmpty()) {
            return ContextStats.empty();
        }

        long currentEpochDay = currentDate.toEpochDay();
        long minEpochDay = currentDate.minusDays(contextRetentionDays).toEpochDay();
        TDigest bytes = TDigest.createMergingDigest(contextCompression);
        TDigest pkts = TDigest.createMergingDigest(contextCompression);
        int days = 0;

        for (Map.Entry<Long, ContextBucket> e : byDay.subMap(minEpochDay, true, currentEpochDay, false).entrySet()) {
            ContextBucket bucket = e.getValue();
            if (bucket.count <= 0L) {
                continue;
            }
            bytes.add(bucket.bytesDigest);
            pkts.add(bucket.pktsDigest);
            days++;
        }
        if (days == 0) {
            return ContextStats.empty();
        }

        return new ContextStats(
                days,
                days >= contextMinDays,
                q(bytes, 0.50d), q(pkts, 0.50d),
                q(bytes, 0.90d), q(pkts, 0.90d),
                q(bytes, bytesQuantile), q(pkts, pktsQuantile));
    }

    /** 查询 Pair EMA；如果超过事件时间 TTL 没再学习，则视为不存在。 */
    public PairStats pairStats(long pairHash, long referenceEventMillis) {
        if (!pairEnabled) {
            return null;
        }
        PairHistory history = pairs.get(pairHash);
        if (history == null) {
            return null;
        }
        long cutoff = referenceEventMillis - pairTtlDays * 86400000L;
        if (history.lastSeenMillis < cutoff) {
            return null;
        }
        return new PairStats(history.emaBytes, history.emaPkts, history.sampleCount, history.lastSeenMillis);
    }

    /** Apply one fully analyzed window atomically from the source's checkpoint critical section. */
    public void apply(WindowUpdate update, LocalDateTime windowEnd, ZoneId zoneId) {
        long referenceMillis = windowEnd.atZone(zoneId).toInstant().toEpochMilli();
        pruneContext(windowEnd.toLocalDate());
        prunePairs(referenceMillis);

        if (contextEnabled) for (ContextUpdate context : update.contextUpdates) {
            NavigableMap<Long, ContextBucket> byDay = contextBuckets.computeIfAbsent(
                    context.contextKey, k -> new TreeMap<Long, ContextBucket>());
            ContextBucket bucket = byDay.get(context.epochDay);
            if (bucket == null) {
                bucket = new ContextBucket(
                        TDigest.createMergingDigest(contextCompression),
                        TDigest.createMergingDigest(contextCompression), 0L);
                byDay.put(context.epochDay, bucket);
            }
            bucket.bytesDigest.add(context.bytesDigest);
            bucket.pktsDigest.add(context.pktsDigest);
            bucket.count += context.count;
        }

        if (pairEnabled) for (PairSample sample : update.pairSamples) {
            // remove + put 是为了把“刚更新的 Pair”移动到 LinkedHashMap 尾部；
            // 超过 max.entries 时优先淘汰最久没有被更新的 Pair。
            PairHistory history = pairs.remove(sample.pairHash);
            if (history == null) {
                history = new PairHistory(sample.bytes, sample.pkts, 1L, sample.eventTimeMillis);
            } else {
                // EMA = old*(1-alpha) + current*alpha。默认 alpha=0.1，意味着历史占 90%，新样本占 10%。
                history.emaBytes = ema(history.emaBytes, sample.bytes, pairEmaAlpha);
                history.emaPkts = ema(history.emaPkts, sample.pkts, pairEmaAlpha);
                history.sampleCount++;
                history.lastSeenMillis = Math.max(history.lastSeenMillis, sample.eventTimeMillis);
            }
            pairs.put(sample.pairHash, history);
            evictToCapacity();
        }
    }

    /** 把内存里的 Pair EMA 转成可被 Flink Operator State 保存的轻量 POJO。 */
    public List<PairSnapshot> snapshotPairs() {
        List<PairSnapshot> snapshots = new ArrayList<>(pairs.size());
        for (Map.Entry<Long, PairHistory> e : pairs.entrySet()) {
            PairHistory h = e.getValue();
            snapshots.add(new PairSnapshot(e.getKey(), h.emaBytes, h.emaPkts, h.sampleCount, h.lastSeenMillis));
        }
        return snapshots;
    }

    /** 把 Context t-digest 序列化为 byte[]，供 Flink Checkpoint 保存。 */
    public List<ContextSnapshot> snapshotContexts() {
        List<ContextSnapshot> snapshots = new ArrayList<>();
        for (Map.Entry<String, NavigableMap<Long, ContextBucket>> outer : contextBuckets.entrySet()) {
            for (Map.Entry<Long, ContextBucket> inner : outer.getValue().entrySet()) {
                ContextBucket bucket = inner.getValue();
                snapshots.add(new ContextSnapshot(
                        outer.getKey(), inner.getKey(), bucket.count,
                        TDigestSerde.encode(bucket.bytesDigest), TDigestSerde.encode(bucket.pktsDigest)));
            }
        }
        return snapshots;
    }

    public void restorePair(PairSnapshot snapshot) {
        pairs.put(snapshot.pairHash, new PairHistory(
                snapshot.emaBytes, snapshot.emaPkts, snapshot.sampleCount, snapshot.lastSeenMillis));
        evictToCapacity();
    }

    public void restoreContext(ContextSnapshot snapshot) {
        NavigableMap<Long, ContextBucket> byDay = contextBuckets.computeIfAbsent(
                snapshot.contextKey, k -> new TreeMap<Long, ContextBucket>());
        byDay.put(snapshot.epochDay, new ContextBucket(
                TDigestSerde.decode(snapshot.bytesDigest),
                TDigestSerde.decode(snapshot.pktsDigest),
                snapshot.count));
    }

    public int pairSize() { return pairs.size(); }

    public int contextBucketSize() {
        int size = 0;
        for (NavigableMap<Long, ContextBucket> m : contextBuckets.values()) {
            size += m.size();
        }
        return size;
    }

    private void pruneContext(LocalDate referenceDate) {
        long minEpochDay = referenceDate.minusDays(contextRetentionDays).toEpochDay();
        Iterator<Map.Entry<String, NavigableMap<Long, ContextBucket>>> outer = contextBuckets.entrySet().iterator();
        while (outer.hasNext()) {
            NavigableMap<Long, ContextBucket> byDay = outer.next().getValue();
            byDay.headMap(minEpochDay, false).clear();
            if (byDay.isEmpty()) {
                outer.remove();
            }
        }
    }

    private void prunePairs(long referenceMillis) {
        long cutoff = referenceMillis - pairTtlDays * 86400000L;
        Iterator<Map.Entry<Long, PairHistory>> it = pairs.entrySet().iterator();
        while (it.hasNext()) {
            if (it.next().getValue().lastSeenMillis < cutoff) {
                it.remove();
            }
        }
    }

    private void evictToCapacity() {
        while (pairs.size() > pairMaxEntries) {
            Iterator<Long> it = pairs.keySet().iterator();
            if (!it.hasNext()) {
                return;
            }
            it.next();
            it.remove();
        }
    }

    private static long q(TDigest digest, double quantile) {
        double value = digest.quantile(quantile);
        if (!Double.isFinite(value) || value <= 0d) {
            return 0L;
        }
        return value >= Long.MAX_VALUE ? Long.MAX_VALUE : Math.round(value);
    }

    private static double ema(double oldValue, long current, double alpha) {
        return oldValue * (1d - alpha) + ((double) current) * alpha;
    }

    private static double clampAlpha(double alpha) {
        if (alpha <= 0d || alpha > 1d) {
            throw new IllegalArgumentException("history.pair.ema.alpha must be in (0,1]");
        }
        return alpha;
    }

    private static final class ContextBucket {
        private final TDigest bytesDigest;
        private final TDigest pktsDigest;
        private long count;

        private ContextBucket(TDigest bytesDigest, TDigest pktsDigest, long count) {
            this.bytesDigest = bytesDigest;
            this.pktsDigest = pktsDigest;
            this.count = count;
        }
    }

    private static final class PairHistory {
        private double emaBytes;
        private double emaPkts;
        private long sampleCount;
        private long lastSeenMillis;

        private PairHistory(double emaBytes, double emaPkts, long sampleCount, long lastSeenMillis) {
            this.emaBytes = emaBytes;
            this.emaPkts = emaPkts;
            this.sampleCount = sampleCount;
            this.lastSeenMillis = lastSeenMillis;
        }
    }

    public static final class PairStats {
        private final double emaBytes;
        private final double emaPkts;
        private final long sampleCount;
        private final long lastSeenMillis;

        private PairStats(double emaBytes, double emaPkts, long sampleCount, long lastSeenMillis) {
            this.emaBytes = emaBytes;
            this.emaPkts = emaPkts;
            this.sampleCount = sampleCount;
            this.lastSeenMillis = lastSeenMillis;
        }

        public double getEmaBytes() { return emaBytes; }
        public double getEmaPkts() { return emaPkts; }
        public long getSampleCount() { return sampleCount; }
        public long getLastSeenMillis() { return lastSeenMillis; }
    }

    public static final class WindowUpdate {
        private final List<ContextUpdate> contextUpdates = new ArrayList<>();
        private final List<PairSample> pairSamples = new ArrayList<>();

        public void addContext(ContextUpdate update) { contextUpdates.add(update); }
        public void addPairSample(PairSample sample) { pairSamples.add(sample); }
        public List<ContextUpdate> getContextUpdates() { return contextUpdates; }
        public List<PairSample> getPairSamples() { return pairSamples; }
    }

    public static final class ContextUpdate {
        private final String contextKey;
        private final long epochDay;
        private final TDigest bytesDigest;
        private final TDigest pktsDigest;
        private final long count;

        public ContextUpdate(String contextKey, long epochDay, TDigest bytesDigest, TDigest pktsDigest, long count) {
            this.contextKey = contextKey;
            this.epochDay = epochDay;
            this.bytesDigest = bytesDigest;
            this.pktsDigest = pktsDigest;
            this.count = count;
        }
    }

    public static final class PairSample {
        private final long pairHash;
        private final long bytes;
        private final long pkts;
        private final long eventTimeMillis;

        public PairSample(long pairHash, long bytes, long pkts, long eventTimeMillis) {
            this.pairHash = pairHash;
            this.bytes = bytes;
            this.pkts = pkts;
            this.eventTimeMillis = eventTimeMillis;
        }
    }

    /** Flink operator-state POJO. */
    public static final class PairSnapshot implements Serializable {
        private static final long serialVersionUID = 1L;
        public long pairHash;
        public double emaBytes;
        public double emaPkts;
        public long sampleCount;
        public long lastSeenMillis;

        public PairSnapshot() {}

        public PairSnapshot(long pairHash, double emaBytes, double emaPkts, long sampleCount, long lastSeenMillis) {
            this.pairHash = pairHash;
            this.emaBytes = emaBytes;
            this.emaPkts = emaPkts;
            this.sampleCount = sampleCount;
            this.lastSeenMillis = lastSeenMillis;
        }
    }

    /** Flink operator-state POJO with compact serialized t-digests. */
    public static final class ContextSnapshot implements Serializable {
        private static final long serialVersionUID = 1L;
        public String contextKey;
        public long epochDay;
        public long count;
        public byte[] bytesDigest;
        public byte[] pktsDigest;

        public ContextSnapshot() {}

        public ContextSnapshot(String contextKey, long epochDay, long count, byte[] bytesDigest, byte[] pktsDigest) {
            this.contextKey = contextKey;
            this.epochDay = epochDay;
            this.count = count;
            this.bytesDigest = bytesDigest;
            this.pktsDigest = pktsDigest;
        }
    }
}

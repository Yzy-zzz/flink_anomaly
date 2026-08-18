package cn.ac.iie.anomaly.rule;

import cn.ac.iie.anomaly.history.ContextStats;
import cn.ac.iie.anomaly.history.HistoricalBaselineStore;
import cn.ac.iie.anomaly.model.MetricRecord;
import com.tdunning.math.stats.TDigest;

import java.io.Serializable;
import java.time.LocalDate;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.PriorityQueue;

/**
 * 【导读：type=2 的“省内存累计器”】
 *
 * 5 分钟窗口可能有海量记录，本类绝不保存全量明细，只保存两类摘要：
 * 1) 每个 Context 的 t-digest，用于近似计算 P50/P90/P99.9 等分位数；
 * 2) 固定容量的最小堆，只留下 totalBytes 最大的候选记录。
 *
 * 因此 candidate.capacity=20000 的含义不是“最多读 2 万行”，而是“读全量，但只保留最大的 2 万条候选”。
 *
 * Bounded accumulator for one five-minute query.
 * It keeps per-context quantile sketches plus a fixed-size heap of the largest records.
 * No full IP-pair hash table is retained.
 */
public final class LargeTrafficAccumulator implements Serializable {
    private static final long serialVersionUID = 1L;

    private final double compression;
    private final int candidateCapacity;
    // 最小堆：堆顶永远是当前候选里 bytes 最小的那条。
    // 新记录比堆顶更大时，淘汰堆顶，从而最终保留窗口内最大的 N 条。
    private final PriorityQueue<Candidate> candidates;
    private final Map<String, ContextAccumulator> contexts = new HashMap<>();
    private final Map<String, ContextStats> historicalStats = new HashMap<>();
    private long rowCount;

    public LargeTrafficAccumulator(double compression, int candidateCapacity) {
        this.compression = compression;
        this.candidateCapacity = candidateCapacity;
        this.candidates = new PriorityQueue<>(candidateCapacity, new CandidateBytesComparator());
    }

    /** 把一条记录加入 Context 分布摘要，并尝试进入“最大流量候选集合”。 */
    public void add(MetricRecord record, String contextKey, ContextStats history) {
        ContextAccumulator context = contexts.get(contextKey);
        if (context == null) {
            context = new ContextAccumulator(compression);
            contexts.put(contextKey, context);
            historicalStats.put(contextKey, history == null ? ContextStats.empty() : history);
        }
        context.add(record, history);
        rowCount++;

        Candidate candidate = new Candidate(record, contextKey);
        if (candidateCapacity <= 0) {
            return;
        }
        if (candidates.size() < candidateCapacity) {
            candidates.offer(candidate);
        } else if (record.totalBytes() > candidates.peek().record.totalBytes()) {
            candidates.poll();
            candidates.offer(candidate);
        }
    }

    public List<Candidate> candidateSnapshot() {
        return new ArrayList<>(candidates);
    }

    public ContextStats historicalStats(String contextKey) {
        ContextStats stats = historicalStats.get(contextKey);
        return stats == null ? ContextStats.empty() : stats;
    }

    /**
     * 当历史 Context 还不成熟时，用当前窗口自身的分布临时兜底。
     * 这就是冷启动阶段 baseline 可能来自 CURRENT_CONTEXT_P50 的来源。
     */
    public ContextStats currentStats(String contextKey, double bytesQuantile, double pktsQuantile) {
        ContextAccumulator context = contexts.get(contextKey);
        if (context == null || context.count == 0L) {
            return ContextStats.empty();
        }
        return new ContextStats(
                0, true,
                q(context.rawBytes, 0.50d), q(context.rawPkts, 0.50d),
                q(context.rawBytes, 0.90d), q(context.rawPkts, 0.90d),
                q(context.rawBytes, bytesQuantile), q(context.rawPkts, pktsQuantile));
    }

    public List<HistoricalBaselineStore.ContextUpdate> contextUpdates(LocalDate date) {
        List<HistoricalBaselineStore.ContextUpdate> updates = new ArrayList<>(contexts.size());
        long epochDay = date.toEpochDay();
        for (Map.Entry<String, ContextAccumulator> e : contexts.entrySet()) {
            ContextAccumulator c = e.getValue();
            if (c.count <= 0L) {
                continue;
            }
            updates.add(new HistoricalBaselineStore.ContextUpdate(
                    e.getKey(), epochDay, c.historyBytes, c.historyPkts, c.count));
        }
        return updates;
    }

    public long getRowCount() {
        return rowCount;
    }

    public int getContextCount() {
        return contexts.size();
    }

    private static long q(TDigest digest, double quantile) {
        double value = digest.quantile(quantile);
        if (!Double.isFinite(value) || value <= 0d) {
            return 0L;
        }
        return value >= Long.MAX_VALUE ? Long.MAX_VALUE : Math.round(value);
    }

    public static final class Candidate implements Serializable {
        private static final long serialVersionUID = 1L;
        private final MetricRecord record;
        private final String contextKey;

        private Candidate(MetricRecord record, String contextKey) {
            this.record = record;
            this.contextKey = contextKey;
        }

        public MetricRecord getRecord() { return record; }
        public String getContextKey() { return contextKey; }
    }

    private static final class CandidateBytesComparator implements Comparator<Candidate>, Serializable {
        private static final long serialVersionUID = 1L;
        @Override
        public int compare(Candidate left, Candidate right) {
            return Long.compare(left.record.totalBytes(), right.record.totalBytes());
        }
    }

    private static final class ContextAccumulator implements Serializable {
        private static final long serialVersionUID = 1L;
        // raw*：保留当前窗口原始分布，用于“当前窗口兜底阈值”。
        private final TDigest rawBytes;
        private final TDigest rawPkts;
        // history*：准备写回长期 Context 历史的分布。极端值会先封顶，防止抬高未来基线。
        private final TDigest historyBytes;
        private final TDigest historyPkts;
        private long count;

        private ContextAccumulator(double compression) {
            rawBytes = TDigest.createMergingDigest(compression);
            rawPkts = TDigest.createMergingDigest(compression);
            historyBytes = TDigest.createMergingDigest(compression);
            historyPkts = TDigest.createMergingDigest(compression);
        }

        private void add(MetricRecord record, ContextStats history) {
            long bytes = record.totalBytes();
            long pkts = record.totalPkts();
            rawBytes.add(bytes);
            rawPkts.add(pkts);

            // Do not let a huge current outlier freely raise the normal historical context.
            // Once enough historical days exist, cap updates at the previous historical high quantile.
            long historyBytesValue = bytes;
            long historyPktsValue = pkts;
            if (history != null && history.getContributingDays() > 0) {
                if (history.getThresholdBytes() > 0L) {
                    historyBytesValue = Math.min(bytes, history.getThresholdBytes());
                }
                if (history.getThresholdPkts() > 0L) {
                    historyPktsValue = Math.min(pkts, history.getThresholdPkts());
                }
            }
            historyBytes.add(historyBytesValue);
            historyPkts.add(historyPktsValue);
            count++;
        }
    }
}

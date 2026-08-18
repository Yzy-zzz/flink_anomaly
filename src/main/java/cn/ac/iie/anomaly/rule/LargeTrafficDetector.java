package cn.ac.iie.anomaly.rule;

import cn.ac.iie.anomaly.config.AppConfig;
import cn.ac.iie.anomaly.history.ContextStats;
import cn.ac.iie.anomaly.history.HistoricalBaselineStore;
import cn.ac.iie.anomaly.model.AlertRecord;
import cn.ac.iie.anomaly.model.MetricRecord;
import cn.ac.iie.anomaly.util.ConnectionKey;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * 【导读：anomalyType=2 的核心判定公式都在这里】
 *
 * 可以把 detect() 看成 4 步：
 * 1) 选择基线：成熟 Pair 用 Pair EMA，否则用 Context P50；
 * 2) 计算真正阈值：max(Context 高分位, baseline * multiplier)；
 * 3) 按 bytes + (pkts 或 extremeBytes) 的组合规则判异常；
 * 4) 决定当前 Pair 是否以及如何写回 EMA。
 *
 * 本版本特别加入 CAPPED_BOOTSTRAP：冷启动 Pair 即使第一次就异常，也允许学习“被 Context 高分位封顶后的值”，
 * 解决异常 Pair 永远 sampleCount=0、永远退回 Context baseline 的学习饿死问题。
 */
public final class LargeTrafficDetector implements Serializable {
    private static final long serialVersionUID = 1L;

    private static final String BASELINE_PAIR_EMA = "PAIR_EMA";
    private static final String BASELINE_HISTORICAL_CONTEXT = "HISTORICAL_CONTEXT_P50";
    private static final String BASELINE_CURRENT_CONTEXT = "CURRENT_CONTEXT_P50";
    private static final String CONTEXT_HISTORICAL = "HISTORICAL";
    private static final String CONTEXT_CURRENT_WINDOW = "CURRENT_WINDOW";
    private static final String LEARNING_NORMAL = "NORMAL";
    private static final String LEARNING_CAPPED_BOOTSTRAP = "CAPPED_BOOTSTRAP";
    private static final String LEARNING_SKIP_ANOMALOUS_MATURE = "SKIP_ANOMALOUS_MATURE";
    private static final String LEARNING_SKIP_ANOMALOUS = "SKIP_ANOMALOUS";

    private final double bytesQuantile;
    private final double pktsQuantile;
    private final double bytesBaselineMultiplier;
    private final double pktsBaselineMultiplier;
    private final double extremeMultiplier;
    private final boolean bootstrapAnomalyCappedLearningEnabled;
    private final String logId;
    private final String vendorCode;
    private final String remark1;
    private final String remark2;

    public LargeTrafficDetector(AppConfig config) {
        this.bytesQuantile = config.getDouble("rule.large.bytes.quantile", 0.999d);
        this.pktsQuantile = config.getDouble("rule.large.pkts.quantile", 0.999d);
        this.bytesBaselineMultiplier = config.getDouble("rule.large.bytes.baseline.multiplier", 4d);
        this.pktsBaselineMultiplier = config.getDouble("rule.large.pkts.baseline.multiplier", 4d);
        this.extremeMultiplier = config.getDouble("rule.large.extreme.multiplier", 2d);
        this.bootstrapAnomalyCappedLearningEnabled = config.getBoolean(
                "history.pair.bootstrap.anomaly.capped.learning.enabled", true);
        this.logId = config.get("alert.logid", "STATIC_LOG_ID");
        this.vendorCode = config.get("alert.vendorCode", "V001");
        this.remark1 = config.get("alert.remark1", "");
        this.remark2 = config.get("alert.remark2", "");
    }

    /** 对当前 5 分钟窗口保留下来的大流量候选做最终 type=2 判断。 */
    public DetectionResult detect(LargeTrafficAccumulator acc, HistoricalBaselineStore historyStore) {
        List<LargeTrafficAccumulator.Candidate> candidates = acc.candidateSnapshot();
        if (candidates.isEmpty()) {
            return new DetectionResult(Collections.<AlertRecord>emptyList(),
                    Collections.<HistoricalBaselineStore.PairSample>emptyList());
        }

        Map<String, AlertWithBytes> bestAlertByPair = new HashMap<>();
        Set<Long> anomalousPairHashes = new HashSet<>();
        Map<Long, BootstrapLearningCap> bootstrapLearningCaps = new HashMap<>();
        int pairMinSamples = historyStore.getPairMinSamples();

        for (LargeTrafficAccumulator.Candidate candidate : candidates) {
            MetricRecord record = candidate.getRecord();
            ContextSelection contextSelection = effectiveContext(acc, candidate.getContextKey());
            ContextStats context = contextSelection.stats;
            if (context.getThresholdBytes() <= 0L) {
                continue;
            }

            String pairKey = record.pairKey();
            long pairHash = ConnectionKey.hash64(pairKey);
            // Pair = srcIp + dstIp + protocol。历史里只保存 64 位 hash，减少内存。
            HistoricalBaselineStore.PairStats pair = historyStore.pairStats(pairHash, record.getCollectTimestamp());
            long pairSampleCount = pair == null ? 0L : pair.getSampleCount();
            // 默认至少学习 3 个样本后，才认为 Pair 自己的 EMA 足够可信。
            boolean pairMature = pair != null && pairSampleCount >= pairMinSamples;

            long baselineBytes;
            long baselinePkts;
            String baselineSource;
            // 基线选择优先级：Pair EMA > 历史 Context P50 > 当前窗口 Context P50。
            if (pairMature) {
                baselineBytes = positiveRound(pair.getEmaBytes(), context.getP50Bytes());
                baselinePkts = positiveRound(pair.getEmaPkts(), context.getP50Pkts());
                baselineSource = BASELINE_PAIR_EMA;
            } else {
                baselineBytes = context.getP50Bytes();
                baselinePkts = context.getP50Pkts();
                baselineSource = contextSelection.historicalUsable
                        ? BASELINE_HISTORICAL_CONTEXT : BASELINE_CURRENT_CONTEXT;
            }

            long currentBytes = record.totalBytes();
            long currentPkts = record.totalPkts();
            // 真正的 bytes 报警阈值不是 baseline*4 这么简单，而是二者取大：
            // max(Context P99.9, baselineBytes * 4)。这样可避免 baseline 很小时阈值低得离谱。
            double bytesThreshold = Math.max((double) context.getThresholdBytes(),
                    baselineBytes * bytesBaselineMultiplier);
            double pktsThreshold = Math.max((double) context.getThresholdPkts(),
                    baselinePkts * pktsBaselineMultiplier);
            double extremeBytesThreshold = ((double) context.getThresholdBytes()) * extremeMultiplier;

            // 三个布尔量就是告警证据中 bytes_anomaly / pkts_anomaly / extreme_bytes 的来源。
            boolean bytesAnomaly = currentBytes > bytesThreshold;
            boolean pktsAnomaly = currentPkts > pktsThreshold;
            boolean extremeBytes = currentBytes > extremeBytesThreshold;

            // 最终 type=2 条件：bytes 必须异常，并且 pkts 也异常，或者 bytes 已经极端异常。
            if (bytesAnomaly && (pktsAnomaly || extremeBytes)) {
                anomalousPairHashes.add(pairHash);

                String pairLearningMode;
                Long pairLearningCapBytes = null;
                Long pairLearningCapPkts = null;
                if (!pairMature && bootstrapAnomalyCappedLearningEnabled) {
                    // 冷启动修复：允许异常 Pair 学习，但学习值不得超过 Context 高分位阈值。
                    // 例：当前 5MB、Context P99.9=1MB，则 EMA 只学习 1MB，不直接学习 5MB。
                    long capBytes = learningCap(context.getThresholdBytes(), baselineBytes, currentBytes);
                    long capPkts = learningCap(context.getThresholdPkts(), baselinePkts, currentPkts);
                    bootstrapLearningCaps.put(pairHash, new BootstrapLearningCap(capBytes, capPkts));
                    pairLearningMode = LEARNING_CAPPED_BOOTSTRAP;
                    pairLearningCapBytes = capBytes;
                    pairLearningCapPkts = capPkts;
                } else if (pairMature) {
                    // Pair 已经成熟后，异常样本继续完全跳过，避免攻击把稳定 EMA 越抬越高。
                    pairLearningMode = LEARNING_SKIP_ANOMALOUS_MATURE;
                } else {
                    pairLearningMode = LEARNING_SKIP_ANOMALOUS;
                }

                // 把“为什么报警”所需的全部中间值封装起来，AlertRecord 会原样写入 anomalyDetail。
                AlertRecord.LargeTrafficEvidence evidence = new AlertRecord.LargeTrafficEvidence(
                        baselineSource,
                        baselineBytes,
                        baselinePkts,
                        pairSampleCount,
                        pairMinSamples,
                        contextSelection.historicalUsable ? CONTEXT_HISTORICAL : CONTEXT_CURRENT_WINDOW,
                        contextSelection.historicalDays,
                        context.getP50Bytes(),
                        context.getP50Pkts(),
                        context.getP90Bytes(),
                        context.getP90Pkts(),
                        bytesQuantile,
                        pktsQuantile,
                        context.getThresholdBytes(),
                        context.getThresholdPkts(),
                        bytesBaselineMultiplier,
                        pktsBaselineMultiplier,
                        bytesThreshold,
                        pktsThreshold,
                        extremeMultiplier,
                        extremeBytesThreshold,
                        bytesAnomaly,
                        pktsAnomaly,
                        extremeBytes,
                        pairLearningMode,
                        pairLearningCapBytes,
                        pairLearningCapPkts);
                AlertRecord alert = AlertRecord.largeTraffic(
                        logId, vendorCode, remark1, remark2, record, evidence);
                AlertWithBytes old = bestAlertByPair.get(pairKey);
                if (old == null || currentBytes > old.currentBytes) {
                    bestAlertByPair.put(pairKey, new AlertWithBytes(alert, currentBytes));
                }
            }
        }

        List<AlertRecord> alerts = new ArrayList<>(bestAlertByPair.size());
        for (AlertWithBytes value : bestAlertByPair.values()) {
            alerts.add(value.alert);
        }

        // Normal candidates learn their real value. An anomalous pair is still protected from
        // contaminating a mature EMA. During bootstrap only, however, an anomalous pair may learn
        // a value capped at the effective context high-quantile threshold so it can eventually
        // reach pairMinSamples instead of being permanently stuck on the context fallback.
        List<LargeTrafficAccumulator.Candidate> chronological = new ArrayList<>(candidates);
        chronological.sort(new Comparator<LargeTrafficAccumulator.Candidate>() {
            @Override
            public int compare(LargeTrafficAccumulator.Candidate a, LargeTrafficAccumulator.Candidate b) {
                return Long.compare(a.getRecord().getCollectTimestamp(), b.getRecord().getCollectTimestamp());
            }
        });

        List<HistoricalBaselineStore.PairSample> pairSamples = new ArrayList<>();
        for (LargeTrafficAccumulator.Candidate candidate : chronological) {
            MetricRecord record = candidate.getRecord();
            long pairHash = ConnectionKey.hash64(record.pairKey());
            if (!anomalousPairHashes.contains(pairHash)) {
                pairSamples.add(new HistoricalBaselineStore.PairSample(
                        pairHash, record.totalBytes(), record.totalPkts(), record.getCollectTimestamp()));
                continue;
            }

            BootstrapLearningCap cap = bootstrapLearningCaps.get(pairHash);
            if (cap == null) {
                // Mature anomalous pairs, or bootstrap learning disabled: preserve the old behavior.
                continue;
            }
            pairSamples.add(new HistoricalBaselineStore.PairSample(
                    pairHash,
                    Math.min(record.totalBytes(), cap.bytes),
                    Math.min(record.totalPkts(), cap.pkts),
                    record.getCollectTimestamp()));
        }

        return new DetectionResult(alerts, pairSamples);
    }

    /**
     * Context 冷启动选择：历史达到 min.days 就用历史；否则临时使用当前窗口分布。
     */
    private ContextSelection effectiveContext(LargeTrafficAccumulator acc, String contextKey) {
        ContextStats historical = acc.historicalStats(contextKey);
        if (historical.isUsable()) {
            return new ContextSelection(historical, true, historical.getContributingDays());
        }
        // Cold start: use this window's own distribution until enough historical dates exist.
        return new ContextSelection(
                acc.currentStats(contextKey, bytesQuantile, pktsQuantile),
                false,
                historical.getContributingDays());
    }

    private static long learningCap(long contextThreshold, long baseline, long current) {
        if (contextThreshold > 0L) {
            return contextThreshold;
        }
        if (baseline > 0L) {
            return baseline;
        }
        return Math.max(0L, current);
    }

    private static long positiveRound(double value, long fallback) {
        if (!Double.isFinite(value) || value <= 0d) {
            return fallback;
        }
        return value >= Long.MAX_VALUE ? Long.MAX_VALUE : Math.round(value);
    }

    private static final class ContextSelection {
        private final ContextStats stats;
        private final boolean historicalUsable;
        private final int historicalDays;

        private ContextSelection(ContextStats stats, boolean historicalUsable, int historicalDays) {
            this.stats = stats;
            this.historicalUsable = historicalUsable;
            this.historicalDays = historicalDays;
        }
    }

    private static final class BootstrapLearningCap {
        private final long bytes;
        private final long pkts;

        private BootstrapLearningCap(long bytes, long pkts) {
            this.bytes = bytes;
            this.pkts = pkts;
        }
    }

    private static final class AlertWithBytes {
        private final AlertRecord alert;
        private final long currentBytes;
        private AlertWithBytes(AlertRecord alert, long currentBytes) {
            this.alert = alert;
            this.currentBytes = currentBytes;
        }
    }

    public static final class DetectionResult {
        private final List<AlertRecord> alerts;
        private final List<HistoricalBaselineStore.PairSample> pairSamples;

        private DetectionResult(List<AlertRecord> alerts, List<HistoricalBaselineStore.PairSample> pairSamples) {
            this.alerts = alerts;
            this.pairSamples = pairSamples;
        }

        public List<AlertRecord> getAlerts() { return alerts; }
        public List<HistoricalBaselineStore.PairSample> getPairSamples() { return pairSamples; }
    }
}

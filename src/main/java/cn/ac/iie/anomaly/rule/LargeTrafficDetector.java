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


    private final double bytesQuantile;
    private final double pktsQuantile;
    private final double bytesBaselineMultiplier;
    private final double pktsBaselineMultiplier;
    private final double extremeMultiplier;
    private final boolean bootstrapAnomalyCappedLearningEnabled;
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
            ContextStats context = effectiveContext(acc, candidate.getContextKey());
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
            // 基线选择优先级：成熟 Pair 用自身 EMA，否则使用当前生效的 Context P50。
            if (pairMature) {
                baselineBytes = positiveRound(pair.getEmaBytes(), context.getP50Bytes());
                baselinePkts = positiveRound(pair.getEmaPkts(), context.getP50Pkts());
            } else {
                baselineBytes = context.getP50Bytes();
                baselinePkts = context.getP50Pkts();
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

            // 三个布尔量用于内部判定；对外告警只汇总成 anomaly_reason，不再逐个输出调试字段。
            boolean bytesAnomaly = currentBytes > bytesThreshold;
            boolean pktsAnomaly = currentPkts > pktsThreshold;
            boolean extremeBytes = currentBytes > extremeBytesThreshold;

            // 最终 type=2 条件：bytes 必须异常，并且 pkts 也异常，或者 bytes 已经极端异常。
            if (bytesAnomaly && (pktsAnomaly || extremeBytes)) {
                anomalousPairHashes.add(pairHash);

                if (!pairMature && bootstrapAnomalyCappedLearningEnabled) {
                    // 冷启动修复：允许异常 Pair 学习，但学习值不得超过 Context 高分位阈值。
                    // 例：当前 5MB、Context P99.9=1MB，则 EMA 只学习 1MB，不直接学习 5MB。
                    long capBytes = learningCap(context.getThresholdBytes(), baselineBytes, currentBytes);
                    long capPkts = learningCap(context.getThresholdPkts(), baselinePkts, currentPkts);
                    bootstrapLearningCaps.put(pairHash, new BootstrapLearningCap(capBytes, capPkts));
                }
                // 成熟异常 Pair（或关闭 bootstrap 学习时）不写 bootstrapLearningCaps，后面的学习阶段会跳过。

                AlertRecord.LargeTrafficEvidence evidence = new AlertRecord.LargeTrafficEvidence(
                        baselineBytes,
                        baselinePkts,
                        bytesThreshold,
                        pktsThreshold,
                        extremeBytesThreshold,
                        pktsAnomaly);
                AlertRecord alert = AlertRecord.largeTraffic(
                        vendorCode, remark1, remark2, record, evidence);
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
    private ContextStats effectiveContext(LargeTrafficAccumulator acc, String contextKey) {
        ContextStats historical = acc.historicalStats(contextKey);
        if (historical.isUsable()) {
            return historical;
        }
        // Cold start: use this window's own distribution until enough historical dates exist.
        return acc.currentStats(contextKey, bytesQuantile, pktsQuantile);
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

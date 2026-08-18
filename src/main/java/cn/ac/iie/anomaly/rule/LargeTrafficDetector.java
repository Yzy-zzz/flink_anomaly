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

/** Large-traffic detection using historical context sketches + bounded pair EMA history. */
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
            HistoricalBaselineStore.PairStats pair = historyStore.pairStats(pairHash, record.getCollectTimestamp());
            long pairSampleCount = pair == null ? 0L : pair.getSampleCount();
            boolean pairMature = pair != null && pairSampleCount >= pairMinSamples;

            long baselineBytes;
            long baselinePkts;
            String baselineSource;
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
            double bytesThreshold = Math.max((double) context.getThresholdBytes(),
                    baselineBytes * bytesBaselineMultiplier);
            double pktsThreshold = Math.max((double) context.getThresholdPkts(),
                    baselinePkts * pktsBaselineMultiplier);
            double extremeBytesThreshold = ((double) context.getThresholdBytes()) * extremeMultiplier;

            boolean bytesAnomaly = currentBytes > bytesThreshold;
            boolean pktsAnomaly = currentPkts > pktsThreshold;
            boolean extremeBytes = currentBytes > extremeBytesThreshold;

            if (bytesAnomaly && (pktsAnomaly || extremeBytes)) {
                anomalousPairHashes.add(pairHash);

                String pairLearningMode;
                Long pairLearningCapBytes = null;
                Long pairLearningCapPkts = null;
                if (!pairMature && bootstrapAnomalyCappedLearningEnabled) {
                    long capBytes = learningCap(context.getThresholdBytes(), baselineBytes, currentBytes);
                    long capPkts = learningCap(context.getThresholdPkts(), baselinePkts, currentPkts);
                    bootstrapLearningCaps.put(pairHash, new BootstrapLearningCap(capBytes, capPkts));
                    pairLearningMode = LEARNING_CAPPED_BOOTSTRAP;
                    pairLearningCapBytes = capBytes;
                    pairLearningCapPkts = capPkts;
                } else if (pairMature) {
                    pairLearningMode = LEARNING_SKIP_ANOMALOUS_MATURE;
                } else {
                    pairLearningMode = LEARNING_SKIP_ANOMALOUS;
                }

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

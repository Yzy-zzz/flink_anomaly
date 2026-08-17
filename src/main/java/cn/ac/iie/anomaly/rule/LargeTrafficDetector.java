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

    private final double bytesQuantile;
    private final double pktsQuantile;
    private final double bytesBaselineMultiplier;
    private final double pktsBaselineMultiplier;
    private final double extremeMultiplier;
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

        for (LargeTrafficAccumulator.Candidate candidate : candidates) {
            MetricRecord record = candidate.getRecord();
            ContextStats context = effectiveContext(acc, candidate.getContextKey());
            if (context.getThresholdBytes() <= 0L) {
                continue;
            }

            String pairKey = record.pairKey();
            long pairHash = ConnectionKey.hash64(pairKey);
            HistoricalBaselineStore.PairStats pair = historyStore.pairStats(pairHash, record.getCollectTimestamp());

            long baselineBytes;
            long baselinePkts;
            if (pair != null && pair.getSampleCount() >= historyStore.getPairMinSamples()) {
                baselineBytes = positiveRound(pair.getEmaBytes(), context.getP50Bytes());
                baselinePkts = positiveRound(pair.getEmaPkts(), context.getP50Pkts());
            } else {
                baselineBytes = context.getP50Bytes();
                baselinePkts = context.getP50Pkts();
            }

            long currentBytes = record.totalBytes();
            long currentPkts = record.totalPkts();
            double bytesThreshold = Math.max((double) context.getThresholdBytes(),
                    baselineBytes * bytesBaselineMultiplier);
            double pktsThreshold = Math.max((double) context.getThresholdPkts(),
                    baselinePkts * pktsBaselineMultiplier);

            boolean bytesAnomaly = currentBytes > bytesThreshold;
            boolean pktsAnomaly = currentPkts > pktsThreshold;
            boolean extremeBytes = currentBytes > context.getThresholdBytes() * extremeMultiplier;

            if (bytesAnomaly && (pktsAnomaly || extremeBytes)) {
                anomalousPairHashes.add(pairHash);
                AlertRecord alert = AlertRecord.largeTraffic(
                        logId, vendorCode, remark1, remark2, record, baselineBytes, baselinePkts);
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

        // Pair EMA learns only from bounded heavy candidates and never learns from a pair
        // that was anomalous anywhere in this five-minute window.
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
            if (anomalousPairHashes.contains(pairHash)) {
                continue;
            }
            pairSamples.add(new HistoricalBaselineStore.PairSample(
                    pairHash, record.totalBytes(), record.totalPkts(), record.getCollectTimestamp()));
        }

        return new DetectionResult(alerts, pairSamples);
    }

    private ContextStats effectiveContext(LargeTrafficAccumulator acc, String contextKey) {
        ContextStats historical = acc.historicalStats(contextKey);
        if (historical.isUsable()) {
            return historical;
        }
        // Cold start: use this window's own distribution until enough historical dates exist.
        return acc.currentStats(contextKey, bytesQuantile, pktsQuantile);
    }

    private static long positiveRound(double value, long fallback) {
        if (!Double.isFinite(value) || value <= 0d) {
            return fallback;
        }
        return value >= Long.MAX_VALUE ? Long.MAX_VALUE : Math.round(value);
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

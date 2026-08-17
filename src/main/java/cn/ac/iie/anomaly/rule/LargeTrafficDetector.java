package cn.ac.iie.anomaly.rule;

import cn.ac.iie.anomaly.config.AppConfig;
import cn.ac.iie.anomaly.model.AlertRecord;
import cn.ac.iie.anomaly.model.MetricRecord;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/** Finalizes a bounded LargeTrafficAccumulator into type=2 alerts. */
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

    public List<AlertRecord> detect(LargeTrafficAccumulator acc) {
        List<AlertRecord> alerts = new ArrayList<>();
        if (acc == null || acc.getRowCount() == 0L) {
            return alerts;
        }

        double pBytes = safeQuantile(acc.getBytesDigest().quantile(bytesQuantile));
        double pPkts = safeQuantile(acc.getPktsDigest().quantile(pktsQuantile));
        double medianBytes = safeQuantile(acc.getBytesDigest().quantile(0.5d));
        double medianPkts = safeQuantile(acc.getPktsDigest().quantile(0.5d));

        // Deduplication is only over the fixed-size candidate heap, not the full IP-pair cardinality.
        Map<String, MetricRecord> bestByPair = new HashMap<>();
        for (MetricRecord candidate : acc.candidateSnapshot()) {
            String key = candidate.pairKey();
            MetricRecord old = bestByPair.get(key);
            if (old == null || candidate.totalBytes() > old.totalBytes()) {
                bestByPair.put(key, candidate);
            }
        }

        for (Map.Entry<String, MetricRecord> entry : bestByPair.entrySet()) {
            String key = entry.getKey();
            MetricRecord candidate = entry.getValue();
            long currentBytes = candidate.totalBytes();
            long currentPkts = candidate.totalPkts();

            if (currentBytes <= pBytes) {
                continue;
            }

            long estimatedCount = acc.getCountSketch().estimate(key);
            long baselineBytes = estimateExcludingCurrent(
                    acc.getByteSumSketch().estimate(key), estimatedCount, currentBytes, medianBytes);
            long baselinePkts = estimateExcludingCurrent(
                    acc.getPktSumSketch().estimate(key), estimatedCount, currentPkts, medianPkts);

            double bytesThreshold = Math.max(pBytes, baselineBytes * bytesBaselineMultiplier);
            double pktsThreshold = Math.max(pPkts, baselinePkts * pktsBaselineMultiplier);

            boolean bytesAnomaly = currentBytes > bytesThreshold;
            boolean pktsAnomaly = currentPkts > pktsThreshold;
            boolean extremeBytes = currentBytes > pBytes * extremeMultiplier;

            if (bytesAnomaly && (pktsAnomaly || extremeBytes)) {
                alerts.add(AlertRecord.largeTraffic(
                        logId, vendorCode, remark1, remark2, candidate, baselineBytes, baselinePkts));
            }
        }
        return alerts;
    }

    private static long estimateExcludingCurrent(long estimatedSum, long estimatedCount,
                                                 long current, double globalMedian) {
        if (estimatedCount <= 1L) {
            return clampDoubleToLong(globalMedian);
        }
        long adjusted = estimatedSum <= current ? 0L : estimatedSum - current;
        long baseline = adjusted / Math.max(1L, estimatedCount - 1L);
        return baseline <= 0L ? clampDoubleToLong(globalMedian) : baseline;
    }

    private static double safeQuantile(double value) {
        return Double.isFinite(value) && value >= 0d ? value : 0d;
    }

    private static long clampDoubleToLong(double value) {
        if (!Double.isFinite(value) || value <= 0d) {
            return 0L;
        }
        if (value >= Long.MAX_VALUE) {
            return Long.MAX_VALUE;
        }
        return Math.round(value);
    }
}

package cn.ac.iie.anomaly.model;

import java.io.Serializable;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * 最终告警对象。Jackson 会通过 getter 把它序列化成 JSON。
 * anomalyDetail 使用 LinkedHashMap，是为了让日志字段顺序稳定、便于人工阅读。
 */
public class AlertRecord implements Serializable {
    private static final long serialVersionUID = 1L;

    private String logId;
    private String collectTime;
    private String srcIp;
    private String dstIp;
    private String protocol;
    private int anomalyType;
    private Map<String, Object> anomalyDetail = new LinkedHashMap<>();
    private String remark1;
    private String remark2;
    private String vendorCode;

    public AlertRecord() {
    }

    /**
     * anomalyType=2 的 JSON 字段组装处。
     * 如果你想知道日志里的 baseline_source / bytes_threshold 等字段从哪里输出，就看这个方法。
     */
    public static AlertRecord largeTraffic(String vendorCode, String remark1, String remark2,
                                           MetricRecord metric, LargeTrafficEvidence evidence) {
        AlertRecord alert = base(vendorCode, remark1, remark2, metric.getCollectTime(),
                metric.getSrcIp(), metric.getDstIp(), metric.getProtocol(), 2);
        alert.anomalyDetail.put("current_bytes", metric.totalBytes());
        alert.anomalyDetail.put("current_pkts", metric.totalPkts());
        alert.anomalyDetail.put("baseline_source", evidence.baselineSource);
        alert.anomalyDetail.put("baseline_bytes", evidence.baselineBytes);
        alert.anomalyDetail.put("baseline_pkts", evidence.baselinePkts);
        alert.anomalyDetail.put("pair_sample_count", evidence.pairSampleCount);
        alert.anomalyDetail.put("pair_min_samples", evidence.pairMinSamples);

        alert.anomalyDetail.put("context_source", evidence.contextSource);
        alert.anomalyDetail.put("context_historical_days", evidence.contextHistoricalDays);
        alert.anomalyDetail.put("context_p50_bytes", evidence.contextP50Bytes);
        alert.anomalyDetail.put("context_p50_pkts", evidence.contextP50Pkts);
        alert.anomalyDetail.put("context_p90_bytes", evidence.contextP90Bytes);
        alert.anomalyDetail.put("context_p90_pkts", evidence.contextP90Pkts);
        alert.anomalyDetail.put("context_bytes_quantile", evidence.contextBytesQuantile);
        alert.anomalyDetail.put("context_pkts_quantile", evidence.contextPktsQuantile);
        alert.anomalyDetail.put("context_high_quantile_bytes", evidence.contextHighQuantileBytes);
        alert.anomalyDetail.put("context_high_quantile_pkts", evidence.contextHighQuantilePkts);

        alert.anomalyDetail.put("bytes_baseline_multiplier", evidence.bytesBaselineMultiplier);
        alert.anomalyDetail.put("pkts_baseline_multiplier", evidence.pktsBaselineMultiplier);
        alert.anomalyDetail.put("bytes_threshold", evidence.bytesThreshold);
        alert.anomalyDetail.put("pkts_threshold", evidence.pktsThreshold);
        alert.anomalyDetail.put("extreme_multiplier", evidence.extremeMultiplier);
        alert.anomalyDetail.put("extreme_bytes_threshold", evidence.extremeBytesThreshold);
        alert.anomalyDetail.put("bytes_anomaly", evidence.bytesAnomaly);
        alert.anomalyDetail.put("pkts_anomaly", evidence.pktsAnomaly);
        alert.anomalyDetail.put("extreme_bytes", evidence.extremeBytes);

        alert.anomalyDetail.put("pair_learning_mode", evidence.pairLearningMode);
        if (evidence.pairLearningCapBytes != null) {
            alert.anomalyDetail.put("pair_learning_cap_bytes", evidence.pairLearningCapBytes);
        }
        if (evidence.pairLearningCapPkts != null) {
            alert.anomalyDetail.put("pair_learning_cap_pkts", evidence.pairLearningCapPkts);
        }
        return alert;
    }

    public static AlertRecord offHours(String vendorCode, String remark1, String remark2,
                                       String collectTime, String srcIp, String dstIp, String protocol,
                                       int rank, int topN, long conns) {
        AlertRecord alert = base(vendorCode, remark1, remark2, collectTime,
                srcIp, dstIp, protocol, 3);
        alert.anomalyDetail.put("topRank", rank);
        alert.anomalyDetail.put("TopN", topN);
        alert.anomalyDetail.put("conns", conns);
        return alert;
    }

    private static AlertRecord base(String vendorCode, String remark1, String remark2,
                                    String collectTime, String srcIp, String dstIp, String protocol,
                                    int anomalyType) {
        AlertRecord alert = new AlertRecord();
        alert.vendorCode = vendorCode;
        alert.remark1 = remark1;
        alert.remark2 = remark2;
        alert.collectTime = collectTime;
        alert.srcIp = srcIp;
        alert.dstIp = dstIp;
        alert.protocol = protocol;
        alert.anomalyType = anomalyType;
        return alert;
    }

    /**
     * logId 不在规则 Detector 中生成，而是在 Source 真正输出前统一分配。
     * 这样 ID 序号与 Flink 的“输出 + 游标推进 + Checkpoint 状态”处于同一提交路径。
     */
    public void assignLogId(String logId) {
        if (logId == null || logId.trim().isEmpty()) {
            throw new IllegalArgumentException("logId must not be blank");
        }
        if (this.logId != null && !this.logId.equals(logId)) {
            throw new IllegalStateException("logId has already been assigned: " + this.logId);
        }
        this.logId = logId;
    }

    public String getLogId() { return logId; }
    public String getCollectTime() { return collectTime; }
    public String getSrcIp() { return srcIp; }
    public String getDstIp() { return dstIp; }
    public String getProtocol() { return protocol; }
    public int getAnomalyType() { return anomalyType; }
    public Map<String, Object> getAnomalyDetail() { return anomalyDetail; }
    public String getRemark1() { return remark1; }
    public String getRemark2() { return remark2; }
    public String getVendorCode() { return vendorCode; }

    /**
     * type=2 的“判定证据快照”。Detector 负责计算，AlertRecord 负责把这些值写进 anomalyDetail。
     * 这样排查误报时不需要重新猜当时使用了哪个 baseline、P99.9 和 multiplier。
     */
    public static final class LargeTrafficEvidence implements Serializable {
        private static final long serialVersionUID = 1L;

        private final String baselineSource;
        private final long baselineBytes;
        private final long baselinePkts;
        private final long pairSampleCount;
        private final int pairMinSamples;
        private final String contextSource;
        private final int contextHistoricalDays;
        private final long contextP50Bytes;
        private final long contextP50Pkts;
        private final long contextP90Bytes;
        private final long contextP90Pkts;
        private final double contextBytesQuantile;
        private final double contextPktsQuantile;
        private final long contextHighQuantileBytes;
        private final long contextHighQuantilePkts;
        private final double bytesBaselineMultiplier;
        private final double pktsBaselineMultiplier;
        private final double bytesThreshold;
        private final double pktsThreshold;
        private final double extremeMultiplier;
        private final double extremeBytesThreshold;
        private final boolean bytesAnomaly;
        private final boolean pktsAnomaly;
        private final boolean extremeBytes;
        private final String pairLearningMode;
        private final Long pairLearningCapBytes;
        private final Long pairLearningCapPkts;

        public LargeTrafficEvidence(String baselineSource,
                                    long baselineBytes,
                                    long baselinePkts,
                                    long pairSampleCount,
                                    int pairMinSamples,
                                    String contextSource,
                                    int contextHistoricalDays,
                                    long contextP50Bytes,
                                    long contextP50Pkts,
                                    long contextP90Bytes,
                                    long contextP90Pkts,
                                    double contextBytesQuantile,
                                    double contextPktsQuantile,
                                    long contextHighQuantileBytes,
                                    long contextHighQuantilePkts,
                                    double bytesBaselineMultiplier,
                                    double pktsBaselineMultiplier,
                                    double bytesThreshold,
                                    double pktsThreshold,
                                    double extremeMultiplier,
                                    double extremeBytesThreshold,
                                    boolean bytesAnomaly,
                                    boolean pktsAnomaly,
                                    boolean extremeBytes,
                                    String pairLearningMode,
                                    Long pairLearningCapBytes,
                                    Long pairLearningCapPkts) {
            this.baselineSource = baselineSource;
            this.baselineBytes = baselineBytes;
            this.baselinePkts = baselinePkts;
            this.pairSampleCount = pairSampleCount;
            this.pairMinSamples = pairMinSamples;
            this.contextSource = contextSource;
            this.contextHistoricalDays = contextHistoricalDays;
            this.contextP50Bytes = contextP50Bytes;
            this.contextP50Pkts = contextP50Pkts;
            this.contextP90Bytes = contextP90Bytes;
            this.contextP90Pkts = contextP90Pkts;
            this.contextBytesQuantile = contextBytesQuantile;
            this.contextPktsQuantile = contextPktsQuantile;
            this.contextHighQuantileBytes = contextHighQuantileBytes;
            this.contextHighQuantilePkts = contextHighQuantilePkts;
            this.bytesBaselineMultiplier = bytesBaselineMultiplier;
            this.pktsBaselineMultiplier = pktsBaselineMultiplier;
            this.bytesThreshold = bytesThreshold;
            this.pktsThreshold = pktsThreshold;
            this.extremeMultiplier = extremeMultiplier;
            this.extremeBytesThreshold = extremeBytesThreshold;
            this.bytesAnomaly = bytesAnomaly;
            this.pktsAnomaly = pktsAnomaly;
            this.extremeBytes = extremeBytes;
            this.pairLearningMode = pairLearningMode;
            this.pairLearningCapBytes = pairLearningCapBytes;
            this.pairLearningCapPkts = pairLearningCapPkts;
        }
    }
}

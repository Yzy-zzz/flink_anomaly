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
     * anomalyType=2 的对外 anomalyDetail。
     *
     * 这里只输出接收方真正需要的“当前值、参考基线、最终阈值、告警原因”。
     * quantile、multiplier、Pair 学习模式等仍属于检测算法内部实现，不进入对外告警 JSON，
     * 避免算法参数调整导致接口字段频繁变化。
     */
    public static AlertRecord largeTraffic(String vendorCode, String remark1, String remark2,
                                           MetricRecord metric, LargeTrafficEvidence evidence) {
        AlertRecord alert = base(vendorCode, remark1, remark2, metric.getCollectTime(),
                metric.getSrcIp(), metric.getDstIp(), metric.getProtocol(), 2);
        alert.anomalyDetail.put("current_bytes", metric.totalBytes());
        alert.anomalyDetail.put("current_pkts", metric.totalPkts());
        alert.anomalyDetail.put("baseline_bytes", evidence.baselineBytes);
        alert.anomalyDetail.put("baseline_pkts", evidence.baselinePkts);
        alert.anomalyDetail.put("bytes_threshold", evidence.bytesThreshold);
        alert.anomalyDetail.put("pkts_threshold", evidence.pktsThreshold);
        alert.anomalyDetail.put("extreme_bytes_threshold", evidence.extremeBytesThreshold);
        alert.anomalyDetail.put("anomaly_reason", evidence.pktsAnomaly
                ? "BYTES_AND_PKTS" : "EXTREME_BYTES");
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
     * type=2 对外告警真正需要的判定证据。
     * 算法内部的 quantile、multiplier、学习模式等不进入这个接口对象。
     */
    public static final class LargeTrafficEvidence implements Serializable {
        private static final long serialVersionUID = 1L;

        private final long baselineBytes;
        private final long baselinePkts;
        private final double bytesThreshold;
        private final double pktsThreshold;
        private final double extremeBytesThreshold;
        private final boolean pktsAnomaly;

        public LargeTrafficEvidence(long baselineBytes,
                                    long baselinePkts,
                                    double bytesThreshold,
                                    double pktsThreshold,
                                    double extremeBytesThreshold,
                                    boolean pktsAnomaly) {
            this.baselineBytes = baselineBytes;
            this.baselinePkts = baselinePkts;
            this.bytesThreshold = bytesThreshold;
            this.pktsThreshold = pktsThreshold;
            this.extremeBytesThreshold = extremeBytesThreshold;
            this.pktsAnomaly = pktsAnomaly;
        }
    }
}

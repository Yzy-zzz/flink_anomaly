package cn.ac.iie.anomaly.model;

import java.io.Serializable;
import java.util.LinkedHashMap;
import java.util.Map;

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

    public static AlertRecord largeTraffic(String logId, String vendorCode, String remark1, String remark2,
                                           MetricRecord metric, long baselineBytes, long baselinePkts) {
        AlertRecord alert = base(logId, vendorCode, remark1, remark2, metric.getCollectTime(),
                metric.getSrcIp(), metric.getDstIp(), metric.getProtocol(), 2);
        alert.anomalyDetail.put("current_bytes", metric.totalBytes());
        alert.anomalyDetail.put("current_pkts", metric.totalPkts());
        alert.anomalyDetail.put("baseline_bytes", baselineBytes);
        alert.anomalyDetail.put("baseline_pkts", baselinePkts);
        return alert;
    }

    public static AlertRecord offHours(String logId, String vendorCode, String remark1, String remark2,
                                       String collectTime, String srcIp, String dstIp, String protocol,
                                       int rank, int topN, long conns) {
        AlertRecord alert = base(logId, vendorCode, remark1, remark2, collectTime,
                srcIp, dstIp, protocol, 3);
        alert.anomalyDetail.put("topRank", rank);
        alert.anomalyDetail.put("TopN", topN);
        alert.anomalyDetail.put("conns", conns);
        return alert;
    }

    private static AlertRecord base(String logId, String vendorCode, String remark1, String remark2,
                                    String collectTime, String srcIp, String dstIp, String protocol,
                                    int anomalyType) {
        AlertRecord alert = new AlertRecord();
        alert.logId = logId;
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
}

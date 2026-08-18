package cn.ac.iie.anomaly.rule;

import cn.ac.iie.anomaly.config.AppConfig;
import cn.ac.iie.anomaly.model.AlertRecord;
import cn.ac.iie.anomaly.sketch.WeightedSpaceSavingSketch;
import cn.ac.iie.anomaly.util.ConnectionKey;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

/**
 * anomalyType=3 的最终输出器：把 Space-Saving sketch 中估算连接数最大的 Pair 排成 Top-N 告警。
 * 初学者如果只关心 type=2，可以先跳过这个类。
 */
public final class OffHoursDetector implements Serializable {
    private static final long serialVersionUID = 1L;

    private final int topN;
    private final String logId;
    private final String vendorCode;
    private final String remark1;
    private final String remark2;

    public OffHoursDetector(AppConfig config) {
        this.topN = config.getInt("rule.offhours.topn", 100);
        this.logId = config.get("alert.logid", "STATIC_LOG_ID");
        this.vendorCode = config.get("alert.vendorCode", "V001");
        this.remark1 = config.get("alert.remark1", "");
        this.remark2 = config.get("alert.remark2", "");
    }

    public List<AlertRecord> detect(OffHoursAccumulator acc) {
        List<AlertRecord> alerts = new ArrayList<>();
        if (acc == null) {
            return alerts;
        }
        List<WeightedSpaceSavingSketch.Entry> top = acc.getSketch().topN(topN);
        int rank = 1;
        for (WeightedSpaceSavingSketch.Entry entry : top) {
            ConnectionKey.Parts parts = ConnectionKey.decode(entry.getKey());
            alerts.add(AlertRecord.offHours(
                    logId, vendorCode, remark1, remark2,
                    entry.getCollectTime(), parts.srcIp, parts.dstIp, parts.protocol,
                    rank, topN, entry.getEstimate()));
            rank++;
        }
        return alerts;
    }
}

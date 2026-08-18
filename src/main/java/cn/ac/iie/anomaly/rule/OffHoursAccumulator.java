package cn.ac.iie.anomaly.rule;

import cn.ac.iie.anomaly.model.MetricRecord;
import cn.ac.iie.anomaly.sketch.WeightedSpaceSavingSketch;
import cn.ac.iie.anomaly.util.ConnectionKey;

import java.io.Serializable;

/**
 * anomalyType=3 的窗口内累计器。使用 Space-Saving 近似算法，只保留可能进入 Top-N 的重连接 Pair，
 * 不需要为所有 IP Pair 建一个巨大 HashMap。
 */
public class OffHoursAccumulator implements Serializable {
    private static final long serialVersionUID = 1L;

    private final WeightedSpaceSavingSketch sketch;

    public OffHoursAccumulator(int capacity) {
        this.sketch = new WeightedSpaceSavingSketch(capacity);
    }

    public void add(MetricRecord record) {
        sketch.update(ConnectionKey.encode(record.getSrcIp(), record.getDstIp(), record.getProtocol()),
                record.getConnCount(), record.getCollectTime());
    }

    public void mergeFrom(OffHoursAccumulator other) {
        this.sketch.mergeFrom(other.sketch);
    }

    public WeightedSpaceSavingSketch getSketch() {
        return sketch;
    }
}

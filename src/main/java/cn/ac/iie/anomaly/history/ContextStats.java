package cn.ac.iie.anomaly.history;

import java.io.Serializable;

/**
 * 一个 Context 的分位数摘要。
 * p50 是中位数；p90 是 90 分位；thresholdBytes/thresholdPkts 通常是配置的高分位（默认 P99.9）。
 * usable 表示历史天数是否达到 history.context.min.days，可否当成正式历史基线使用。
 */
public final class ContextStats implements Serializable {
    private static final long serialVersionUID = 1L;

    private final int contributingDays;
    private final boolean usable;
    private final long p50Bytes;
    private final long p50Pkts;
    private final long p90Bytes;
    private final long p90Pkts;
    private final long thresholdBytes;
    private final long thresholdPkts;

    public ContextStats(int contributingDays, boolean usable,
                        long p50Bytes, long p50Pkts,
                        long p90Bytes, long p90Pkts,
                        long thresholdBytes, long thresholdPkts) {
        this.contributingDays = contributingDays;
        this.usable = usable;
        this.p50Bytes = p50Bytes;
        this.p50Pkts = p50Pkts;
        this.p90Bytes = p90Bytes;
        this.p90Pkts = p90Pkts;
        this.thresholdBytes = thresholdBytes;
        this.thresholdPkts = thresholdPkts;
    }

    public static ContextStats empty() {
        return new ContextStats(0, false, 0L, 0L, 0L, 0L, 0L, 0L);
    }

    public int getContributingDays() { return contributingDays; }
    public boolean isUsable() { return usable; }
    public long getP50Bytes() { return p50Bytes; }
    public long getP50Pkts() { return p50Pkts; }
    public long getP90Bytes() { return p90Bytes; }
    public long getP90Pkts() { return p90Pkts; }
    public long getThresholdBytes() { return thresholdBytes; }
    public long getThresholdPkts() { return thresholdPkts; }
}

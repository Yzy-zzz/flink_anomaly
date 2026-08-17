package cn.ac.iie.anomaly.model;

import java.io.Serializable;

public class MetricRecord implements Serializable {
    private static final long serialVersionUID = 1L;

    private String collectTime;
    private long collectTimestamp;
    private String srcIp;
    private String dstIp;
    private String protocol;
    private long connCount;
    private long c2sPkts;
    private long s2cPkts;
    private long c2sBytes;
    private long s2cBytes;

    public MetricRecord() {
    }

    public MetricRecord(String collectTime, long collectTimestamp, String srcIp, String dstIp, String protocol,
                        long connCount, long c2sPkts, long s2cPkts, long c2sBytes, long s2cBytes) {
        this.collectTime = collectTime;
        this.collectTimestamp = collectTimestamp;
        this.srcIp = srcIp;
        this.dstIp = dstIp;
        this.protocol = protocol;
        this.connCount = connCount;
        this.c2sPkts = c2sPkts;
        this.s2cPkts = s2cPkts;
        this.c2sBytes = c2sBytes;
        this.s2cBytes = s2cBytes;
    }

    public long totalBytes() {
        return safeAdd(c2sBytes, s2cBytes);
    }

    public long totalPkts() {
        return safeAdd(c2sPkts, s2cPkts);
    }

    private static long safeAdd(long a, long b) {
        if (a > 0 && b > Long.MAX_VALUE - a) {
            return Long.MAX_VALUE;
        }
        return a + b;
    }

    public String pairKey() {
        return String.valueOf(srcIp) + "|" + String.valueOf(dstIp) + "|" + String.valueOf(protocol);
    }

    public String getCollectTime() { return collectTime; }
    public long getCollectTimestamp() { return collectTimestamp; }
    public String getSrcIp() { return srcIp; }
    public String getDstIp() { return dstIp; }
    public String getProtocol() { return protocol; }
    public long getConnCount() { return connCount; }
    public long getC2sPkts() { return c2sPkts; }
    public long getS2cPkts() { return s2cPkts; }
    public long getC2sBytes() { return c2sBytes; }
    public long getS2cBytes() { return s2cBytes; }
}

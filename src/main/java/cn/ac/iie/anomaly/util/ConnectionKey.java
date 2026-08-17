package cn.ac.iie.anomaly.util;

public final class ConnectionKey {
    private ConnectionKey() {
    }

    public static String encode(String srcIp, String dstIp, String protocol) {
        return nullToEmpty(srcIp) + "|" + nullToEmpty(dstIp) + "|" + nullToEmpty(protocol);
    }

    public static Parts decode(String key) {
        String[] parts = key.split("\\|", 3);
        return new Parts(parts.length > 0 ? parts[0] : "",
                parts.length > 1 ? parts[1] : "",
                parts.length > 2 ? parts[2] : "");
    }

    private static String nullToEmpty(String value) {
        return value == null ? "" : value;
    }

    public static final class Parts {
        public final String srcIp;
        public final String dstIp;
        public final String protocol;

        public Parts(String srcIp, String dstIp, String protocol) {
            this.srcIp = srcIp;
            this.dstIp = dstIp;
            this.protocol = protocol;
        }
    }
}

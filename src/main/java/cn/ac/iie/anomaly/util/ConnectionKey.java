package cn.ac.iie.anomaly.util;

/**
 * IP Pair 辅助工具：把 srcIp/dstIp/protocol 编码成稳定 key，并提供 64 位 hash。
 * Pair EMA 历史只保存 hash，不保存长字符串，以降低 20 万 Pair 状态的内存开销。
 */
public final class ConnectionKey {
    private ConnectionKey() {
    }

    public static String encode(String srcIp, String dstIp, String protocol) {
        return nullToEmpty(srcIp) + "|" + nullToEmpty(dstIp) + "|" + nullToEmpty(protocol);
    }

    /** Deterministic 64-bit FNV-1a hash used to keep pair-history keys compact. */
    public static long hash64(String key) {
        long hash = 0xcbf29ce484222325L;
        for (int i = 0; i < key.length(); i++) {
            char c = key.charAt(i);
            hash ^= (c & 0xff);
            hash *= 0x100000001b3L;
            hash ^= ((c >>> 8) & 0xff);
            hash *= 0x100000001b3L;
        }
        return hash;
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

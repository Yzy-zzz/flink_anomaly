package cn.ac.iie.anomaly.source;

import cn.ac.iie.anomaly.model.MetricRecord;
import cn.ac.iie.anomaly.util.TimeUtils;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.time.ZoneId;

/** Pure JSON-to-domain decoder used by the Flink parser and unit tests. */
final class MetricJsonDecoder {
    private MetricJsonDecoder() {
    }

    static MetricRecord decode(String value, ObjectMapper mapper, ZoneId zoneId) throws Exception {
        JsonNode root = mapper.readTree(value);
        if (root == null || !root.isObject()) {
            throw new IllegalArgumentException("JSON root is not an object");
        }

        String collectTime = requiredText(root, "collectTime");
        TimeUtils.ParsedTime parsedTime = TimeUtils.parse(collectTime, zoneId);
        String srcIp = requiredText(root, "srcIp");
        String dstIp = requiredText(root, "dstIp");
        if (!isPlausibleIp(srcIp) || !isPlausibleIp(dstIp)) {
            throw new IllegalArgumentException("srcIp/dstIp is not a valid IPv4/IPv6 address");
        }
        String protocol = optionalText(root, "protocol", "");

        return new MetricRecord(
                parsedTime.getText(),
                parsedTime.getEpochMillis(),
                srcIp,
                dstIp,
                protocol,
                nonNegativeLong(root, "connCount"),
                nonNegativeLong(root, "c2sPkts"),
                nonNegativeLong(root, "s2cPkts"),
                nonNegativeLong(root, "c2sBytes"),
                nonNegativeLong(root, "s2cBytes"));
    }

    private static String requiredText(JsonNode root, String field) {
        JsonNode node = root.get(field);
        if (node == null || node.isNull()) {
            throw new IllegalArgumentException(field + " is missing/null");
        }
        String value = node.asText("").trim();
        if (value.isEmpty()) {
            throw new IllegalArgumentException(field + " is blank");
        }
        return value;
    }

    private static String optionalText(JsonNode root, String field, String defaultValue) {
        JsonNode node = root.get(field);
        if (node == null || node.isNull()) {
            return defaultValue;
        }
        return node.asText(defaultValue).trim();
    }

    private static long nonNegativeLong(JsonNode root, String field) {
        JsonNode node = root.get(field);
        if (node == null || node.isNull()) {
            return 0L;
        }
        if (!node.isNumber() && !node.isTextual()) {
            throw new IllegalArgumentException(field + " is not numeric");
        }
        String text = node.asText("0").trim();
        if (text.isEmpty()) {
            return 0L;
        }
        final long value;
        try {
            value = Long.parseLong(text);
        } catch (NumberFormatException e) {
            throw new IllegalArgumentException(field + " is not a long: " + text, e);
        }
        return Math.max(0L, value);
    }

    static boolean isPlausibleIp(String value) {
        if (value == null) {
            return false;
        }
        String ip = value.trim();
        if (ip.isEmpty() || ip.length() > 45) {
            return false;
        }
        if (ip.indexOf(':') >= 0) {
            if (ip.indexOf(':') == ip.lastIndexOf(':')) {
                return false;
            }
            for (int i = 0; i < ip.length(); i++) {
                char c = ip.charAt(i);
                boolean ok = c == ':' || c == '.'
                        || (c >= '0' && c <= '9')
                        || (c >= 'a' && c <= 'f')
                        || (c >= 'A' && c <= 'F');
                if (!ok) {
                    return false;
                }
            }
            return true;
        }

        String[] parts = ip.split("\\.", -1);
        if (parts.length != 4) {
            return false;
        }
        for (String part : parts) {
            if (part.isEmpty() || part.length() > 3) {
                return false;
            }
            int n = 0;
            for (int i = 0; i < part.length(); i++) {
                char c = part.charAt(i);
                if (c < '0' || c > '9') {
                    return false;
                }
                n = n * 10 + (c - '0');
            }
            if (n > 255) {
                return false;
            }
        }
        return true;
    }
}

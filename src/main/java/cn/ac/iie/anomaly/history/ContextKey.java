package cn.ac.iie.anomaly.history;

import java.time.DayOfWeek;
import java.time.LocalDateTime;

/**
 * 【导读：什么叫 Context】
 * Context = protocol + WORKDAY/WEEKEND + 时间槽。
 * 例如 2026-08-13 00:02、protocol=UNKNOWN、slotMinutes=5，会归到：UNKNOWN + WORKDAY + slot0。
 * 注意这里不含 IP，因此同一 Context 的不同 src/dst 会共享 P50/P99.9。
 */
public final class ContextKey {
    private static final char SEP = '\u001f';

    private ContextKey() {
    }

    public static String of(String protocol, LocalDateTime time, int slotMinutes) {
        int minuteOfDay = time.getHour() * 60 + time.getMinute();
        int slot = minuteOfDay / slotMinutes;
        DayOfWeek dow = time.getDayOfWeek();
        String dayType = (dow == DayOfWeek.SATURDAY || dow == DayOfWeek.SUNDAY) ? "WEEKEND" : "WORKDAY";
        return normalize(protocol) + SEP + dayType + SEP + slot;
    }

    private static String normalize(String value) {
        if (value == null || value.trim().isEmpty()) {
            return "UNKNOWN";
        }
        return value.trim();
    }
}

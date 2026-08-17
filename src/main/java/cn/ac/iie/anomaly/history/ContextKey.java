package cn.ac.iie.anomaly.history;

import java.time.DayOfWeek;
import java.time.LocalDateTime;

/** Compact context key used for historical distribution baselines. */
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

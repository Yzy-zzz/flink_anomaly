package cn.ac.iie.anomaly.util;

import java.sql.Timestamp;
import java.time.DayOfWeek;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeParseException;
import java.util.Date;

public final class TimeUtils {
    private static final DateTimeFormatter OUTPUT = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");
    private static final DateTimeFormatter[] INPUTS = new DateTimeFormatter[] {
            DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss"),
            DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.S"),
            DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.SSS"),
            DateTimeFormatter.ISO_LOCAL_DATE_TIME
    };

    private TimeUtils() {
    }

    public static ParsedTime parse(Object value, ZoneId zoneId) {
        if (value == null) {
            throw new IllegalArgumentException("collectTime is null");
        }
        LocalDateTime local;
        if (value instanceof LocalDateTime) {
            local = (LocalDateTime) value;
        } else if (value instanceof Timestamp) {
            local = ((Timestamp) value).toLocalDateTime();
        } else if (value instanceof Date) {
            local = LocalDateTime.ofInstant(((Date) value).toInstant(), zoneId);
        } else if (value instanceof Instant) {
            local = LocalDateTime.ofInstant((Instant) value, zoneId);
        } else {
            local = parseText(String.valueOf(value).trim());
        }
        long epochMillis = local.atZone(zoneId).toInstant().toEpochMilli();
        return new ParsedTime(local.format(OUTPUT), local, epochMillis);
    }

    private static LocalDateTime parseText(String text) {
        DateTimeParseException last = null;
        for (DateTimeFormatter formatter : INPUTS) {
            try {
                return LocalDateTime.parse(text, formatter);
            } catch (DateTimeParseException e) {
                last = e;
            }
        }
        throw new IllegalArgumentException("Unsupported collectTime: " + text, last);
    }

    public static boolean isOffHours(LocalDateTime time, int weekdayStartHour, int weekdayEndHour) {
        DayOfWeek day = time.getDayOfWeek();
        if (day == DayOfWeek.SATURDAY || day == DayOfWeek.SUNDAY) {
            return true;
        }
        int hour = time.getHour();
        if (weekdayStartHour > weekdayEndHour) {
            return hour >= weekdayStartHour || hour < weekdayEndHour;
        }
        return hour >= weekdayStartHour && hour < weekdayEndHour;
    }

    public static final class ParsedTime {
        private final String text;
        private final LocalDateTime localDateTime;
        private final long epochMillis;

        public ParsedTime(String text, LocalDateTime localDateTime, long epochMillis) {
            this.text = text;
            this.localDateTime = localDateTime;
            this.epochMillis = epochMillis;
        }

        public String getText() { return text; }
        public LocalDateTime getLocalDateTime() { return localDateTime; }
        public long getEpochMillis() { return epochMillis; }
    }
}

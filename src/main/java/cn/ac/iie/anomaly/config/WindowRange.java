package cn.ac.iie.anomaly.config;

import java.io.Serializable;
import java.time.Clock;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.time.temporal.ChronoUnit;

/** A half-open business-time window: [start, end). */
public final class WindowRange implements Serializable {
    private static final long serialVersionUID = 1L;
    public static final DateTimeFormatter DATETIME = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");
    private static final DateTimeFormatter DATE = DateTimeFormatter.ofPattern("yyyy-MM-dd");

    private final LocalDateTime start;
    private final LocalDateTime end;
    private final ZoneId zoneId;

    public WindowRange(LocalDateTime start, LocalDateTime end, ZoneId zoneId) {
        if (!end.isAfter(start)) {
            throw new IllegalArgumentException("window end must be after start");
        }
        this.start = start;
        this.end = end;
        this.zoneId = zoneId;
    }

    public static WindowRange fromStart(LocalDateTime start, int sizeMinutes, ZoneId zoneId) {
        return new WindowRange(start, start.plusMinutes(sizeMinutes), zoneId);
    }

    /**
     * Returns the end timestamp of the newest window that is safe to read.
     * Example: size=5, delay=1, business time=20:11:20 -> 20:10:00.
     */
    public static LocalDateTime latestClosedEnd(AppConfig config, Clock clock) {
        ZoneId zone = ZoneId.of(config.get("business.timezone", "Asia/Shanghai"));
        int sizeMinutes = config.getInt("window.size.minutes", 5);
        int delayMinutes = config.getInt("window.delay.minutes", 1);
        ZonedDateTime effective = ZonedDateTime.now(clock).withZoneSameInstant(zone).minusMinutes(delayMinutes);
        int flooredMinute = (effective.getMinute() / sizeMinutes) * sizeMinutes;
        return effective.withMinute(flooredMinute).withSecond(0).withNano(0).toLocalDateTime();
    }

    /** Starting cursor when no checkpoint state exists. */
    public static LocalDateTime initialStart(AppConfig config, Clock clock) {
        int sizeMinutes = config.getInt("window.size.minutes", 5);
        String mode = config.get("source.start.mode", "latest_closed");
        if ("latest_closed".equalsIgnoreCase(mode)) {
            return latestClosedEnd(config, clock).minusMinutes(sizeMinutes);
        }
        if ("fixed".equalsIgnoreCase(mode)) {
            LocalDateTime start = LocalDateTime.parse(config.get("source.start.time"), DATETIME);
            validateAlignment(start, sizeMinutes);
            return start;
        }
        throw new IllegalArgumentException("Unsupported source.start.mode: " + mode);
    }

    public static void validateAlignment(LocalDateTime time, int sizeMinutes) {
        if (time.getSecond() != 0 || time.getNano() != 0 || time.getMinute() % sizeMinutes != 0) {
            throw new IllegalArgumentException("time must align to " + sizeMinutes + "-minute boundary: " + time);
        }
    }

    /**
     * Doris predicates deliberately include all three time columns:
     * dayTime/dayHourTime help partition pruning; collectTime defines the exact window.
     */
    public String toDorisFilter() {
        LocalDateTime lowerHour = start.truncatedTo(ChronoUnit.HOURS);
        LocalDateTime upperHour = end.truncatedTo(ChronoUnit.HOURS);
        if (!upperHour.equals(end)) {
            upperHour = upperHour.plusHours(1);
        }
        String startDate = start.toLocalDate().format(DATE);
        String endDate = end.minusNanos(1).toLocalDate().format(DATE);
        return "dayTime >= '" + startDate + "'"
                + " AND dayTime <= '" + endDate + "'"
                + " AND dayHourTime >= '" + lowerHour.format(DATETIME) + "'"
                + " AND dayHourTime < '" + upperHour.format(DATETIME) + "'"
                + " AND collectTime >= '" + start.format(DATETIME) + "'"
                + " AND collectTime < '" + end.format(DATETIME) + "'";
    }

    public LocalDateTime getStart() { return start; }
    public LocalDateTime getEnd() { return end; }
    public ZoneId getZoneId() { return zoneId; }
    public String startText() { return start.format(DATETIME); }
    public String endText() { return end.format(DATETIME); }

    @Override
    public String toString() {
        return "[" + startText() + ", " + endText() + ") " + zoneId;
    }
}

package cn.ac.iie.anomaly.config;

import java.io.Serializable;
import java.time.LocalDateTime;
import java.time.ZoneId;
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

    /** Floors a business-local timestamp to the previous N-minute boundary. */
    public static LocalDateTime floorToWindow(LocalDateTime time, int sizeMinutes) {
        if (sizeMinutes <= 0 || 60 % sizeMinutes != 0) {
            throw new IllegalArgumentException("sizeMinutes must be a positive divisor of 60");
        }
        int flooredMinute = (time.getMinute() / sizeMinutes) * sizeMinutes;
        return time.withMinute(flooredMinute).withSecond(0).withNano(0);
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

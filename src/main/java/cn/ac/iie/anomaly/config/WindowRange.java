package cn.ac.iie.anomaly.config;

import java.io.Serializable;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.time.temporal.ChronoUnit;

/** Business-time window using left-closed/right-open semantics: [start, end). */
public final class WindowRange implements Serializable {
    private static final long serialVersionUID = 1L;
    public static final DateTimeFormatter DATETIME = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");
    private static final DateTimeFormatter DATE = DateTimeFormatter.ofPattern("yyyy-MM-dd");

    private final LocalDateTime start;
    private final LocalDateTime end;
    private final ZoneId zoneId;

    public WindowRange(LocalDateTime start, LocalDateTime end, ZoneId zoneId) {
        if (start == null || end == null || zoneId == null) {
            throw new IllegalArgumentException("window start/end/zoneId must not be null");
        }
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
     * Backward-compatible helper kept only so an old Doris source accidentally left in an
     * overlaid checkout does not break compilation. The Kafka topology never calls this method.
     */
    @Deprecated
    public String toDorisFilter() {
        return toDorisFilter(start, end);
    }

    /**
     * Backward-compatible Doris predicate helper. New Kafka code does not depend on Doris.
     */
    @Deprecated
    public String toDorisFilter(LocalDateTime queryStart, LocalDateTime queryEnd) {
        if (queryStart == null || queryEnd == null || !queryEnd.isAfter(queryStart)) {
            throw new IllegalArgumentException("queryEnd must be after queryStart");
        }
        if (queryStart.isBefore(start) || queryEnd.isAfter(end)) {
            throw new IllegalArgumentException("query range must stay inside window " + this);
        }

        LocalDateTime hour = queryStart.truncatedTo(ChronoUnit.HOURS);
        LocalDateTime lastIncluded = queryEnd.minusNanos(1);
        if (!lastIncluded.truncatedTo(ChronoUnit.HOURS).equals(hour)) {
            throw new IllegalArgumentException("query range must stay inside one Doris hourly partition");
        }

        String day = queryStart.toLocalDate().format(DATE);
        return "dayTime = '" + day + "'"
                + " AND dayHourTime = '" + hour.format(DATETIME) + "'"
                + " AND collectTime >= '" + queryStart.format(DATETIME) + "'"
                + " AND collectTime < '" + queryEnd.format(DATETIME) + "'";
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

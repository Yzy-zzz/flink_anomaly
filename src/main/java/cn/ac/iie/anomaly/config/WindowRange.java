package cn.ac.iie.anomaly.config;

import java.io.Serializable;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.time.temporal.ChronoUnit;

/**
 * 业务 5 分钟窗口，采用左闭右开 [start, end)。
 * 例如 [00:00, 00:05) 包含 00:04:59，但不包含 00:05:00，避免相邻窗口重复统计。
 */
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
     * Doris predicates deliberately include all three sort/partition time columns.
     *
     * metric_endpoint stores dayHourTime as the exact hour boundary (for example 10:00:00 for
     * every row in partition pYYYYMMDD10).  Using equality on dayTime/dayHourTime and a range on
     * collectTime matches the table sort-key prefix (dayTime, dayHourTime, collectTime) better than
     * using a broad one-hour range on dayHourTime.
     */
    public String toDorisFilter() {
        return toDorisFilter(start, end);
    }

    /** Builds the same optimized Doris predicate for a sub-range inside this business window. */
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

package cn.ac.iie.anomaly.rule;

import cn.ac.iie.anomaly.config.AppConfig;
import cn.ac.iie.anomaly.model.AlertRecord;
import cn.ac.iie.anomaly.model.MetricRecord;
import cn.ac.iie.anomaly.util.TimeUtils;

import java.io.Serializable;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.List;

/**
 * In-memory analyzer for one five-minute Doris query.
 * Its memory footprint is bounded by Sketch sizes and candidate capacities.
 */
public final class FiveMinuteWindowAnalyzer implements Serializable {
    private static final long serialVersionUID = 1L;

    private final boolean largeEnabled;
    private final boolean offHoursEnabled;
    private final int weekdayStartHour;
    private final int weekdayEndHour;

    private final LargeTrafficAccumulator largeAccumulator;
    private final OffHoursAccumulator offHoursAccumulator;
    private final LargeTrafficDetector largeDetector;
    private final OffHoursDetector offHoursDetector;
    private long rowCount;
    private long offHoursRowCount;

    public FiveMinuteWindowAnalyzer(AppConfig config) {
        this.largeEnabled = config.getBoolean("rule.large.enabled", true);
        this.offHoursEnabled = config.getBoolean("rule.offhours.enabled", true);
        this.weekdayStartHour = config.getInt("rule.offhours.weekday.start.hour", 20);
        this.weekdayEndHour = config.getInt("rule.offhours.weekday.end.hour", 8);

        if (largeEnabled) {
            this.largeAccumulator = new LargeTrafficAccumulator(
                    config.getDouble("rule.large.tdigest.compression", 200d),
                    config.getInt("rule.large.cms.depth", 5),
                    config.getInt("rule.large.cms.width", 262144),
                    config.getInt("rule.large.candidate.capacity", 20000));
            this.largeDetector = new LargeTrafficDetector(config);
        } else {
            this.largeAccumulator = null;
            this.largeDetector = null;
        }

        if (offHoursEnabled) {
            this.offHoursAccumulator = new OffHoursAccumulator(
                    config.getInt("rule.offhours.sketch.capacity", 2048));
            this.offHoursDetector = new OffHoursDetector(config);
        } else {
            this.offHoursAccumulator = null;
            this.offHoursDetector = null;
        }
    }

    public void add(MetricRecord record, LocalDateTime collectLocalTime) {
        rowCount++;
        if (largeEnabled) {
            largeAccumulator.add(record);
        }
        if (offHoursEnabled && TimeUtils.isOffHours(collectLocalTime, weekdayStartHour, weekdayEndHour)) {
            offHoursAccumulator.add(record);
            offHoursRowCount++;
        }
    }

    public List<AlertRecord> finish() {
        List<AlertRecord> alerts = new ArrayList<>();
        if (largeEnabled) {
            alerts.addAll(largeDetector.detect(largeAccumulator));
        }
        if (offHoursEnabled && offHoursRowCount > 0L) {
            alerts.addAll(offHoursDetector.detect(offHoursAccumulator));
        }
        return alerts;
    }

    public long getRowCount() { return rowCount; }
    public long getOffHoursRowCount() { return offHoursRowCount; }
}

package cn.ac.iie.anomaly.rule;

import cn.ac.iie.anomaly.config.AppConfig;
import cn.ac.iie.anomaly.config.WindowRange;
import cn.ac.iie.anomaly.history.ContextKey;
import cn.ac.iie.anomaly.history.ContextStats;
import cn.ac.iie.anomaly.history.HistoricalBaselineStore;
import cn.ac.iie.anomaly.model.AlertRecord;
import cn.ac.iie.anomaly.model.MetricRecord;
import cn.ac.iie.anomaly.util.TimeUtils;

import java.io.Serializable;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * In-memory analyzer for one five-minute Doris query.
 * Long-lived history is read-only while the query runs; a bounded update plan is returned at finish().
 */
public final class FiveMinuteWindowAnalyzer implements Serializable {
    private static final long serialVersionUID = 1L;

    private final boolean largeEnabled;
    private final boolean offHoursEnabled;
    private final int weekdayStartHour;
    private final int weekdayEndHour;
    private final int contextSlotMinutes;
    private final double bytesQuantile;
    private final double pktsQuantile;
    private final LocalDate currentDate;
    private final HistoricalBaselineStore historyStore;

    private final LargeTrafficAccumulator largeAccumulator;
    private final OffHoursAccumulator offHoursAccumulator;
    private final LargeTrafficDetector largeDetector;
    private final OffHoursDetector offHoursDetector;
    private final Map<String, ContextStats> contextStatsCache = new HashMap<>();
    private long rowCount;
    private long offHoursRowCount;

    public FiveMinuteWindowAnalyzer(AppConfig config, HistoricalBaselineStore historyStore, WindowRange window) {
        this.largeEnabled = config.getBoolean("rule.large.enabled", true);
        this.offHoursEnabled = config.getBoolean("rule.offhours.enabled", true);
        this.weekdayStartHour = config.getInt("rule.offhours.weekday.start.hour", 20);
        this.weekdayEndHour = config.getInt("rule.offhours.weekday.end.hour", 8);
        this.contextSlotMinutes = config.getInt("history.context.slot.minutes", 5);
        this.bytesQuantile = config.getDouble("rule.large.bytes.quantile", 0.999d);
        this.pktsQuantile = config.getDouble("rule.large.pkts.quantile", 0.999d);
        this.currentDate = window.getStart().toLocalDate();
        this.historyStore = historyStore;

        if (largeEnabled) {
            this.largeAccumulator = new LargeTrafficAccumulator(
                    config.getDouble("rule.large.tdigest.compression", 200d),
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
            String contextKey = ContextKey.of(record.getProtocol(), collectLocalTime, contextSlotMinutes);
            ContextStats stats = contextStatsCache.get(contextKey);
            if (stats == null) {
                stats = historyStore.contextStats(contextKey, currentDate, bytesQuantile, pktsQuantile);
                contextStatsCache.put(contextKey, stats);
            }
            largeAccumulator.add(record, contextKey, stats);
        }
        if (offHoursEnabled && TimeUtils.isOffHours(collectLocalTime, weekdayStartHour, weekdayEndHour)) {
            offHoursAccumulator.add(record);
            offHoursRowCount++;
        }
    }

    public WindowAnalysisResult finish() {
        List<AlertRecord> alerts = new ArrayList<>();
        HistoricalBaselineStore.WindowUpdate historyUpdate = new HistoricalBaselineStore.WindowUpdate();

        if (largeEnabled) {
            LargeTrafficDetector.DetectionResult largeResult = largeDetector.detect(largeAccumulator, historyStore);
            alerts.addAll(largeResult.getAlerts());
            for (HistoricalBaselineStore.PairSample pairSample : largeResult.getPairSamples()) {
                historyUpdate.addPairSample(pairSample);
            }
            for (HistoricalBaselineStore.ContextUpdate contextUpdate : largeAccumulator.contextUpdates(currentDate)) {
                historyUpdate.addContext(contextUpdate);
            }
        }
        if (offHoursEnabled && offHoursRowCount > 0L) {
            alerts.addAll(offHoursDetector.detect(offHoursAccumulator));
        }
        return new WindowAnalysisResult(alerts, historyUpdate);
    }

    public long getRowCount() { return rowCount; }
    public long getOffHoursRowCount() { return offHoursRowCount; }
    public int getLargeContextCount() { return largeAccumulator == null ? 0 : largeAccumulator.getContextCount(); }
}

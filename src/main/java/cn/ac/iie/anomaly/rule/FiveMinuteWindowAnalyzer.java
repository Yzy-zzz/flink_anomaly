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
 * One complete business-time window analyzer.
 *
 * <p>The Kafka version can have several windows open at the same time while waiting for delayed
 * records.  Snapshot/restore support therefore stores only this window's bounded accumulators and
 * cached context thresholds, never the whole long-lived history store.</p>
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
    private final Map<String, ContextStats> contextStatsCache;
    private long rowCount;
    private long offHoursRowCount;

    public FiveMinuteWindowAnalyzer(AppConfig config, HistoricalBaselineStore historyStore, WindowRange window) {
        this(config, historyStore, window, null);
    }

    public FiveMinuteWindowAnalyzer(AppConfig config,
                                    HistoricalBaselineStore historyStore,
                                    WindowRange window,
                                    Snapshot restored) {
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
            this.largeAccumulator = restored != null && restored.largeAccumulator != null
                    ? restored.largeAccumulator
                    : new LargeTrafficAccumulator(
                            config.getDouble("rule.large.tdigest.compression", 200d),
                            config.getInt("rule.large.candidate.capacity", 20000));
            this.largeDetector = new LargeTrafficDetector(config);
        } else {
            this.largeAccumulator = null;
            this.largeDetector = null;
        }

        if (offHoursEnabled) {
            this.offHoursAccumulator = restored != null && restored.offHoursAccumulator != null
                    ? restored.offHoursAccumulator
                    : new OffHoursAccumulator(config.getInt("rule.offhours.sketch.capacity", 2048));
            this.offHoursDetector = new OffHoursDetector(config);
        } else {
            this.offHoursAccumulator = null;
            this.offHoursDetector = null;
        }

        this.contextStatsCache = restored != null && restored.contextStatsCache != null
                ? new HashMap<>(restored.contextStatsCache)
                : new HashMap<String, ContextStats>();
        this.rowCount = restored == null ? 0L : restored.rowCount;
        this.offHoursRowCount = restored == null ? 0L : restored.offHoursRowCount;
    }

    /** Add one normalized metric record to this business-time window. */
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

    /** Finish this complete window and produce alerts plus one atomic history-update plan. */
    public WindowAnalysisResult finish() {
        List<AlertRecord> alerts = new ArrayList<>();
        HistoricalBaselineStore.WindowUpdate historyUpdate = new HistoricalBaselineStore.WindowUpdate();

        if (largeEnabled) {
            // Delayed Kafka windows may have been opened hours earlier. Refresh the read-only
            // historical context view now so final detection uses all earlier windows that have
            // since been finalized in event-time order.
            largeAccumulator.refreshHistoricalStats(
                    historyStore, currentDate, bytesQuantile, pktsQuantile);
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

    /** Compact checkpoint image for one still-open Kafka event-time window. */
    public Snapshot snapshot() {
        return new Snapshot(
                largeAccumulator,
                offHoursAccumulator,
                new HashMap<>(contextStatsCache),
                rowCount,
                offHoursRowCount);
    }

    public long getRowCount() { return rowCount; }
    public long getOffHoursRowCount() { return offHoursRowCount; }
    public int getLargeContextCount() { return largeAccumulator == null ? 0 : largeAccumulator.getContextCount(); }

    public static final class Snapshot implements Serializable {
        private static final long serialVersionUID = 1L;
        public LargeTrafficAccumulator largeAccumulator;
        public OffHoursAccumulator offHoursAccumulator;
        public Map<String, ContextStats> contextStatsCache;
        public long rowCount;
        public long offHoursRowCount;

        public Snapshot() {
        }

        public Snapshot(LargeTrafficAccumulator largeAccumulator,
                        OffHoursAccumulator offHoursAccumulator,
                        Map<String, ContextStats> contextStatsCache,
                        long rowCount,
                        long offHoursRowCount) {
            this.largeAccumulator = largeAccumulator;
            this.offHoursAccumulator = offHoursAccumulator;
            this.contextStatsCache = contextStatsCache;
            this.rowCount = rowCount;
            this.offHoursRowCount = offHoursRowCount;
        }
    }
}

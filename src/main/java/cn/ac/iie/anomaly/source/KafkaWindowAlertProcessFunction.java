package cn.ac.iie.anomaly.source;

import cn.ac.iie.anomaly.config.AppConfig;
import cn.ac.iie.anomaly.config.WindowRange;
import cn.ac.iie.anomaly.history.HistoricalBaselineStore;
import cn.ac.iie.anomaly.model.AlertRecord;
import cn.ac.iie.anomaly.model.MetricRecord;
import cn.ac.iie.anomaly.rule.FiveMinuteWindowAnalyzer;
import cn.ac.iie.anomaly.rule.WindowAnalysisResult;
import cn.ac.iie.anomaly.util.AlertLogIdGenerator;
import org.apache.flink.api.common.accumulators.LongCounter;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

/**
 * Single ordered anomaly-analysis operator driven by Kafka event-time watermarks.
 *
 * <p>KafkaSource may read partitions in parallel, but all normalized records are keyed to one
 * analysis key before this function so long-lived Pair EMA/context history remains globally ordered,
 * matching the old Doris source semantics.  Several compact 5-minute analyzers may remain open while
 * waiting for delayed records.  When the watermark reaches a window end, the window is finalized,
 * alerts are emitted, and its history update is applied atomically in event-time order.</p>
 */
public final class KafkaWindowAlertProcessFunction
        extends KeyedProcessFunction<Integer, MetricRecord, AlertRecord>
        implements CheckpointedFunction {

    private static final Logger LOG = LoggerFactory.getLogger(KafkaWindowAlertProcessFunction.class);
    private static final long serialVersionUID = 1L;

    private final AppConfig config;
    private final int windowMinutes;
    private final ZoneId zoneId;

    private transient HistoricalBaselineStore historyStore;
    private transient AlertLogIdGenerator alertLogIdGenerator;
    private transient Map<Long, WindowHolder> openWindowsByEndMillis;

    private transient ListState<HistoricalBaselineStore.PairSnapshot> pairHistoryState;
    private transient ListState<HistoricalBaselineStore.ContextSnapshot> contextHistoryState;
    private transient ListState<OpenWindowSnapshot> openWindowState;
    private transient ListState<String> alertLogIdState;

    private transient LongCounter processedRows;
    private transient LongCounter lateRows;
    private transient LongCounter processedWindows;
    private transient LongCounter emittedAlerts;
    private long localLateRows;

    public KafkaWindowAlertProcessFunction(AppConfig config) {
        this.config = config;
        this.windowMinutes = config.getInt("window.size.minutes", 5);
        this.zoneId = ZoneId.of(config.get("business.timezone", "Asia/Shanghai"));
    }

    @Override
    public void open(org.apache.flink.configuration.Configuration parameters) {
        if (historyStore == null) {
            historyStore = new HistoricalBaselineStore(config);
        }
        if (alertLogIdGenerator == null) {
            alertLogIdGenerator = new AlertLogIdGenerator(config.get("alert.device.id"), zoneId);
        }
        if (openWindowsByEndMillis == null) {
            openWindowsByEndMillis = new TreeMap<>();
        }
        processedRows = getRuntimeContext().getLongCounter("kafka-analysis-processed-rows");
        lateRows = getRuntimeContext().getLongCounter("kafka-analysis-too-late-rows");
        processedWindows = getRuntimeContext().getLongCounter("kafka-analysis-processed-windows");
        emittedAlerts = getRuntimeContext().getLongCounter("kafka-analysis-emitted-alerts");
    }

    @Override
    public void processElement(MetricRecord record, Context ctx, Collector<AlertRecord> out) {
        LocalDateTime collectLocalTime = LocalDateTime.ofInstant(
                Instant.ofEpochMilli(record.getCollectTimestamp()), zoneId);
        LocalDateTime windowStart = WindowRange.floorToWindow(collectLocalTime, windowMinutes);
        WindowRange window = WindowRange.fromStart(windowStart, windowMinutes, zoneId);
        long windowEndMillis = window.getEnd().atZone(zoneId).toInstant().toEpochMilli();
        long currentWatermark = ctx.timerService().currentWatermark();

        // Once this timer boundary has passed, recreating the window would duplicate alerts/history.
        if (windowEndMillis <= currentWatermark) {
            lateRows.add(1L);
            long count = ++localLateRows;
            if (count <= 20L || count % 10000L == 0L) {
                LOG.warn("Drop too-late metric: collectTime={}, window={}, currentWatermark={}, lateRows={}",
                        record.getCollectTime(), window,
                        currentWatermark == Long.MIN_VALUE ? "MIN" : Instant.ofEpochMilli(currentWatermark),
                        count);
            }
            return;
        }

        WindowHolder holder = openWindowsByEndMillis.get(windowEndMillis);
        if (holder == null) {
            holder = new WindowHolder(window, new FiveMinuteWindowAnalyzer(config, historyStore, window));
            openWindowsByEndMillis.put(windowEndMillis, holder);
            ctx.timerService().registerEventTimeTimer(windowEndMillis);
            LOG.info("KAFKA_WINDOW_OPEN window={}, endEpochMillis={}, openWindows={}",
                    window, windowEndMillis, openWindowsByEndMillis.size());
        }

        holder.analyzer.add(record, collectLocalTime);
        processedRows.add(1L);
    }

    @Override
    public void onTimer(long timestamp, OnTimerContext ctx, Collector<AlertRecord> out) {
        WindowHolder holder = openWindowsByEndMillis.remove(timestamp);
        if (holder == null) {
            LOG.warn("Event-time timer fired without open-window state: timer={}", timestamp);
            return;
        }

        WindowAnalysisResult analysis = holder.analyzer.finish();
        List<AlertRecord> alerts = analysis.getAlerts();
        for (AlertRecord alert : alerts) {
            alert.assignLogId(alertLogIdGenerator.nextId());
            out.collect(alert);
            emittedAlerts.add(1L);
        }

        historyStore.apply(analysis.getHistoryUpdate(), holder.window.getEnd(), zoneId);
        processedWindows.add(1L);
        LOG.info("KAFKA_WINDOW_FINALIZED window={}, rows={}, offHoursRows={}, alerts={}, contexts={}, "
                        + "pairHistoryEntries={}, contextHistoryBuckets={}, openWindows={}, watermark={}",
                holder.window,
                holder.analyzer.getRowCount(),
                holder.analyzer.getOffHoursRowCount(),
                alerts.size(),
                holder.analyzer.getLargeContextCount(),
                historyStore.pairSize(),
                historyStore.contextBucketSize(),
                openWindowsByEndMillis.size(),
                timestamp);
    }

    @Override
    public void snapshotState(FunctionSnapshotContext context) throws Exception {
        pairHistoryState.clear();
        for (HistoricalBaselineStore.PairSnapshot snapshot : historyStore.snapshotPairs()) {
            pairHistoryState.add(snapshot);
        }

        contextHistoryState.clear();
        for (HistoricalBaselineStore.ContextSnapshot snapshot : historyStore.snapshotContexts()) {
            contextHistoryState.add(snapshot);
        }

        openWindowState.clear();
        for (WindowHolder holder : openWindowsByEndMillis.values()) {
            openWindowState.add(new OpenWindowSnapshot(
                    holder.window.startText(),
                    holder.analyzer.snapshot()));
        }

        alertLogIdState.clear();
        String generatorState = alertLogIdGenerator.snapshotState();
        if (!generatorState.isEmpty()) {
            alertLogIdState.add(generatorState);
        }
    }

    @Override
    public void initializeState(FunctionInitializationContext context) throws Exception {
        historyStore = new HistoricalBaselineStore(config);
        alertLogIdGenerator = new AlertLogIdGenerator(config.get("alert.device.id"), zoneId);
        openWindowsByEndMillis = new TreeMap<>();

        pairHistoryState = context.getOperatorStateStore().getListState(new ListStateDescriptor<>(
                "kafka-large-traffic-pair-history-v1", HistoricalBaselineStore.PairSnapshot.class));
        contextHistoryState = context.getOperatorStateStore().getListState(new ListStateDescriptor<>(
                "kafka-large-traffic-context-history-v1", HistoricalBaselineStore.ContextSnapshot.class));
        openWindowState = context.getOperatorStateStore().getListState(new ListStateDescriptor<>(
                "kafka-open-window-accumulators-v1", OpenWindowSnapshot.class));
        alertLogIdState = context.getOperatorStateStore().getListState(new ListStateDescriptor<>(
                "kafka-alert-log-id-sequence-v1", String.class));

        if (!context.isRestored()) {
            return;
        }

        for (HistoricalBaselineStore.PairSnapshot snapshot : pairHistoryState.get()) {
            historyStore.restorePair(snapshot);
        }
        for (HistoricalBaselineStore.ContextSnapshot snapshot : contextHistoryState.get()) {
            historyStore.restoreContext(snapshot);
        }
        for (OpenWindowSnapshot snapshot : openWindowState.get()) {
            LocalDateTime start = LocalDateTime.parse(snapshot.windowStart, WindowRange.DATETIME);
            WindowRange window = WindowRange.fromStart(start, windowMinutes, zoneId);
            long endMillis = window.getEnd().atZone(zoneId).toInstant().toEpochMilli();
            FiveMinuteWindowAnalyzer analyzer = new FiveMinuteWindowAnalyzer(
                    config, historyStore, window, snapshot.analyzerSnapshot);
            openWindowsByEndMillis.put(endMillis, new WindowHolder(window, analyzer));
        }
        for (String state : alertLogIdState.get()) {
            if (state != null && !state.trim().isEmpty()) {
                alertLogIdGenerator.restoreState(state.trim());
                break;
            }
        }

        LOG.info("Restored Kafka analyzer state: openWindows={}, pairHistoryEntries={}, contextHistoryBuckets={}",
                openWindowsByEndMillis.size(), historyStore.pairSize(), historyStore.contextBucketSize());
    }

    private static final class WindowHolder {
        private final WindowRange window;
        private final FiveMinuteWindowAnalyzer analyzer;

        private WindowHolder(WindowRange window, FiveMinuteWindowAnalyzer analyzer) {
            this.window = window;
            this.analyzer = analyzer;
        }
    }

    /** Flink operator-state POJO for a still-open event-time window. */
    public static final class OpenWindowSnapshot implements Serializable {
        private static final long serialVersionUID = 1L;
        public String windowStart;
        public FiveMinuteWindowAnalyzer.Snapshot analyzerSnapshot;

        public OpenWindowSnapshot() {
        }

        public OpenWindowSnapshot(String windowStart, FiveMinuteWindowAnalyzer.Snapshot analyzerSnapshot) {
            this.windowStart = windowStart;
            this.analyzerSnapshot = analyzerSnapshot;
        }
    }
}

package cn.ac.iie.anomaly.source;

import cn.ac.iie.anomaly.config.AppConfig;
import cn.ac.iie.anomaly.config.WindowRange;
import cn.ac.iie.anomaly.history.HistoricalBaselineStore;
import cn.ac.iie.anomaly.model.AlertRecord;
import cn.ac.iie.anomaly.model.MetricRecord;
import cn.ac.iie.anomaly.rule.FiveMinuteWindowAnalyzer;
import cn.ac.iie.anomaly.rule.WindowAnalysisResult;
import cn.ac.iie.anomaly.util.TimeUtils;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.metrics.Counter;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.Clock;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.util.List;
import java.util.regex.Pattern;

/**
 * Long-running source that polls Doris one mature five-minute data window at a time.
 *
 * Data progress is NOT driven by wall clock. The source periodically queries MAX(collectTime),
 * subtracts a configurable stability delay, floors to a five-minute boundary, and only processes
 * windows whose end is not after that safe data watermark.
 *
 * Consistency:
 * - a whole Doris window is read and analyzed without mutating long-lived history;
 * - after the query fully succeeds, alerts + history update + cursor advance are committed together;
 * - an empty window can be configured to stay on the same cursor and retry later;
 * - failures during a query do not partially update historical baselines.
 */
public class DorisPollingAlertSource extends RichParallelSourceFunction<AlertRecord>
        implements CheckpointedFunction {

    private static final Logger LOG = LoggerFactory.getLogger(DorisPollingAlertSource.class);
    private static final long serialVersionUID = 2L;
    private static final Pattern SAFE_TABLE = Pattern.compile("[A-Za-z0-9_.$]+(?:\\.[A-Za-z0-9_.$]+)?");

    private static final String SELECT_FIELDS = String.join(",",
            "collectTime",
            "srcIp",
            "dstIp",
            "protocol",
            "connCount",
            "c2sPkts",
            "s2cPkts",
            "c2sBytes",
            "s2cBytes");

    private final AppConfig config;

    private volatile boolean running = true;
    private volatile LocalDateTime nextWindowStart;

    private transient ListState<String> cursorState;
    private transient ListState<HistoricalBaselineStore.PairSnapshot> pairHistoryState;
    private transient ListState<HistoricalBaselineStore.ContextSnapshot> contextHistoryState;
    private transient HistoricalBaselineStore historyStore;
    private transient Object stateMutex;
    private transient ZoneId zoneId;
    private transient Connection currentConnection;
    private transient Statement currentStatement;

    private transient Counter processedWindows;
    private transient Counter queriedRows;
    private transient Counter emittedAlerts;
    private transient Counter badRows;
    private transient Counter queryFailures;
    private transient Counter watermarkQueries;
    private transient Counter emptyWindowRetries;

    private transient LocalDateTime cachedDorisMaxCollectTime;
    private transient LocalDateTime cachedSafeWindowEnd;
    private transient long nextWatermarkRefreshAt;

    public DorisPollingAlertSource(AppConfig config) {
        this.config = config;
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        this.zoneId = ZoneId.of(config.get("business.timezone", "Asia/Shanghai"));
        this.stateMutex = this.stateMutex == null ? new Object() : this.stateMutex;
        this.historyStore = this.historyStore == null ? new HistoricalBaselineStore(config) : this.historyStore;
        Class.forName("com.mysql.cj.jdbc.Driver");

        processedWindows = getRuntimeContext().getMetricGroup().counter("processedWindows");
        queriedRows = getRuntimeContext().getMetricGroup().counter("queriedRows");
        emittedAlerts = getRuntimeContext().getMetricGroup().counter("emittedAlerts");
        badRows = getRuntimeContext().getMetricGroup().counter("badRows");
        queryFailures = getRuntimeContext().getMetricGroup().counter("queryFailures");
        watermarkQueries = getRuntimeContext().getMetricGroup().counter("watermarkQueries");
        emptyWindowRetries = getRuntimeContext().getMetricGroup().counter("emptyWindowRetries");
    }

    @Override
    public void run(SourceContext<AlertRecord> ctx) throws Exception {
        if (getRuntimeContext().getNumberOfParallelSubtasks() != 1) {
            throw new IllegalStateException(
                    "DorisPollingAlertSource must run with parallelism=1. "
                            + "Increase parallelism only after implementing Doris query sharding + Sketch merge.");
        }

        final int windowMinutes = config.getInt("window.size.minutes", 5);
        final long idlePollMillis = Math.max(1000L,
                config.getLong("source.poll.interval.seconds", 15L) * 1000L);
        final long watermarkPollMillis = Math.max(5000L,
                config.getLong("source.watermark.poll.interval.seconds", 60L) * 1000L);
        final long retryMillis = Math.max(1000L,
                config.getLong("source.retry.interval.seconds", 30L) * 1000L);
        final long emptyRetryMillis = Math.max(1000L,
                config.getLong("source.empty.window.retry.seconds", 300L) * 1000L);
        final boolean advanceEmptyWindow = config.getBoolean("source.empty.window.advance", false);
        final int maxFailures = Math.max(1, config.getInt("source.retry.max.failures", 10));
        int consecutiveFailures = 0;

        if (nextWindowStart != null) {
            LOG.info("Restored Doris window cursor from checkpoint: {}", nextWindowStart.format(WindowRange.DATETIME));
        }
        LOG.info("Restored historical state: pairEntries={}, contextBuckets={}",
                historyStore.pairSize(), historyStore.contextBucketSize());

        while (running) {
            try {
                refreshDataWatermarkIfNeeded(watermarkPollMillis);

                if (cachedSafeWindowEnd == null) {
                    sleepInterruptibly(idlePollMillis);
                    continue;
                }

                if (nextWindowStart == null) {
                    nextWindowStart = initialCursorFromDataWatermark(cachedSafeWindowEnd, windowMinutes);
                    LOG.info("No restored cursor. Initial Doris window cursor={}, dorisMaxCollectTime={}, safeWindowEnd={}",
                            nextWindowStart.format(WindowRange.DATETIME),
                            formatNullable(cachedDorisMaxCollectTime), formatNullable(cachedSafeWindowEnd));
                }

                LocalDateTime nextWindowEnd = nextWindowStart.plusMinutes(windowMinutes);
                if (nextWindowEnd.isAfter(cachedSafeWindowEnd)) {
                    sleepInterruptibly(idlePollMillis);
                    continue;
                }

                WindowRange window = WindowRange.fromStart(nextWindowStart, windowMinutes, zoneId);
                WindowQueryResult result = queryAndAnalyze(window);

                if (result.rowCount == 0L && !advanceEmptyWindow) {
                    emptyWindowRetries.inc();
                    LOG.warn("Empty Doris window {}. Cursor is NOT advanced because source.empty.window.advance=false. "
                                    + "This window will be retried after {} seconds. dorisMaxCollectTime={}, safeWindowEnd={}",
                            window, emptyRetryMillis / 1000L,
                            formatNullable(cachedDorisMaxCollectTime), formatNullable(cachedSafeWindowEnd));
                    // Force a fresh MAX(collectTime) check on the next loop after the empty-window wait.
                    nextWatermarkRefreshAt = 0L;
                    sleepInterruptibly(emptyRetryMillis);
                    consecutiveFailures = 0;
                    continue;
                }

                // Alerts + history update + cursor advance are one logical source action.
                synchronized (ctx.getCheckpointLock()) {
                    synchronized (stateMutex) {
                        for (AlertRecord alert : result.analysis.getAlerts()) {
                            ctx.collect(alert);
                            emittedAlerts.inc();
                        }
                        historyStore.apply(result.analysis.getHistoryUpdate(), window.getEnd(), zoneId);
                        nextWindowStart = window.getEnd();
                    }
                }

                processedWindows.inc();
                queriedRows.inc(result.rowCount);
                consecutiveFailures = 0;
                LOG.info("Window finished: {}, rows={}, offHoursRows={}, alerts={}, contexts={}, "
                                + "pairHistoryEntries={}, contextHistoryBuckets={}, nextCursor={}",
                        window, result.rowCount, result.offHoursRowCount, result.analysis.getAlerts().size(),
                        result.contextCount, historyStore.pairSize(), historyStore.contextBucketSize(),
                        nextWindowStart.format(WindowRange.DATETIME));
            } catch (SQLException e) {
                queryFailures.inc();
                consecutiveFailures++;
                LOG.error("Doris operation failed (failure {}/{}). Cursor is NOT advanced.",
                        consecutiveFailures, maxFailures, e);
                if (consecutiveFailures >= maxFailures) {
                    throw e;
                }
                nextWatermarkRefreshAt = 0L;
                sleepInterruptibly(retryMillis);
            }
        }
    }

    private LocalDateTime initialCursorFromDataWatermark(LocalDateTime safeWindowEnd, int windowMinutes) {
        String mode = config.get("source.start.mode", "latest_data");
        if ("latest_data".equalsIgnoreCase(mode) || "latest_closed".equalsIgnoreCase(mode)) {
            return safeWindowEnd.minusMinutes(windowMinutes);
        }
        if ("fixed".equalsIgnoreCase(mode)) {
            LocalDateTime start = LocalDateTime.parse(config.get("source.start.time"), WindowRange.DATETIME);
            WindowRange.validateAlignment(start, windowMinutes);
            return start;
        }
        throw new IllegalArgumentException("Unsupported source.start.mode: " + mode);
    }

    private void refreshDataWatermarkIfNeeded(long pollMillis) throws SQLException {
        long now = System.currentTimeMillis();
        if (cachedSafeWindowEnd != null && now < nextWatermarkRefreshAt) {
            return;
        }

        LocalDateTime maxCollectTime = queryMaxCollectTime();
        watermarkQueries.inc();
        nextWatermarkRefreshAt = now + pollMillis;
        if (maxCollectTime == null) {
            cachedDorisMaxCollectTime = null;
            cachedSafeWindowEnd = null;
            LOG.warn("Doris watermark query returned NULL MAX(collectTime). Waiting for data.");
            return;
        }

        int stableDelayMinutes = Math.max(0, config.getInt("source.doris.stable.delay.minutes", 60));
        int windowMinutes = config.getInt("window.size.minutes", 5);
        LocalDateTime safeEnd = WindowRange.floorToWindow(
                maxCollectTime.minusMinutes(stableDelayMinutes), windowMinutes);

        boolean changed = !maxCollectTime.equals(cachedDorisMaxCollectTime)
                || !safeEnd.equals(cachedSafeWindowEnd);
        cachedDorisMaxCollectTime = maxCollectTime;
        cachedSafeWindowEnd = safeEnd;
        if (changed) {
            LOG.info("Doris data watermark updated: maxCollectTime={}, stableDelay={}m, safeWindowEnd={}, cursor={}",
                    maxCollectTime.format(WindowRange.DATETIME), stableDelayMinutes,
                    safeEnd.format(WindowRange.DATETIME), formatNullable(nextWindowStart));
        }
    }

    private LocalDateTime queryMaxCollectTime() throws SQLException {
        String table = validatedTable();
        int lookbackDays = Math.max(1, config.getInt("source.watermark.lookback.days", 3));
        LocalDate today = ZonedDateTime.now(Clock.systemUTC()).withZoneSameInstant(zoneId).toLocalDate();
        LocalDate lower = today.minusDays(lookbackDays - 1L);
        String sql = "SELECT MAX(collectTime) FROM " + table
                + " WHERE dayTime >= '" + lower + "' AND dayTime <= '" + today + "'";

        LOG.debug("Query Doris data watermark: {}", sql);
        try (Connection connection = newConnection()) {
            currentConnection = connection;
            try (Statement statement = connection.createStatement(ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY)) {
                currentStatement = statement;
                int timeout = config.getInt("source.watermark.query.timeout.seconds", 120);
                if (timeout > 0) {
                    statement.setQueryTimeout(timeout);
                }
                try (ResultSet rs = statement.executeQuery(sql)) {
                    if (!rs.next() || rs.getObject(1) == null) {
                        return null;
                    }
                    return TimeUtils.parse(rs.getObject(1), zoneId).getLocalDateTime();
                }
            } finally {
                currentStatement = null;
            }
        } finally {
            currentConnection = null;
        }
    }

    private WindowQueryResult queryAndAnalyze(WindowRange window) throws SQLException {
        String table = validatedTable();
        String sql = "SELECT " + SELECT_FIELDS + " FROM " + table + " WHERE " + window.toDorisFilter();
        FiveMinuteWindowAnalyzer analyzer = new FiveMinuteWindowAnalyzer(config, historyStore, window);
        long progressEvery = Math.max(0L, config.getLong("source.progress.log.rows", 1000000L));
        long rows = 0L;

        LOG.info("Start Doris five-minute query: window={}, sql={}", window, sql);
        long startedAt = System.currentTimeMillis();

        try (Connection connection = newConnection()) {
            currentConnection = connection;
            try (Statement statement = connection.createStatement(ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY)) {
                currentStatement = statement;
                if (config.getBoolean("doris.jdbc.streaming.enabled", true)) {
                    statement.setFetchSize(Integer.MIN_VALUE);
                } else {
                    statement.setFetchSize(config.getInt("doris.jdbc.fetch.size", 5000));
                }
                int queryTimeoutSeconds = config.getInt("doris.jdbc.query.timeout.seconds", 1800);
                if (queryTimeoutSeconds > 0) {
                    statement.setQueryTimeout(queryTimeoutSeconds);
                }

                try (ResultSet rs = statement.executeQuery(sql)) {
                    while (running && rs.next()) {
                        try {
                            TimeUtils.ParsedTime parsedTime = TimeUtils.parse(rs.getObject(1), zoneId);
                            MetricRecord record = new MetricRecord(
                                    parsedTime.getText(),
                                    parsedTime.getEpochMillis(),
                                    safeString(rs.getString(2)),
                                    safeString(rs.getString(3)),
                                    safeString(rs.getString(4)),
                                    nonNegative(rs.getLong(5)),
                                    nonNegative(rs.getLong(6)),
                                    nonNegative(rs.getLong(7)),
                                    nonNegative(rs.getLong(8)),
                                    nonNegative(rs.getLong(9)));
                            analyzer.add(record, parsedTime.getLocalDateTime());
                            rows++;
                            if (progressEvery > 0L && rows % progressEvery == 0L) {
                                LOG.info("Doris query progress: window={}, rows={}", window, rows);
                            }
                        } catch (RuntimeException rowError) {
                            badRows.inc();
                            LOG.warn("Skip malformed Doris row in window {}: {}", window, rowError.toString());
                        }
                    }
                }
            } finally {
                currentStatement = null;
            }
        } finally {
            currentConnection = null;
        }

        if (!running) {
            throw new SQLException("Source cancelled while reading Doris window " + window);
        }

        WindowAnalysisResult analysis = analyzer.finish();
        long elapsedMs = System.currentTimeMillis() - startedAt;
        LOG.info("Doris query analyzed: window={}, rows={}, elapsedMs={}, alerts={}, contexts={}",
                window, rows, elapsedMs, analysis.getAlerts().size(), analyzer.getLargeContextCount());
        return new WindowQueryResult(rows, analyzer.getOffHoursRowCount(), analyzer.getLargeContextCount(), analysis);
    }

    private Connection newConnection() throws SQLException {
        return DriverManager.getConnection(
                config.get("doris.jdbc.url"), config.get("doris.username"), config.get("doris.password"));
    }

    private String validatedTable() {
        String table = config.get("doris.table");
        if (!SAFE_TABLE.matcher(table).matches()) {
            throw new IllegalArgumentException("Unsafe doris.table: " + table);
        }
        return table;
    }

    private static String safeString(String value) {
        return value == null ? "" : value;
    }

    private static long nonNegative(long value) {
        return Math.max(0L, value);
    }

    private static String formatNullable(LocalDateTime value) {
        return value == null ? "null" : value.format(WindowRange.DATETIME);
    }

    private void sleepInterruptibly(long millis) throws InterruptedException {
        long remaining = millis;
        while (running && remaining > 0L) {
            long chunk = Math.min(1000L, remaining);
            Thread.sleep(chunk);
            remaining -= chunk;
        }
    }

    @Override
    public void cancel() {
        running = false;
        closeQuietly(currentStatement);
        closeQuietly(currentConnection);
    }

    private static void closeQuietly(AutoCloseable closeable) {
        if (closeable == null) {
            return;
        }
        try {
            closeable.close();
        } catch (Exception ignored) {
            // Best effort cancellation path.
        }
    }

    @Override
    public void snapshotState(FunctionSnapshotContext context) throws Exception {
        synchronized (stateMutex) {
            cursorState.clear();
            if (nextWindowStart != null) {
                cursorState.add(nextWindowStart.format(WindowRange.DATETIME));
            }

            pairHistoryState.clear();
            for (HistoricalBaselineStore.PairSnapshot snapshot : historyStore.snapshotPairs()) {
                pairHistoryState.add(snapshot);
            }

            contextHistoryState.clear();
            for (HistoricalBaselineStore.ContextSnapshot snapshot : historyStore.snapshotContexts()) {
                contextHistoryState.add(snapshot);
            }
        }
    }

    @Override
    public void initializeState(FunctionInitializationContext context) throws Exception {
        this.stateMutex = new Object();
        this.historyStore = new HistoricalBaselineStore(config);

        cursorState = context.getOperatorStateStore().getListState(new ListStateDescriptor<>(
                "next-doris-five-minute-window-v2", String.class));
        pairHistoryState = context.getOperatorStateStore().getListState(new ListStateDescriptor<>(
                "large-traffic-pair-history-v2", HistoricalBaselineStore.PairSnapshot.class));
        contextHistoryState = context.getOperatorStateStore().getListState(new ListStateDescriptor<>(
                "large-traffic-context-history-v2", HistoricalBaselineStore.ContextSnapshot.class));

        if (context.isRestored()) {
            for (String value : cursorState.get()) {
                if (value != null && !value.trim().isEmpty()) {
                    nextWindowStart = LocalDateTime.parse(value.trim(), WindowRange.DATETIME);
                    break;
                }
            }
            for (HistoricalBaselineStore.PairSnapshot snapshot : pairHistoryState.get()) {
                historyStore.restorePair(snapshot);
            }
            for (HistoricalBaselineStore.ContextSnapshot snapshot : contextHistoryState.get()) {
                historyStore.restoreContext(snapshot);
            }
        }
    }

    private static final class WindowQueryResult {
        private final long rowCount;
        private final long offHoursRowCount;
        private final int contextCount;
        private final WindowAnalysisResult analysis;

        private WindowQueryResult(long rowCount, long offHoursRowCount, int contextCount,
                                  WindowAnalysisResult analysis) {
            this.rowCount = rowCount;
            this.offHoursRowCount = offHoursRowCount;
            this.contextCount = contextCount;
            this.analysis = analysis;
        }
    }
}

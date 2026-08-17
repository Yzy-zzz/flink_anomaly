package cn.ac.iie.anomaly.source;

import cn.ac.iie.anomaly.config.AppConfig;
import cn.ac.iie.anomaly.config.WindowRange;
import cn.ac.iie.anomaly.model.AlertRecord;
import cn.ac.iie.anomaly.model.MetricRecord;
import cn.ac.iie.anomaly.rule.FiveMinuteWindowAnalyzer;
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
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.util.List;
import java.util.regex.Pattern;

/**
 * Long-running source that polls Doris one closed five-minute window at a time.
 *
 * Important consistency design:
 * - raw rows are NOT emitted to downstream operators;
 * - a complete window is analyzed locally with bounded Sketches;
 * - only after the SQL ResultSet has been fully consumed are alerts emitted;
 * - the next-window cursor is advanced in the same checkpoint-lock critical section.
 *
 * Therefore a failure in the middle of a large Doris query can safely re-read that whole window
 * without double-counting partially read rows in downstream Flink window state.
 */
public class DorisPollingAlertSource extends RichParallelSourceFunction<AlertRecord>
        implements CheckpointedFunction {

    private static final Logger LOG = LoggerFactory.getLogger(DorisPollingAlertSource.class);
    private static final long serialVersionUID = 1L;
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
    private transient ZoneId zoneId;
    private transient Connection currentConnection;
    private transient Statement currentStatement;

    private transient Counter processedWindows;
    private transient Counter queriedRows;
    private transient Counter emittedAlerts;
    private transient Counter badRows;
    private transient Counter queryFailures;

    public DorisPollingAlertSource(AppConfig config) {
        this.config = config;
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        this.zoneId = ZoneId.of(config.get("business.timezone", "Asia/Shanghai"));
        Class.forName("com.mysql.cj.jdbc.Driver");

        processedWindows = getRuntimeContext().getMetricGroup().counter("processedWindows");
        queriedRows = getRuntimeContext().getMetricGroup().counter("queriedRows");
        emittedAlerts = getRuntimeContext().getMetricGroup().counter("emittedAlerts");
        badRows = getRuntimeContext().getMetricGroup().counter("badRows");
        queryFailures = getRuntimeContext().getMetricGroup().counter("queryFailures");
    }

    @Override
    public void run(SourceContext<AlertRecord> ctx) throws Exception {
        if (getRuntimeContext().getNumberOfParallelSubtasks() != 1) {
            throw new IllegalStateException(
                    "DorisPollingAlertSource must run with parallelism=1. "
                            + "Increase Doris/Flink parallelism only after implementing query sharding + Sketch merge.");
        }

        final int windowMinutes = config.getInt("window.size.minutes", 5);
        final long idlePollMillis = Math.max(1000L,
                config.getLong("source.poll.interval.seconds", 15L) * 1000L);
        final long retryMillis = Math.max(1000L,
                config.getLong("source.retry.interval.seconds", 30L) * 1000L);
        final int maxFailures = Math.max(1, config.getInt("source.retry.max.failures", 10));
        int consecutiveFailures = 0;

        if (nextWindowStart == null) {
            nextWindowStart = WindowRange.initialStart(config, Clock.systemUTC());
            LOG.info("No restored cursor. Initial Doris window cursor={}", nextWindowStart.format(WindowRange.DATETIME));
        } else {
            LOG.info("Restored Doris window cursor from checkpoint: {}", nextWindowStart.format(WindowRange.DATETIME));
        }

        while (running) {
            LocalDateTime latestClosedEnd = WindowRange.latestClosedEnd(config, Clock.systemUTC());
            LocalDateTime nextWindowEnd = nextWindowStart.plusMinutes(windowMinutes);

            if (nextWindowEnd.isAfter(latestClosedEnd)) {
                sleepInterruptibly(idlePollMillis);
                continue;
            }

            WindowRange window = WindowRange.fromStart(nextWindowStart, windowMinutes, zoneId);
            try {
                WindowQueryResult result = queryAndAnalyze(window);

                // Emission + cursor advance are one logical source action relative to checkpointing.
                synchronized (ctx.getCheckpointLock()) {
                    for (AlertRecord alert : result.alerts) {
                        ctx.collect(alert);
                        emittedAlerts.inc();
                    }
                    nextWindowStart = window.getEnd();
                }

                processedWindows.inc();
                queriedRows.inc(result.rowCount);
                consecutiveFailures = 0;
                LOG.info("Window finished: {}, rows={}, offHoursRows={}, alerts={}, nextCursor={}",
                        window, result.rowCount, result.offHoursRowCount, result.alerts.size(),
                        nextWindowStart.format(WindowRange.DATETIME));
            } catch (SQLException e) {
                queryFailures.inc();
                consecutiveFailures++;
                LOG.error("Doris query failed for window {} (failure {}/{}). Cursor is NOT advanced.",
                        window, consecutiveFailures, maxFailures, e);
                if (consecutiveFailures >= maxFailures) {
                    throw e;
                }
                sleepInterruptibly(retryMillis);
            }
        }
    }

    private WindowQueryResult queryAndAnalyze(WindowRange window) throws SQLException {
        String table = config.get("doris.table");
        if (!SAFE_TABLE.matcher(table).matches()) {
            throw new IllegalArgumentException("Unsafe doris.table: " + table);
        }

        String sql = "SELECT " + SELECT_FIELDS + " FROM " + table + " WHERE " + window.toDorisFilter();
        FiveMinuteWindowAnalyzer analyzer = new FiveMinuteWindowAnalyzer(config);
        long progressEvery = Math.max(0L, config.getLong("source.progress.log.rows", 1000000L));
        long rows = 0L;

        LOG.info("Start Doris five-minute query: window={}, sql={}", window, sql);
        long startedAt = System.currentTimeMillis();

        try (Connection connection = DriverManager.getConnection(
                config.get("doris.jdbc.url"), config.get("doris.username"), config.get("doris.password"))) {
            currentConnection = connection;
            try (Statement statement = connection.createStatement(ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY)) {
                currentStatement = statement;
                if (config.getBoolean("doris.jdbc.streaming.enabled", true)) {
                    // Connector/J documented row-streaming mode: forward-only + read-only + MIN_VALUE.
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

        List<AlertRecord> alerts = analyzer.finish();
        long elapsedMs = System.currentTimeMillis() - startedAt;
        LOG.info("Doris query analyzed: window={}, rows={}, elapsedMs={}, alerts={}",
                window, rows, elapsedMs, alerts.size());
        return new WindowQueryResult(rows, analyzer.getOffHoursRowCount(), alerts);
    }

    private static String safeString(String value) {
        return value == null ? "" : value;
    }

    private static long nonNegative(long value) {
        return Math.max(0L, value);
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
        cursorState.clear();
        if (nextWindowStart != null) {
            cursorState.add(nextWindowStart.format(WindowRange.DATETIME));
        }
    }

    @Override
    public void initializeState(FunctionInitializationContext context) throws Exception {
        ListStateDescriptor<String> descriptor = new ListStateDescriptor<>(
                "next-doris-five-minute-window", String.class);
        cursorState = context.getOperatorStateStore().getListState(descriptor);
        if (context.isRestored()) {
            for (String value : cursorState.get()) {
                if (value != null && !value.trim().isEmpty()) {
                    nextWindowStart = LocalDateTime.parse(value.trim(), WindowRange.DATETIME);
                    break;
                }
            }
        }
    }

    private static final class WindowQueryResult {
        private final long rowCount;
        private final long offHoursRowCount;
        private final List<AlertRecord> alerts;

        private WindowQueryResult(long rowCount, long offHoursRowCount, List<AlertRecord> alerts) {
            this.rowCount = rowCount;
            this.offHoursRowCount = offHoursRowCount;
            this.alerts = alerts;
        }
    }
}

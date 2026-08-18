package cn.ac.iie.anomaly.sink;

import org.apache.kafka.clients.producer.ProducerInterceptor;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Instant;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Kafka Producer 发送结果监控拦截器。
 *
 * <p>默认不再对每条记录打印 INFO 日志，而是按固定时间间隔输出一次汇总，避免高吞吐时日志爆量。
 * 真正的发送失败仍然立即打印 ERROR；第一条 broker ACK 成功时仍打印一次 KAFKA_WRITE_VERIFIED。
 *
 * <p>注意：这里的汇总是 Kafka 传输侧统计。业务 5 分钟检测窗口的异常类型分布，
 * 由 DorisPollingAlertSource 的 ALERT_WINDOW_SUMMARY 输出，两者职责分开。
 */
public final class KafkaLoggingProducerInterceptor implements ProducerInterceptor<byte[], byte[]> {
    private static final Logger LOG = LoggerFactory.getLogger(KafkaLoggingProducerInterceptor.class);

    public static final String SUMMARY_INTERVAL_MS_CONFIG =
            "net.traffic.kafka.monitor.delivery.summary.interval.ms";
    public static final String PER_RECORD_LOG_ENABLED_CONFIG =
            "net.traffic.kafka.monitor.delivery.per.record.log.enabled";

    private static final long DEFAULT_SUMMARY_INTERVAL_MS = 300_000L;
    private static final DateTimeFormatter SUMMARY_TIME_FORMAT =
            DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss").withZone(ZoneId.systemDefault());

    private final AtomicLong attemptedRecords = new AtomicLong();
    private final AtomicLong attemptedBytes = new AtomicLong();
    private final AtomicLong successRecords = new AtomicLong();
    private final AtomicLong successBytes = new AtomicLong();
    private final AtomicLong failedRecords = new AtomicLong();
    private final AtomicLong inFlightRecords = new AtomicLong();
    private final AtomicBoolean firstSuccessfulAckLogged = new AtomicBoolean(false);
    private final AtomicLong lastImmediateFailureBucketStart = new AtomicLong(Long.MIN_VALUE);

    private volatile String clientId = "unknown";
    private volatile long summaryIntervalMs = DEFAULT_SUMMARY_INTERVAL_MS;
    private volatile boolean perRecordLogEnabled = false;

    // 汇总窗口的快照。所有读写都由 summaryLock 保护，防止 onSend 与 ACK 回调并发输出重复汇总。
    private final Object summaryLock = new Object();
    private long currentBucketStartMs = -1L;
    private long lastAttemptedRecords = 0L;
    private long lastAttemptedBytes = 0L;
    private long lastSuccessRecords = 0L;
    private long lastSuccessBytes = 0L;
    private long lastFailedRecords = 0L;

    @Override
    public void configure(Map<String, ?> configs) {
        Object configuredClientId = configs.get("client.id");
        if (configuredClientId != null && !configuredClientId.toString().trim().isEmpty()) {
            clientId = configuredClientId.toString().trim();
        }

        summaryIntervalMs = positiveLong(
                configs.get(SUMMARY_INTERVAL_MS_CONFIG), DEFAULT_SUMMARY_INTERVAL_MS);
        perRecordLogEnabled = booleanValue(
                configs.get(PER_RECORD_LOG_ENABLED_CONFIG), false);
        currentBucketStartMs = alignBucketStart(System.currentTimeMillis());

        LOG.info(
                "KAFKA_MONITOR_READY clientId={} summaryIntervalMs={} perRecordLogging={} "
                        + "message=Kafka delivery INFO logs are aggregated; failures are still logged immediately",
                clientId,
                summaryIntervalMs,
                perRecordLogEnabled);
    }

    @Override
    public ProducerRecord<byte[], byte[]> onSend(ProducerRecord<byte[], byte[]> record) {
        // 先切换汇总桶，再把当前记录计入新桶，避免边界时第一条记录被算进上一个 5 分钟区间。
        maybeLogSummary(System.currentTimeMillis(), false);

        long valueBytes = record.value() == null ? 0L : record.value().length;
        long keyBytes = record.key() == null ? 0L : record.key().length;
        long recordBytes = keyBytes + valueBytes;
        long attempts = attemptedRecords.incrementAndGet();
        long totalAttemptBytes = attemptedBytes.addAndGet(recordBytes);
        long inFlight = inFlightRecords.incrementAndGet();

        if (perRecordLogEnabled) {
            LOG.info(
                    "KAFKA_SEND_ATTEMPT clientId={} topic={} bytes={} keyBytes={} valueBytes={} "
                            + "attemptedRecords={} attemptedBytes={} inFlight={}",
                    clientId,
                    record.topic(),
                    recordBytes,
                    keyBytes,
                    valueBytes,
                    attempts,
                    totalAttemptBytes,
                    inFlight);
        }
        return record;
    }

    @Override
    public void onAcknowledgement(RecordMetadata metadata, Exception exception) {
        // ACK 按实际到达时间计入 Kafka 传输汇总区间。
        maybeLogSummary(System.currentTimeMillis(), false);
        long inFlight = Math.max(0L, inFlightRecords.decrementAndGet());

        if (exception == null && metadata != null) {
            long keyBytes = Math.max(0, metadata.serializedKeySize());
            long valueBytes = Math.max(0, metadata.serializedValueSize());
            long recordBytes = keyBytes + valueBytes;
            long successes = successRecords.incrementAndGet();
            long totalSuccessBytes = successBytes.addAndGet(recordBytes);

            if (firstSuccessfulAckLogged.compareAndSet(false, true)) {
                LOG.info(
                        "KAFKA_WRITE_VERIFIED clientId={} topic={} partition={} offset={} "
                                + "message=First broker acknowledgement received; Kafka connection/auth/topic write path is working",
                        clientId,
                        metadata.topic(),
                        metadata.partition(),
                        metadata.offset());
            }

            if (perRecordLogEnabled) {
                LOG.info(
                        "KAFKA_SEND_SUCCESS clientId={} topic={} partition={} offset={} bytes={} "
                                + "successRecords={} successBytes={} failedRecords={} inFlight={}",
                        clientId,
                        metadata.topic(),
                        metadata.partition(),
                        metadata.offset(),
                        recordBytes,
                        successes,
                        totalSuccessBytes,
                        failedRecords.get(),
                        inFlight);
            }
            return;
        }

        long failures = failedRecords.incrementAndGet();
        String errorType = exception == null ? "unknown" : exception.getClass().getName();
        String errorMessage = exception == null ? "unknown" : String.valueOf(exception.getMessage());

        // 默认每个 5 分钟传输区间只立即打印第一条失败，避免 Kafka 故障时 ERROR 也被刷屏；
        // 该区间全部失败数仍会完整进入 KAFKA_DELIVERY_SUMMARY。
        long failureBucketStart = alignBucketStart(System.currentTimeMillis());
        long previousFailureBucket = lastImmediateFailureBucketStart.getAndSet(failureBucketStart);
        if (perRecordLogEnabled || previousFailureBucket != failureBucketStart) {
            LOG.error(
                    "KAFKA_SEND_FAILED clientId={} failedRecords={} successRecords={} attemptedRecords={} "
                            + "attemptedBytes={} inFlight={} errorType={} errorMessage={} "
                            + "message=First send failure in current summary interval; remaining failures are aggregated",
                    clientId,
                    failures,
                    successRecords.get(),
                    attemptedRecords.get(),
                    attemptedBytes.get(),
                    inFlight,
                    errorType,
                    errorMessage,
                    exception);
        }
    }

    private void maybeLogSummary(long nowMs, boolean force) {
        synchronized (summaryLock) {
            if (currentBucketStartMs < 0L) {
                currentBucketStartMs = alignBucketStart(nowMs);
            }

            long nowBucketStart = alignBucketStart(nowMs);
            if (!force && nowBucketStart <= currentBucketStartMs) {
                return;
            }

            long totalAttempts = attemptedRecords.get();
            long totalAttemptBytes = attemptedBytes.get();
            long totalSuccesses = successRecords.get();
            long totalSuccessBytes = successBytes.get();
            long totalFailures = failedRecords.get();

            long intervalAttempts = totalAttempts - lastAttemptedRecords;
            long intervalAttemptBytes = totalAttemptBytes - lastAttemptedBytes;
            long intervalSuccesses = totalSuccesses - lastSuccessRecords;
            long intervalSuccessBytes = totalSuccessBytes - lastSuccessBytes;
            long intervalFailures = totalFailures - lastFailedRecords;
            long avgSuccessBytes = intervalSuccesses == 0L ? 0L : intervalSuccessBytes / intervalSuccesses;

            // 正常跨桶时输出上一个完整桶；close() 强制输出当前未满桶。
            long intervalEndMs = force ? nowMs : nowBucketStart;
            if (intervalAttempts > 0L || intervalSuccesses > 0L || intervalFailures > 0L || force) {
                LOG.info(
                        "KAFKA_DELIVERY_SUMMARY clientId={} intervalStart={} intervalEnd={} intervalMs={} "
                                + "attemptedRecords={} successfulRecords={} failedRecords={} attemptedBytes={} "
                                + "successBytes={} avgSuccessBytes={} inFlight={} totalAttemptedRecords={} "
                                + "totalSuccessfulRecords={} totalFailedRecords={}",
                        clientId,
                        formatTime(currentBucketStartMs),
                        formatTime(intervalEndMs),
                        Math.max(0L, intervalEndMs - currentBucketStartMs),
                        intervalAttempts,
                        intervalSuccesses,
                        intervalFailures,
                        intervalAttemptBytes,
                        intervalSuccessBytes,
                        avgSuccessBytes,
                        inFlightRecords.get(),
                        totalAttempts,
                        totalSuccesses,
                        totalFailures);
            }

            lastAttemptedRecords = totalAttempts;
            lastAttemptedBytes = totalAttemptBytes;
            lastSuccessRecords = totalSuccesses;
            lastSuccessBytes = totalSuccessBytes;
            lastFailedRecords = totalFailures;
            currentBucketStartMs = force ? nowMs : nowBucketStart;
        }
    }

    private static String formatTime(long timestampMs) {
        return SUMMARY_TIME_FORMAT.format(Instant.ofEpochMilli(timestampMs));
    }

    private long alignBucketStart(long timestampMs) {
        return timestampMs - Math.floorMod(timestampMs, summaryIntervalMs);
    }

    private static long positiveLong(Object value, long defaultValue) {
        if (value == null) {
            return defaultValue;
        }
        try {
            long parsed = Long.parseLong(value.toString().trim());
            return parsed > 0L ? parsed : defaultValue;
        } catch (NumberFormatException e) {
            return defaultValue;
        }
    }

    private static boolean booleanValue(Object value, boolean defaultValue) {
        return value == null ? defaultValue : Boolean.parseBoolean(value.toString().trim());
    }

    @Override
    public void close() {
        maybeLogSummary(System.currentTimeMillis(), true);
        LOG.info(
                "KAFKA_MONITOR_CLOSED clientId={} attemptedRecords={} attemptedBytes={} "
                        + "successRecords={} successBytes={} failedRecords={} inFlight={}",
                clientId,
                attemptedRecords.get(),
                attemptedBytes.get(),
                successRecords.get(),
                successBytes.get(),
                failedRecords.get(),
                inFlightRecords.get());
    }
}

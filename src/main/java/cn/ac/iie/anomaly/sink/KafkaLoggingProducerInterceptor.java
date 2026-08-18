package cn.ac.iie.anomaly.sink;

import org.apache.kafka.clients.producer.ProducerInterceptor;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Kafka Producer 发送结果日志拦截器。
 *
 * <p>这个拦截器由 KafkaProducer 自己实例化，因此回调发生在真正的 Producer 发送链路中：
 * onSend 记录准备发送的数据量；onAcknowledgement 记录 broker ACK 成功或最终失败。
 * 这样可以继续使用 Flink KafkaSink，而不需要为了拿 callback 改成自定义 KafkaProducer Sink。
 */
public final class KafkaLoggingProducerInterceptor implements ProducerInterceptor<byte[], byte[]> {
    private static final Logger LOG = LoggerFactory.getLogger(KafkaLoggingProducerInterceptor.class);

    private final AtomicLong attemptedRecords = new AtomicLong();
    private final AtomicLong attemptedBytes = new AtomicLong();
    private final AtomicLong successRecords = new AtomicLong();
    private final AtomicLong successBytes = new AtomicLong();
    private final AtomicLong failedRecords = new AtomicLong();
    private final AtomicLong inFlightRecords = new AtomicLong();
    private final AtomicBoolean firstSuccessfulAckLogged = new AtomicBoolean(false);

    private volatile String clientId = "unknown";

    @Override
    public void configure(Map<String, ?> configs) {
        Object configuredClientId = configs.get("client.id");
        if (configuredClientId != null && !configuredClientId.toString().trim().isEmpty()) {
            clientId = configuredClientId.toString().trim();
        }
        LOG.info("KAFKA_MONITOR_READY clientId={} deliveryResultLogging=true", clientId);
    }

    @Override
    public ProducerRecord<byte[], byte[]> onSend(ProducerRecord<byte[], byte[]> record) {
        long valueBytes = record.value() == null ? 0L : record.value().length;
        long keyBytes = record.key() == null ? 0L : record.key().length;
        long recordBytes = keyBytes + valueBytes;
        long attempts = attemptedRecords.incrementAndGet();
        long totalAttemptBytes = attemptedBytes.addAndGet(recordBytes);
        long inFlight = inFlightRecords.incrementAndGet();

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
        return record;
    }

    @Override
    public void onAcknowledgement(RecordMetadata metadata, Exception exception) {
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
            return;
        }

        long failures = failedRecords.incrementAndGet();
        String errorType = exception == null ? "unknown" : exception.getClass().getName();
        String errorMessage = exception == null ? "unknown" : String.valueOf(exception.getMessage());
        LOG.error(
                "KAFKA_SEND_FAILED clientId={} failedRecords={} successRecords={} attemptedRecords={} "
                        + "attemptedBytes={} inFlight={} errorType={} errorMessage={}",
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

    @Override
    public void close() {
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

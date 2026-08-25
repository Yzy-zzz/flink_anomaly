package cn.ac.iie.anomaly.source;

import cn.ac.iie.anomaly.config.AppConfig;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Collections;
import java.util.List;
import java.util.Properties;

/**
 * Startup preflight for Kafka input. It checks broker/topic metadata plus topic READ/group offset
 * authorization without joining/rebalancing the consumer group and without committing offsets.
 */
public final class KafkaInputConnectivityCheck {
    private static final Logger LOG = LoggerFactory.getLogger(KafkaInputConnectivityCheck.class);

    private KafkaInputConnectivityCheck() {
    }

    public static int verify(AppConfig config) {
        if (!config.getBoolean("kafka.input.connection.check.enabled", true)) {
            LOG.info("KAFKA_INPUT_CONNECTION_CHECK_SKIPPED reason=disabled");
            return -1;
        }

        String bootstrap = config.get("kafka.bootstrap.servers", "localhost:9092");
        String topic = config.get("kafka.input.topic", "METRIC-ENDPOINT");
        String groupId = config.get("kafka.input.group.id", "net-traffic-sentinel-metric-endpoint-v1");
        long timeoutMs = Math.max(1000L, config.getLong("kafka.input.connection.timeout.ms", 15000L));
        boolean failFast = config.getBoolean("kafka.input.connection.fail-fast", true);

        Properties props = KafkaSourceFactory.buildConsumerProperties(config);
        props.remove("commit.offsets.on.checkpoint");
        props.remove("partition.discovery.interval.ms");
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrap);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        props.put(ConsumerConfig.CLIENT_ID_CONFIG,
                config.get("kafka.input.client.id.prefix", "net-traffic-sentinel-input") + "-startup-probe");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest");
        props.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, "1");
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");

        try (KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props)) {
            LOG.info("KAFKA_INPUT_CONNECTION_CHECK_START bootstrapServers={}, topic={}, groupId={}, timeoutMs={}",
                    bootstrap, topic, groupId, timeoutMs);

            List<PartitionInfo> partitions = consumer.partitionsFor(topic, Duration.ofMillis(timeoutMs));
            if (partitions == null || partitions.isEmpty()) {
                throw new IllegalStateException("Kafka input topic has no visible partitions: " + topic);
            }

            TopicPartition probePartition = new TopicPartition(topic, partitions.get(0).partition());

            // OffsetFetch checks authorization for the configured group.id without joining the group, so an
            // already-running consumer group is not rebalanced by this startup probe.
            consumer.committed(Collections.singleton(probePartition), Duration.ofMillis(timeoutMs));

            // Manual assignment does not use group coordination. Fetch at most one record to force topic READ
            // authorization. Nothing is committed, so the real Flink KafkaSource start position is unaffected.
            consumer.assign(Collections.singleton(probePartition));
            consumer.seekToBeginning(Collections.singleton(probePartition));
            consumer.poll(Duration.ofMillis(Math.min(timeoutMs, 2000L)));

            LOG.info("KAFKA_INPUT_CONNECTION_OK topic={}, partitions={}, probePartition={}, groupId={}, "
                            + "message=broker/topic/read/group preflight succeeded; no group rebalance and no offset commit",
                    topic, partitions.size(), probePartition.partition(), groupId);
            return partitions.size();
        } catch (Exception e) {
            LOG.error("KAFKA_INPUT_CONNECTION_FAILED bootstrapServers={}, topic={}, groupId={}, errorType={}, errorMessage={}",
                    bootstrap, topic, groupId, e.getClass().getName(), String.valueOf(e.getMessage()), e);
            if (failFast) {
                throw new IllegalStateException("Kafka input startup preflight failed", e);
            }
            return -1;
        }
    }
}

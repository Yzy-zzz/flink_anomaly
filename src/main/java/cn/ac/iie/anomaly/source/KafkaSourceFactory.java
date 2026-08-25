package cn.ac.iie.anomaly.source;

import cn.ac.iie.anomaly.config.AppConfig;
import cn.ac.iie.anomaly.util.TimeUtils;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.KafkaSourceBuilder;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.OffsetResetStrategy;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.ZoneId;
import java.util.Locale;
import java.util.Properties;

/** Builds the unbounded Kafka input source for METRIC-ENDPOINT-compatible JSON records. */
public final class KafkaSourceFactory {
    private static final Logger LOG = LoggerFactory.getLogger(KafkaSourceFactory.class);

    private KafkaSourceFactory() {
    }

    public static KafkaSource<String> create(AppConfig config) {
        String bootstrap = config.get("kafka.bootstrap.servers", "localhost:9092");
        String topic = config.get("kafka.input.topic", "METRIC-ENDPOINT");
        String groupId = config.get("kafka.input.group.id", "net-traffic-sentinel-metric-endpoint-v1");

        Properties props = buildConsumerProperties(config);
        KafkaSourceBuilder<String> builder = KafkaSource.<String>builder()
                .setBootstrapServers(bootstrap)
                .setTopics(topic)
                .setGroupId(groupId)
                .setClientIdPrefix(config.get("kafka.input.client.id.prefix", "net-traffic-sentinel-input"))
                .setProperties(props)
                .setStartingOffsets(startingOffsets(config))
                .setValueOnlyDeserializer(new SimpleStringSchema());

        LOG.info("Creating Kafka input source: bootstrapServers={}, topic={}, groupId={}, startMode={}, clientIdPrefix={}",
                bootstrap,
                topic,
                groupId,
                config.get("kafka.input.start.mode", "latest"),
                config.get("kafka.input.client.id.prefix", "net-traffic-sentinel-input"));
        return builder.build();
    }

    public static Properties buildConsumerProperties(AppConfig config) {
        Properties props = buildSecurityProperties(config);
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
        props.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG,
                String.valueOf(config.getInt("kafka.input.max.poll.records", 5000)));
        props.put(ConsumerConfig.REQUEST_TIMEOUT_MS_CONFIG,
                String.valueOf(config.getInt("kafka.input.request.timeout.ms", 30000)));
        props.put(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG,
                String.valueOf(config.getInt("kafka.input.session.timeout.ms", 45000)));
        props.put(ConsumerConfig.HEARTBEAT_INTERVAL_MS_CONFIG,
                String.valueOf(config.getInt("kafka.input.heartbeat.interval.ms", 15000)));
        props.put("commit.offsets.on.checkpoint",
                String.valueOf(config.getBoolean("kafka.input.commit.offsets.on.checkpoint", true)));
        props.put("partition.discovery.interval.ms",
                String.valueOf(config.getLong("kafka.input.partition.discovery.interval.ms", 60000L)));
        return props;
    }

    public static Properties buildSecurityProperties(AppConfig config) {
        Properties props = new Properties();
        props.put("security.protocol", config.get("kafka.security.protocol", "SASL_PLAINTEXT"));
        props.put("sasl.kerberos.service.name", config.get("kafka.sasl.kerberos.service.name", "kafka"));
        props.put("sasl.mechanism", config.get("kafka.sasl.mechanism", "GSSAPI"));
        return props;
    }

    private static OffsetsInitializer startingOffsets(AppConfig config) {
        String mode = config.get("kafka.input.start.mode", "latest").trim().toLowerCase(Locale.ROOT);
        if ("earliest".equals(mode)) {
            return OffsetsInitializer.earliest();
        }
        if ("latest".equals(mode)) {
            return OffsetsInitializer.latest();
        }
        if ("committed".equals(mode)) {
            return OffsetsInitializer.committedOffsets(committedResetStrategy(config));
        }
        if ("timestamp".equals(mode)) {
            String text = config.get("kafka.input.start.timestamp", "").trim();
            if (text.isEmpty()) {
                throw new IllegalArgumentException(
                        "kafka.input.start.timestamp is required when kafka.input.start.mode=timestamp");
            }
            ZoneId zone = ZoneId.of(config.get("business.timezone", "Asia/Shanghai"));
            long epochMillis = TimeUtils.parse(text, zone).getEpochMillis();
            return OffsetsInitializer.timestamp(epochMillis);
        }
        throw new IllegalArgumentException(
                "kafka.input.start.mode must be one of: earliest, latest, committed, timestamp");
    }

    private static OffsetResetStrategy committedResetStrategy(AppConfig config) {
        String reset = config.get("kafka.input.committed.offset.reset", "earliest")
                .trim().toLowerCase(Locale.ROOT);
        if ("earliest".equals(reset)) {
            return OffsetResetStrategy.EARLIEST;
        }
        if ("latest".equals(reset)) {
            return OffsetResetStrategy.LATEST;
        }
        throw new IllegalArgumentException(
                "kafka.input.committed.offset.reset must be earliest or latest");
    }
}

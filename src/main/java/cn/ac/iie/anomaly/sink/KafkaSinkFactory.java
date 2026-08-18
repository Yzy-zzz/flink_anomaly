package cn.ac.iie.anomaly.sink;

import cn.ac.iie.anomaly.config.AppConfig;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.connector.base.DeliveryGuarantee;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.connector.kafka.sink.KafkaSinkBuilder;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

/**
 * Kafka 告警输出工厂。
 *
 * 当前 Kafka 集群使用 Kerberos/GSSAPI 认证，Producer 参数默认如下：
 * security.protocol=SASL_PLAINTEXT
 * sasl.kerberos.service.name=kafka
 * sasl.mechanism=GSSAPI
 *
 * Kerberos 凭据本身不写入工程代码；运行环境需要提供有效的 Kerberos ticket/JAAS/krb5 配置。
 */
public final class KafkaSinkFactory {
    private static final Logger LOG = LoggerFactory.getLogger(KafkaSinkFactory.class);

    private KafkaSinkFactory() {
    }

    public static KafkaSink<String> create(AppConfig config) {
        String bootstrapServers = config.get("kafka.bootstrap.servers", "localhost:9092");
        String topic = config.get("kafka.topic", "anomaly_alert");
        String delivery = config.get("kafka.delivery.guarantee", "at_least_once");

        Properties producerProperties = buildProducerProperties(config);

        LOG.info(
                "Creating Kafka alert sink: bootstrapServers={}, topic={}, deliveryGuarantee={}, clientId={}, "
                        + "securityProtocol={}, saslMechanism={}, kerberosServiceName={}, deliveryMonitoring={}, "
                        + "deliverySummaryIntervalMs={}, perRecordDeliveryLogging={}",
                bootstrapServers,
                topic,
                delivery,
                producerProperties.getProperty(ProducerConfig.CLIENT_ID_CONFIG),
                producerProperties.getProperty("security.protocol"),
                producerProperties.getProperty("sasl.mechanism"),
                producerProperties.getProperty("sasl.kerberos.service.name"),
                config.getBoolean("kafka.monitor.delivery.log.enabled", true),
                config.getLong("kafka.monitor.delivery.summary.interval.ms", 300000L),
                config.getBoolean("kafka.monitor.delivery.per-record.log.enabled", false));

        KafkaSinkBuilder<String> builder = KafkaSink.<String>builder()
                .setBootstrapServers(bootstrapServers)
                .setKafkaProducerConfig(producerProperties)
                .setRecordSerializer(KafkaRecordSerializationSchema.builder()
                        .setTopic(topic)
                        .setValueSerializationSchema(new SimpleStringSchema())
                        .build());

        if ("exactly_once".equalsIgnoreCase(delivery)) {
            builder.setDeliveryGuarantee(DeliveryGuarantee.EXACTLY_ONCE)
                    .setTransactionalIdPrefix(config.get("kafka.transactional.id.prefix", "anomaly-alert-"));
        } else if ("at_least_once".equalsIgnoreCase(delivery)) {
            builder.setDeliveryGuarantee(DeliveryGuarantee.AT_LEAST_ONCE);
        } else {
            throw new IllegalArgumentException("Unsupported kafka.delivery.guarantee: " + delivery);
        }
        return builder.build();
    }

    static Properties buildSecurityProperties(AppConfig config) {
        Properties properties = new Properties();

        // 按目标 Kafka 集群要求设置 SASL/Kerberos 参数。
        properties.put(
                "security.protocol",
                config.get("kafka.security.protocol", "SASL_PLAINTEXT"));
        properties.put(
                "sasl.kerberos.service.name",
                config.get("kafka.sasl.kerberos.service.name", "kafka"));
        properties.put(
                "sasl.mechanism",
                config.get("kafka.sasl.mechanism", "GSSAPI"));

        return properties;
    }

    static Properties buildProducerProperties(AppConfig config) {
        Properties properties = buildSecurityProperties(config);
        properties.put(
                ProducerConfig.CLIENT_ID_CONFIG,
                config.get("kafka.client.id", "net-traffic-sentinel-alert"));

        if (config.getBoolean("kafka.monitor.delivery.log.enabled", true)) {
            properties.put(
                    ProducerConfig.INTERCEPTOR_CLASSES_CONFIG,
                    KafkaLoggingProducerInterceptor.class.getName());
            properties.put(
                    KafkaLoggingProducerInterceptor.SUMMARY_INTERVAL_MS_CONFIG,
                    String.valueOf(config.getLong("kafka.monitor.delivery.summary.interval.ms", 300000L)));
            properties.put(
                    KafkaLoggingProducerInterceptor.PER_RECORD_LOG_ENABLED_CONFIG,
                    String.valueOf(config.getBoolean("kafka.monitor.delivery.per-record.log.enabled", false)));
        }

        return properties;
    }
}

package cn.ac.iie.anomaly.sink;

import cn.ac.iie.anomaly.config.AppConfig;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.connector.base.DeliveryGuarantee;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.connector.kafka.sink.KafkaSinkBuilder;

public final class KafkaSinkFactory {
    private KafkaSinkFactory() {
    }

    public static KafkaSink<String> create(AppConfig config) {
        String delivery = config.get("kafka.delivery.guarantee", "at_least_once");
        KafkaSinkBuilder<String> builder = KafkaSink.<String>builder()
                .setBootstrapServers(config.get("kafka.bootstrap.servers", "localhost:9092"))
                .setRecordSerializer(KafkaRecordSerializationSchema.builder()
                        .setTopic(config.get("kafka.topic", "anomaly_alert"))
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
}

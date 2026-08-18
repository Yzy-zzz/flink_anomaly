package cn.ac.iie.anomaly.sink;

import cn.ac.iie.anomaly.config.AppConfig;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.DescribeClusterResult;
import org.apache.kafka.clients.admin.TopicDescription;
import org.apache.kafka.common.Node;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;
import java.util.Collections;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

/**
 * 在 TaskManager 侧启动 Kafka 输出分支时执行一次连接/元数据探测。
 *
 * <p>探测成功说明当前运行容器能完成 Kafka 网络连接、SASL/Kerberos 认证并读取目标 topic 元数据。
 * 真正的 Produce 权限与写入结果仍以 KafkaLoggingProducerInterceptor 的 broker ACK 日志为准。
 */
public final class KafkaConnectivityProbe extends RichMapFunction<String, String> {
    private static final Logger LOG = LoggerFactory.getLogger(KafkaConnectivityProbe.class);

    private final AppConfig config;

    public KafkaConnectivityProbe(AppConfig config) {
        this.config = config;
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        if (!config.getBoolean("kafka.monitor.connection.check.enabled", true)) {
            LOG.info("KAFKA_CONNECTION_CHECK_SKIPPED reason=disabled");
            return;
        }

        // 只让第 0 个 subtask 做主动探测，避免提高并行度后所有 subtask 同时创建 AdminClient。
        if (getRuntimeContext().getIndexOfThisSubtask() != 0) {
            return;
        }

        String bootstrapServers = config.get("kafka.bootstrap.servers", "localhost:9092");
        String topic = config.get("kafka.topic", "anomaly_alert");
        long timeoutMs = config.getLong("kafka.monitor.connection.timeout.ms", 10000L);
        boolean failFast = config.getBoolean("kafka.monitor.connection.fail-fast", false);

        Properties properties = KafkaSinkFactory.buildSecurityProperties(config);
        properties.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        properties.put(AdminClientConfig.CLIENT_ID_CONFIG,
                config.get("kafka.client.id", "net-traffic-sentinel-alert") + "-probe");
        properties.put(AdminClientConfig.REQUEST_TIMEOUT_MS_CONFIG, String.valueOf(timeoutMs));
        properties.put(AdminClientConfig.DEFAULT_API_TIMEOUT_MS_CONFIG, String.valueOf(timeoutMs));

        LOG.info(
                "KAFKA_CONNECTION_CHECK_START bootstrapServers={} topic={} timeoutMs={} securityProtocol={} "
                        + "saslMechanism={} kerberosServiceName={}",
                bootstrapServers,
                topic,
                timeoutMs,
                properties.getProperty("security.protocol"),
                properties.getProperty("sasl.mechanism"),
                properties.getProperty("sasl.kerberos.service.name"));

        try (AdminClient adminClient = AdminClient.create(properties)) {
            DescribeClusterResult cluster = adminClient.describeCluster();
            String clusterId = cluster.clusterId().get(timeoutMs, TimeUnit.MILLISECONDS);
            Collection<Node> nodes = cluster.nodes().get(timeoutMs, TimeUnit.MILLISECONDS);
            Map<String, TopicDescription> topics = adminClient
                    .describeTopics(Collections.singleton(topic))
                    .all()
                    .get(timeoutMs, TimeUnit.MILLISECONDS);
            TopicDescription topicDescription = topics.get(topic);
            int partitions = topicDescription == null ? -1 : topicDescription.partitions().size();

            LOG.info(
                    "KAFKA_CONNECTION_OK bootstrapServers={} topic={} clusterId={} brokerCount={} partitions={} "
                            + "message=Network/SASL/topic metadata check succeeded",
                    bootstrapServers,
                    topic,
                    clusterId,
                    nodes == null ? 0 : nodes.size(),
                    partitions);
        } catch (Exception e) {
            LOG.error(
                    "KAFKA_CONNECTION_FAILED bootstrapServers={} topic={} timeoutMs={} errorType={} errorMessage={}",
                    bootstrapServers,
                    topic,
                    timeoutMs,
                    e.getClass().getName(),
                    String.valueOf(e.getMessage()),
                    e);
            if (failFast) {
                throw e;
            }
        }
    }

    @Override
    public String map(String value) {
        return value;
    }
}

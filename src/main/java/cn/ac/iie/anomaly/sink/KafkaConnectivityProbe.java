package cn.ac.iie.anomaly.sink;

import cn.ac.iie.anomaly.config.AppConfig;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.DescribeClusterResult;
import org.apache.kafka.clients.admin.TopicDescription;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.errors.UnknownTopicOrPartitionException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;
import java.util.Collections;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

/**
 * 在 TaskManager 侧启动 Kafka 输出分支时执行一次连接/元数据探测。
 *
 * <p>集群连通性和 topic 元数据检查分开记录，避免 topic 暂时未就绪时误报成“Kafka 连接失败”。
 * 真正的 Produce 权限与写入结果仍以 KafkaLoggingProducerInterceptor 的 broker ACK 为准。
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

        boolean clusterVerified = false;
        try (AdminClient adminClient = AdminClient.create(properties)) {
            // 第一层：只验证 broker 集群是否可连通、SASL/Kerberos 是否能完成并读取集群元数据。
            DescribeClusterResult cluster = adminClient.describeCluster();
            String clusterId = cluster.clusterId().get(timeoutMs, TimeUnit.MILLISECONDS);
            Collection<Node> nodes = cluster.nodes().get(timeoutMs, TimeUnit.MILLISECONDS);
            int brokerCount = nodes == null ? 0 : nodes.size();
            clusterVerified = true;

            LOG.info(
                    "KAFKA_CONNECTION_OK bootstrapServers={} clusterId={} brokerCount={} "
                            + "message=Kafka network/SASL cluster metadata check succeeded",
                    bootstrapServers,
                    clusterId,
                    brokerCount);

            // 第二层：topic 元数据单独检查。UnknownTopicOrPartition 是 topic 层问题，不再伪装成连接失败。
            try {
                Map<String, TopicDescription> topics = adminClient
                        .describeTopics(Collections.singleton(topic))
                        .all()
                        .get(timeoutMs, TimeUnit.MILLISECONDS);
                TopicDescription topicDescription = topics.get(topic);
                int partitions = topicDescription == null ? -1 : topicDescription.partitions().size();
                LOG.info(
                        "KAFKA_TOPIC_METADATA_OK topic={} partitions={} clusterId={} brokerCount={}",
                        topic,
                        partitions,
                        clusterId,
                        brokerCount);
            } catch (Exception topicException) {
                Throwable cause = unwrap(topicException);
                if (cause instanceof UnknownTopicOrPartitionException && !failFast) {
                    LOG.warn(
                            "KAFKA_TOPIC_METADATA_NOT_READY topic={} clusterId={} brokerCount={} "
                                    + "errorType={} errorMessage={} message=Cluster connection is OK; "
                                    + "producer may refresh/create topic metadata later",
                            topic,
                            clusterId,
                            brokerCount,
                            cause.getClass().getName(),
                            String.valueOf(cause.getMessage()));
                    return;
                }

                LOG.error(
                        "KAFKA_TOPIC_METADATA_FAILED topic={} clusterId={} brokerCount={} errorType={} errorMessage={}",
                        topic,
                        clusterId,
                        brokerCount,
                        cause.getClass().getName(),
                        String.valueOf(cause.getMessage()),
                        topicException);
                if (failFast) {
                    throw topicException;
                }
            }
        } catch (Exception e) {
            // 如果集群检查已经成功，则这里可能是 fail-fast=true 时主动抛出的 topic 异常，
            // 不再重复误报成 KAFKA_CONNECTION_FAILED。
            if (clusterVerified) {
                if (failFast) {
                    throw e;
                }
                return;
            }

            Throwable cause = unwrap(e);
            LOG.error(
                    "KAFKA_CONNECTION_FAILED bootstrapServers={} timeoutMs={} errorType={} errorMessage={}",
                    bootstrapServers,
                    timeoutMs,
                    cause.getClass().getName(),
                    String.valueOf(cause.getMessage()),
                    e);
            if (failFast) {
                throw e;
            }
        }
    }

    private static Throwable unwrap(Throwable error) {
        Throwable current = error;
        while (current instanceof ExecutionException && current.getCause() != null) {
            current = current.getCause();
        }
        return current;
    }

    @Override
    public String map(String value) {
        return value;
    }
}

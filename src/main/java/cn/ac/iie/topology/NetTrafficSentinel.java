package cn.ac.iie.topology;

import cn.ac.iie.anomaly.config.AppConfig;
import cn.ac.iie.anomaly.config.WindowRange;
import cn.ac.iie.anomaly.model.AlertRecord;
import cn.ac.iie.anomaly.model.MetricRecord;
import cn.ac.iie.anomaly.sink.AlertJsonMapper;
import cn.ac.iie.anomaly.sink.KafkaConnectivityProbe;
import cn.ac.iie.anomaly.sink.KafkaSinkFactory;
import cn.ac.iie.anomaly.sink.LocalLogSink;
import cn.ac.iie.anomaly.sink.NoOpStringSink;
import cn.ac.iie.anomaly.source.KafkaInputConnectivityCheck;
import cn.ac.iie.anomaly.source.KafkaSourceFactory;
import cn.ac.iie.anomaly.source.KafkaWindowAlertProcessFunction;
import cn.ac.iie.anomaly.source.MetricJsonParser;
import cn.ac.iie.anomaly.util.TimeUtils;
import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.time.ZoneId;
import java.util.Locale;

/**
 * NetTrafficSentinel Kafka edition.
 *
 * Pipeline:
 * Kafka(METRIC-ENDPOINT JSON) -> normalize/validate -> collectTime event-time + watermark
 * -> ordered 5-minute anomaly analyzer -> JSON -> log/Kafka(anomaly_alert).
 */
public class NetTrafficSentinel {
    private static final Logger LOG = LoggerFactory.getLogger(NetTrafficSentinel.class);

    public static void main(String[] args) throws Exception {
        AppConfig config = AppConfig.load(args);
        validateConfig(config);

        // Preflight before submitting the streaming graph. It discovers the actual partition count and
        // verifies broker/topic/read/group access without committing any consumer offset.
        int discoveredPartitions = KafkaInputConnectivityCheck.verify(config);
        int sourceParallelism = resolveSourceParallelism(config, discoveredPartitions);

        LOG.info("Starting NetTrafficSentinel Kafka edition: jobName={}, inputTopic={}, inputGroupId={}, "
                        + "inputStartMode={}, discoveredPartitions={}, sourceParallelism={}, window={}m, "
                        + "maxOutOfOrderness={}m, timezone={}, logSink={}, kafkaSink={}",
                config.get("job.name", "NET-TRAFFIC-ANOMALY"),
                config.get("kafka.input.topic", "METRIC-ENDPOINT"),
                config.get("kafka.input.group.id", "net-traffic-sentinel-metric-endpoint-v1"),
                config.get("kafka.input.start.mode", "latest"),
                discoveredPartitions,
                sourceParallelism,
                config.getInt("window.size.minutes", 5),
                config.getLong("kafka.input.max.out.of.orderness.minutes", 360L),
                config.get("business.timezone", "Asia/Shanghai"),
                config.getBoolean("alert.output.log.enabled", false),
                config.getBoolean("alert.output.kafka.enabled", true));

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setRuntimeMode(RuntimeExecutionMode.STREAMING);
        env.setRestartStrategy(RestartStrategies.fixedDelayRestart(
                config.getInt("restart.attempts", 20),
                Time.seconds(config.getLong("restart.delay.seconds", 10L))));
        configureCheckpointing(env, config);

        KafkaSource<String> kafkaSource = KafkaSourceFactory.create(config);
        DataStreamSource<String> rawJson = env.fromSource(
                kafkaSource,
                WatermarkStrategy.noWatermarks(),
                "metric-endpoint-kafka-source");
        rawJson.setParallelism(sourceParallelism);
        rawJson.uid("metric-endpoint-kafka-source-v1");

        SingleOutputStreamOperator<MetricRecord> metrics = rawJson
                .flatMap(new MetricJsonParser(config))
                .name("metric-json-normalizer")
                .uid("metric-json-normalizer-v1")
                .setParallelism(sourceParallelism);

        long maxOutOfOrdernessMinutes = config.getLong("kafka.input.max.out.of.orderness.minutes", 360L);
        long idleTimeoutMinutes = config.getLong("kafka.input.watermark.idle.timeout.minutes", 2L);
        WatermarkStrategy<MetricRecord> watermarkStrategy = WatermarkStrategy
                .<MetricRecord>forBoundedOutOfOrderness(Duration.ofMinutes(maxOutOfOrdernessMinutes))
                .withTimestampAssigner(new SerializableTimestampAssigner<MetricRecord>() {
                    private static final long serialVersionUID = 1L;
                    @Override
                    public long extractTimestamp(MetricRecord element, long recordTimestamp) {
                        return element.getCollectTimestamp();
                    }
                });
        if (idleTimeoutMinutes > 0L) {
            watermarkStrategy = watermarkStrategy.withIdleness(Duration.ofMinutes(idleTimeoutMinutes));
        }

        SingleOutputStreamOperator<MetricRecord> eventTimeMetrics = metrics
                .assignTimestampsAndWatermarks(watermarkStrategy)
                .name("collect-time-watermarks")
                .uid("collect-time-watermarks-v1")
                .setParallelism(sourceParallelism);

        // Global historical Pair EMA/context state intentionally stays single-parallelism to retain the
        // ordered semantics of the old Doris source. KafkaSource/parsing may still be parallel.
        SingleOutputStreamOperator<AlertRecord> alerts = eventTimeMetrics
                .keyBy(new KeySelector<MetricRecord, Integer>() {
                    private static final long serialVersionUID = 1L;
                    @Override
                    public Integer getKey(MetricRecord value) {
                        return 0;
                    }
                })
                .process(new KafkaWindowAlertProcessFunction(config))
                .name("five-minute-kafka-anomaly-analyzer")
                .uid("five-minute-kafka-anomaly-analyzer-v1")
                .setParallelism(1);

        SingleOutputStreamOperator<String> jsonAlerts = alerts
                .map(new AlertJsonMapper())
                .name("alert-to-json")
                .uid("alert-to-json-v3")
                .setParallelism(1);

        boolean logEnabled = config.getBoolean("alert.output.log.enabled", false);
        boolean kafkaEnabled = config.getBoolean("alert.output.kafka.enabled", true);
        if (logEnabled) {
            jsonAlerts.addSink(new LocalLogSink()).name("alert-local-log").uid("alert-local-log-v3");
        }
        if (kafkaEnabled) {
            SingleOutputStreamOperator<String> kafkaAlerts = jsonAlerts
                    .map(new KafkaConnectivityProbe(config))
                    .name("kafka-output-connectivity-probe")
                    .uid("kafka-output-connectivity-probe-v2");
            kafkaAlerts.sinkTo(KafkaSinkFactory.create(config))
                    .name("alert-kafka")
                    .uid("alert-kafka-v3");
        }
        if (!logEnabled && !kafkaEnabled) {
            jsonAlerts.addSink(new NoOpStringSink()).name("alert-noop").uid("alert-noop-v3");
        }

        env.execute(config.get("job.name", "NET-TRAFFIC-ANOMALY"));
    }

    private static int resolveSourceParallelism(AppConfig config, int discoveredPartitions) {
        int configured = config.getInt("kafka.input.source.parallelism", 0);
        if (configured > 0) {
            return configured;
        }
        int maxAuto = Math.max(1, config.getInt("kafka.input.source.parallelism.max", 4));
        if (discoveredPartitions > 0) {
            return Math.max(1, Math.min(discoveredPartitions, maxAuto));
        }
        return 1;
    }

    private static void configureCheckpointing(StreamExecutionEnvironment env, AppConfig config) {
        boolean checkpointEnabled = config.getBoolean("checkpoint.enabled", true);
        String delivery = config.get("kafka.output.delivery.guarantee",
                config.get("kafka.delivery.guarantee", "at_least_once"));
        boolean kafkaEnabled = config.getBoolean("alert.output.kafka.enabled", true);
        if (kafkaEnabled
                && ("exactly_once".equalsIgnoreCase(delivery) || "at_least_once".equalsIgnoreCase(delivery))
                && !checkpointEnabled) {
            throw new IllegalArgumentException("Kafka output " + delivery + " requires checkpoint.enabled=true");
        }
        if (!checkpointEnabled) {
            return;
        }

        env.enableCheckpointing(config.getLong("checkpoint.interval.ms", 300000L), CheckpointingMode.EXACTLY_ONCE);
        CheckpointConfig checkpointConfig = env.getCheckpointConfig();
        checkpointConfig.setCheckpointTimeout(config.getLong("checkpoint.timeout.ms", 900000L));
        checkpointConfig.setMinPauseBetweenCheckpoints(config.getLong("checkpoint.min.pause.ms", 30000L));
        checkpointConfig.setMaxConcurrentCheckpoints(1);
        checkpointConfig.setExternalizedCheckpointCleanup(
                CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);

        String checkpointDir = config.get("checkpoint.storage.path", "").trim();
        if (!checkpointDir.isEmpty()) {
            checkpointConfig.setCheckpointStorage(checkpointDir);
            LOG.info("Checkpoint storage: {}", checkpointDir);
        } else {
            LOG.warn("checkpoint.storage.path is empty. Configure an HDFS path for production YARN recovery.");
        }
    }

    private static void validateConfig(AppConfig config) {
        int windowMinutes = config.getInt("window.size.minutes", 5);
        if (windowMinutes <= 0 || 60 % windowMinutes != 0) {
            throw new IllegalArgumentException("window.size.minutes must be a positive divisor of 60");
        }
        if (config.getLong("kafka.input.max.out.of.orderness.minutes", 360L) < 0L) {
            throw new IllegalArgumentException("kafka.input.max.out.of.orderness.minutes must be >= 0");
        }
        if (config.getLong("kafka.input.watermark.idle.timeout.minutes", 2L) < 0L) {
            throw new IllegalArgumentException("kafka.input.watermark.idle.timeout.minutes must be >= 0");
        }
        if (config.getInt("kafka.input.source.parallelism", 0) < 0) {
            throw new IllegalArgumentException("kafka.input.source.parallelism must be 0(auto) or positive");
        }
        if (config.get("kafka.input.topic", "METRIC-ENDPOINT").trim().isEmpty()) {
            throw new IllegalArgumentException("kafka.input.topic must not be blank");
        }
        if (config.get("kafka.input.group.id", "").trim().isEmpty()) {
            throw new IllegalArgumentException("kafka.input.group.id must not be blank");
        }

        String startMode = config.get("kafka.input.start.mode", "latest").toLowerCase(Locale.ROOT);
        if (!"earliest".equals(startMode)
                && !"latest".equals(startMode)
                && !"committed".equals(startMode)
                && !"timestamp".equals(startMode)) {
            throw new IllegalArgumentException(
                    "kafka.input.start.mode must be earliest, latest, committed or timestamp");
        }
        if ("timestamp".equals(startMode)) {
            String text = config.get("kafka.input.start.timestamp", "").trim();
            if (text.isEmpty()) {
                throw new IllegalArgumentException("kafka.input.start.timestamp is required for timestamp mode");
            }
            ZoneId zone = ZoneId.of(config.get("business.timezone", "Asia/Shanghai"));
            TimeUtils.parse(text, zone);
        }

        String deviceId = config.get("alert.device.id");
        if (!deviceId.matches("\\d{6}")) {
            throw new IllegalArgumentException("alert.device.id must be exactly 6 digits");
        }
        if (!config.getBoolean("rule.large.enabled", true)
                && !config.getBoolean("rule.offhours.enabled", true)) {
            throw new IllegalArgumentException("Both anomaly rules are disabled");
        }

        int topN = config.getInt("rule.offhours.topn", 100);
        int capacity = config.getInt("rule.offhours.sketch.capacity", 2048);
        if (topN <= 0 || capacity <= topN) {
            throw new IllegalArgumentException("rule.offhours.sketch.capacity must be greater than positive TopN");
        }

        double bq = config.getDouble("rule.large.bytes.quantile", 0.999d);
        double pq = config.getDouble("rule.large.pkts.quantile", 0.999d);
        if (bq <= 0d || bq >= 1d || pq <= 0d || pq >= 1d) {
            throw new IllegalArgumentException("large-traffic quantiles must be between 0 and 1");
        }
        if (config.getInt("rule.large.candidate.capacity", 20000) <= 0) {
            throw new IllegalArgumentException("rule.large.candidate.capacity must be positive");
        }

        int contextSlot = config.getInt("history.context.slot.minutes", 5);
        if (contextSlot <= 0 || 60 % contextSlot != 0) {
            throw new IllegalArgumentException("history.context.slot.minutes must be a positive divisor of 60");
        }
        if (config.getInt("history.context.retention.days", 7) <= 0) {
            throw new IllegalArgumentException("history.context.retention.days must be positive");
        }
        int contextMinDays = config.getInt("history.context.min.days", 2);
        if (contextMinDays <= 0 || contextMinDays > config.getInt("history.context.retention.days", 7)) {
            throw new IllegalArgumentException("history.context.min.days must be in [1, retention.days]");
        }
        if (config.getInt("history.pair.ttl.days", 7) <= 0) {
            throw new IllegalArgumentException("history.pair.ttl.days must be positive");
        }
        if (config.getInt("history.pair.max.entries", 200000) < 1000) {
            throw new IllegalArgumentException("history.pair.max.entries must be >= 1000");
        }
        double alpha = config.getDouble("history.pair.ema.alpha", 0.10d);
        if (alpha <= 0d || alpha > 1d) {
            throw new IllegalArgumentException("history.pair.ema.alpha must be in (0,1]");
        }

        // This is a brand-new Kafka topology; deliberately reject any attempt to restore the old Doris state UID.
        if (config.getBoolean("migration.restore.old.doris.state", false)) {
            throw new IllegalArgumentException(
                    "Old Doris source state migration is intentionally unsupported; start this Kafka job with fresh state");
        }
    }
}

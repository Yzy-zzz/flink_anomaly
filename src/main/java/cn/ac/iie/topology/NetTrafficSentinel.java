package cn.ac.iie.topology;

import cn.ac.iie.anomaly.config.AppConfig;
import cn.ac.iie.anomaly.model.AlertRecord;
import cn.ac.iie.anomaly.sink.AlertJsonMapper;
import cn.ac.iie.anomaly.sink.KafkaSinkFactory;
import cn.ac.iie.anomaly.sink.LocalLogSink;
import cn.ac.iie.anomaly.sink.NoOpStringSink;
import cn.ac.iie.anomaly.source.DorisPollingAlertSource;
import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Entry point for the long-running five-minute Doris anomaly detector. */
public class NetTrafficSentinel {
    private static final Logger LOG = LoggerFactory.getLogger(NetTrafficSentinel.class);

    public static void main(String[] args) throws Exception {
        AppConfig config = AppConfig.load(args);
        validateConfig(config);

        LOG.info("Starting NetTrafficSentinel: jobName={}, table={}, window={}m, timezone={}, logSink={}, kafkaSink={}",
                config.get("job.name", "NET-TRAFFIC-ANOMALY"),
                config.get("doris.table"),
                config.getInt("window.size.minutes", 5),
                config.get("business.timezone", "Asia/Shanghai"),
                config.getBoolean("alert.output.log.enabled", true),
                config.getBoolean("alert.output.kafka.enabled", false));

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setRuntimeMode(RuntimeExecutionMode.STREAMING);
        env.setRestartStrategy(RestartStrategies.fixedDelayRestart(
                config.getInt("restart.attempts", 20),
                Time.seconds(config.getLong("restart.delay.seconds", 10L))));
        configureCheckpointing(env, config);

        DataStreamSource<AlertRecord> alerts = env.addSource(new DorisPollingAlertSource(config));
        alerts.name("doris-five-minute-polling-source");
        alerts.uid("doris-five-minute-polling-source-v2");
        alerts.setParallelism(config.getInt("source.parallelism", 1));

        SingleOutputStreamOperator<String> jsonAlerts = alerts
                .map(new AlertJsonMapper())
                .name("alert-to-json")
                .uid("alert-to-json-v2");

        boolean logEnabled = config.getBoolean("alert.output.log.enabled", true);
        boolean kafkaEnabled = config.getBoolean("alert.output.kafka.enabled", false);
        if (logEnabled) {
            jsonAlerts.addSink(new LocalLogSink()).name("alert-local-log").uid("alert-local-log-v2");
        }
        if (kafkaEnabled) {
            jsonAlerts.sinkTo(KafkaSinkFactory.create(config)).name("alert-kafka").uid("alert-kafka-v2");
        }
        if (!logEnabled && !kafkaEnabled) {
            jsonAlerts.addSink(new NoOpStringSink()).name("alert-noop").uid("alert-noop-v2");
        }

        env.execute(config.get("job.name", "NET-TRAFFIC-ANOMALY"));
    }

    private static void configureCheckpointing(StreamExecutionEnvironment env, AppConfig config) {
        boolean checkpointEnabled = config.getBoolean("checkpoint.enabled", true);
        String delivery = config.get("kafka.delivery.guarantee", "at_least_once");
        boolean kafkaEnabled = config.getBoolean("alert.output.kafka.enabled", false);
        if (kafkaEnabled
                && ("exactly_once".equalsIgnoreCase(delivery) || "at_least_once".equalsIgnoreCase(delivery))
                && !checkpointEnabled) {
            throw new IllegalArgumentException("Kafka " + delivery + " requires checkpoint.enabled=true");
        }
        if (!checkpointEnabled) {
            return;
        }

        env.enableCheckpointing(config.getLong("checkpoint.interval.ms", 300000L), CheckpointingMode.EXACTLY_ONCE);
        CheckpointConfig checkpointConfig = env.getCheckpointConfig();
        checkpointConfig.setCheckpointTimeout(config.getLong("checkpoint.timeout.ms", 600000L));
        checkpointConfig.setMinPauseBetweenCheckpoints(config.getLong("checkpoint.min.pause.ms", 30000L));
        checkpointConfig.setMaxConcurrentCheckpoints(1);
        checkpointConfig.setExternalizedCheckpointCleanup(
                CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);

        String checkpointDir = config.get("checkpoint.storage.path", "").trim();
        if (!checkpointDir.isEmpty()) {
            checkpointConfig.setCheckpointStorage(checkpointDir);
            LOG.info("Checkpoint storage: {}", checkpointDir);
        } else {
            LOG.warn("checkpoint.storage.path is empty. Flink will use its configured/default checkpoint storage. "
                    + "For production YARN, configure an HDFS checkpoint path.");
        }
    }

    private static void validateConfig(AppConfig config) {
        int windowMinutes = config.getInt("window.size.minutes", 5);
        if (windowMinutes <= 0 || 60 % windowMinutes != 0) {
            throw new IllegalArgumentException("window.size.minutes must be a positive divisor of 60");
        }
        if (config.getInt("source.parallelism", 1) != 1) {
            throw new IllegalArgumentException("This version requires source.parallelism=1");
        }
        if (config.get("doris.password").startsWith("CHANGE_ME_")) {
            throw new IllegalArgumentException("Use an external application.properties with the Doris password");
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
        if (config.getInt("source.doris.stable.delay.minutes", 60) < 0) {
            throw new IllegalArgumentException("source.doris.stable.delay.minutes must be >= 0");
        }
        String startMode = config.get("source.start.mode", "latest_data");
        if (!"latest_data".equalsIgnoreCase(startMode)
                && !"latest_closed".equalsIgnoreCase(startMode)
                && !"fixed".equalsIgnoreCase(startMode)) {
            throw new IllegalArgumentException("source.start.mode must be latest_data or fixed");
        }
    }
}

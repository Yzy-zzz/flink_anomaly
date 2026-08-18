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

/**
 * 【导读：程序总入口】
 *
 * 这个类可以理解成整个 Flink 作业的“main 方法 + 流水线装配图”。
 * 它本身不负责异常算法，主要做 5 件事：
 * 1. 读取 application.properties；
 * 2. 创建 Flink StreamExecutionEnvironment；
 * 3. 配置重启和 Checkpoint；
 * 4. 把 DorisPollingAlertSource 接到 JSON 转换，再接日志/Kafka Sink；
 * 5. 调用 env.execute() 真正提交并启动作业。
 *
 * 建议初学者先读这个类，建立“Source -> Map -> Sink”的整体印象，
 * 再进入 DorisPollingAlertSource 看数据是如何一批一批被读出来的。
 */
public class NetTrafficSentinel {
    private static final Logger LOG = LoggerFactory.getLogger(NetTrafficSentinel.class);

    public static void main(String[] args) throws Exception {
        // 第一步：加载配置。支持 --config=xxx.properties，也支持项目根目录 application.properties。
        AppConfig config = AppConfig.load(args);
        // 在真正启动 Flink 之前先做参数合法性校验，尽早发现错误配置。
        validateConfig(config);

        LOG.info("Starting NetTrafficSentinel: jobName={}, table={}, window={}m, timezone={}, logSink={}, kafkaSink={}",
                config.get("job.name", "NET-TRAFFIC-ANOMALY"),
                config.get("doris.table"),
                config.getInt("window.size.minutes", 5),
                config.get("business.timezone", "Asia/Shanghai"),
                config.getBoolean("alert.output.log.enabled", true),
                config.getBoolean("alert.output.kafka.enabled", false));

        // Flink 的执行环境。可以把它理解成“搭建整条 DataStream 流水线的工作台”。
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setRuntimeMode(RuntimeExecutionMode.STREAMING);
        env.setRestartStrategy(RestartStrategies.fixedDelayRestart(
                config.getInt("restart.attempts", 20),
                Time.seconds(config.getLong("restart.delay.seconds", 10L))));
        configureCheckpointing(env, config);

        // Source：持续轮询 Doris。注意：本项目不是直接使用 Flink SQL Window，
        // 而是在 Source 内部手工按 5 分钟范围查询 Doris，并输出已经判定好的 AlertRecord。
        DataStreamSource<AlertRecord> alerts = env.addSource(new DorisPollingAlertSource(config));
        alerts.name("doris-five-minute-polling-source");
        alerts.uid("doris-five-minute-polling-source-v2");
        alerts.setParallelism(config.getInt("source.parallelism", 1));

        // Map：把 Java 告警对象序列化成 JSON 字符串，方便写日志或 Kafka。
        SingleOutputStreamOperator<String> jsonAlerts = alerts
                .map(new AlertJsonMapper())
                .name("alert-to-json")
                .uid("alert-to-json-v2");

        boolean logEnabled = config.getBoolean("alert.output.log.enabled", false);
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

        // execute() 之前，上面的代码只是在“描述”作业拓扑；调用 execute() 后才真正提交执行。
        env.execute(config.get("job.name", "NET-TRAFFIC-ANOMALY"));
    }

    /**
     * 配置 Flink Checkpoint。
     *
     * 可以先记住：Checkpoint 就是 Flink 定期给“游标 + 历史基线”拍快照。
     * 任务失败重启后，initializeState() 会从最近一次成功快照恢复，避免从头学习基线。
     */
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
        String deviceId = config.get("alert.device.id");
        if (!deviceId.matches("\\d{6}")) {
            throw new IllegalArgumentException("alert.device.id must be exactly 6 digits");
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

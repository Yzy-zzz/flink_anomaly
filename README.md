# NetTrafficSentinel（Kafka 输入版）

本工程已将原来的 **Doris JDBC 轮询输入**改为 **Flink KafkaSource 持续消费**。异常算法、5 分钟业务窗口、Context 历史基线、Pair EMA、告警 JSON 和 Kafka 输出保持原有语义。

默认数据链路：

```text
Kafka METRIC-ENDPOINT
  -> JSON 校验/归一化
  -> collectTime 事件时间 + Watermark
  -> 5 分钟窗口摘要（允许长时间乱序/延迟）
  -> LargeTraffic / OffHours 检测
  -> HistoricalBaselineStore
  -> AlertRecord -> JSON
  -> Kafka anomaly_alert
```


## 0. 重要：请解压到全新目录，不要覆盖旧 Doris 工程

如果 Maven 编译日志里还出现：

```text
cn/ac/iie/anomaly/source/DorisPollingAlertSource.java
```

说明你当前构建的目录里仍有旧 Doris 版文件。ZIP 覆盖解压不会删除旧文件。

建议把本工程解压成一个新的目录，例如：

```text
D:\Code\B-traffic_behavior\NetTrafficSentinel-Kafka-v2
```

然后在这个新目录执行：

```bash
mvn clean test package
```

本版本额外增加了两层兼容保护：`WindowRange` 恢复旧 `toDorisFilter(...)` 方法用于编译兼容，同时 `pom.xml` 明确排除遗留 `DorisPollingAlertSource.java`。详细说明见 `docs/CLEAN_BUILD_CN.md`。

## 1. 关键结论

### 1.1 不再依赖 Doris

工程已移除 `DorisPollingAlertSource` 和 MySQL Connector 依赖。输入字段直接来自 `METRIC-ENDPOINT` JSON。

算法实际使用字段：

- `collectTime`
- `srcIp`
- `dstIp`
- `protocol`
- `connCount`
- `c2sPkts`
- `s2cPkts`
- `c2sBytes`
- `s2cBytes`

`connBytesSeries`、`connTimeSeries`、`dayHourTime`、`dayTime` 当前检测算法不使用。

示例：

```json
{"c2sBytes":100,"c2sPkts":1,"collectTime":"2026-08-24 13:05:00","connBytesSeries":"100","connCount":1,"connTimeSeries":"1787547942","dayHourTime":"2026-08-24 13","dayTime":"2026-08-24","dstIp":"183.93.235.217","protocol":"UNKNOWN","s2cBytes":0,"s2cPkts":0,"srcIp":"1.224.99.3"}
```

`totalBytes = c2sBytes + s2cBytes`，`totalPkts = c2sPkts + s2cPkts`。

### 1.2 另一个 Flink 程序读取同一 Topic 不会互相抢数据

只要两个作业使用 **不同的 `group.id`**，它们会各自完整消费 `METRIC-ENDPOINT`。

本工程默认：

```properties
kafka.input.group.id=net-traffic-sentinel-metric-endpoint-v1
```

不要把它改成另一个 Flink 作业正在使用的 group.id。

如果两个作业误用了同一个 group.id，Kafka 会把 partitions 分给两个作业，它们会互相“分流”，两边都可能拿不到完整数据。

### 1.3 Partition 数不能假设为 1

Kafka Topic 的 partition 数由实际集群/topic 配置决定，不保证是 1。

程序启动时会主动读取 `METRIC-ENDPOINT` 元数据，并打印：

```text
KAFKA_INPUT_CONNECTION_OK topic=METRIC-ENDPOINT partitions=N ...
```

`kafka.input.source.parallelism=0` 时，会按实际 partition 数自动选择 Source 并行度，并受 `kafka.input.source.parallelism.max` 限制。

异常分析算子仍固定为 1 个并行实例，以保持原来全局 Pair EMA / Context 历史基线的顺序语义。

## 2. Kafka 起始 Offset

配置：

```properties
kafka.input.start.mode=latest
```

可选值：

- `latest`：新部署默认值，只消费启动之后的新消息。
- `earliest`：从 Topic 当前保留范围的最早 offset 开始。
- `committed`：从该 group.id 已提交 offset 开始；没有提交记录时按 `kafka.input.committed.offset.reset` 处理。
- `timestamp`：根据 Kafka **消息自身的 record timestamp** 查找起始 offset。

`timestamp` 模式示例：

```properties
kafka.input.start.mode=timestamp
kafka.input.start.timestamp=2026-08-24 00:00:00
```

注意：这里匹配的是 Kafka ConsumerRecord 的 timestamp，不是 JSON 中的 `collectTime`。正常生产应确认生产端 Kafka record timestamp 与业务时间的关系。

Flink 从自己的 checkpoint 恢复时，checkpoint 中的 Kafka offsets 优先于首次启动配置。

## 3. 延迟/乱序处理

目前不知道最大延迟，因此工程默认：

```properties
kafka.input.max.out.of.orderness.minutes=360
```

即允许约 **6 小时** 的事件时间乱序。

业务窗口仍然以 JSON 的 `collectTime` 为准。例如：

```text
[13:00:00, 13:05:00)
```

只有当 Flink Watermark 越过 `13:05:00` 后，窗口才会 `finish()`、生成告警并更新历史基线。

如果某条记录到达时，该窗口的 Watermark 已经越过窗口结束时间，这条记录会被认定为 `too-late`，丢弃并计入：

```text
kafka-analysis-too-late-rows
```

如果线上发现确实存在超过 6 小时的数据延迟，应提高：

```properties
kafka.input.max.out.of.orderness.minutes=720
```

提高该值的代价是：

1. 告警更晚输出；
2. 同时保持更多未闭合窗口；
3. TaskManager 内存和 checkpoint 体积增加。

此外配置：

```properties
kafka.input.watermark.idle.timeout.minutes=2
```

会把长时间完全没有消息的 Source subtask 标记为 idle，避免整个空闲 subtask 拖住全局 Watermark。

这里的 idleness 位于 JSON 解析后的 subtask 层。如果一个 source subtask 同时负责多个 partition，其中一个 partition 空闲、另一个仍持续有数据，则这个 subtask 不会被标成 idle。若线上 partition 间时间进度差异很大，可把 `kafka.input.source.parallelism` 调到实际 partition 数，再结合延迟指标校准 Watermark。

注意：如果整个 Topic 完全停止产生更晚的事件时间，标准事件时间 Watermark 不会仅因为机器时间流逝自动前进，因此尾部窗口会继续等待，直到后续有更晚事件推动 Watermark。

## 4. 异常 JSON 处理

坏消息不会让整个 Flink 作业失败。

处理规则：

- JSON 无法解析：丢弃；
- `collectTime` 缺失/无法解析：丢弃；
- `srcIp` / `dstIp` 缺失或不是可识别的 IPv4/IPv6 形式：丢弃；
- `protocol` 缺失：按空字符串处理；
- bytes/pkts/connCount 缺失或 null：按 0；
- bytes/pkts/connCount 为负数：按 0；
- 数字字段不是整数：丢弃。

指标：

```text
kafka-input-good-json-rows
kafka-input-bad-json-rows
```

日志只对最初少量坏消息和每 10000 条坏消息抽样输出，避免日志风暴。

## 5. 启动时 Kafka 权限检查

默认开启：

```properties
kafka.input.connection.check.enabled=true
kafka.input.connection.fail-fast=true
```

启动前会：

1. 连接 Kafka broker；
2. 查询 `METRIC-ENDPOINT` topic 元数据；
3. 读取实际 partition 数；
4. 使用配置的真实 group.id 做 OffsetFetch 权限检查，但不加入/rebalance consumer group；
5. 对一个 partition 做手工 assign + 极小 fetch，触发 Topic READ 权限校验；
6. 不提交 offset。

检查失败时默认直接终止启动，避免作业“跑起来但一直读不到数据”。

输出端仍保留原来的 Kafka metadata/Producer delivery 监控。

## 6. Checkpoint 与旧基线

本 Kafka 版本按你的要求 **不继承旧 Doris 作业的历史基线**。

首次启动请使用新的 checkpoint 路径：

```properties
checkpoint.storage.path=hdfs:///flink-checkpoints/net-traffic-sentinel-kafka
```

不要使用旧 Doris 作业 checkpoint/savepoint 恢复本版本。

新版本 checkpoint 保存：

- KafkaSource offsets；
- 等待迟到数据的未闭合 5 分钟窗口摘要；
- Context 历史 t-digest；
- Pair EMA；
- logId 日内序号；
- Flink event-time timers。

因此**本 Kafka 版本自身**发生故障后可以继续从自己的 checkpoint 恢复。

## 7. 主要配置

输入：

```properties
kafka.input.topic=METRIC-ENDPOINT
kafka.input.group.id=net-traffic-sentinel-metric-endpoint-v1
kafka.input.start.mode=latest
kafka.input.max.out.of.orderness.minutes=360
kafka.input.source.parallelism=0
kafka.input.source.parallelism.max=4
```

输出：

```properties
kafka.output.topic=anomaly_alert
kafka.output.delivery.guarantee=at_least_once
```

输入、输出共用：

```properties
kafka.bootstrap.servers=...
kafka.security.protocol=SASL_PLAINTEXT
kafka.sasl.mechanism=GSSAPI
kafka.sasl.kerberos.service.name=kafka
```

## 8. 构建

环境：

- Java 8/11
- Maven
- Flink 1.17.2

构建：

```bash
./build.sh
```

或：

```bash
mvn clean test package
```

Windows 也可以执行：

```bat
build.bat
```

产物：

```text
target/NetTrafficSentinel-1.0-SNAPSHOT.jar
```

## 9. YARN 运行

确认外置 `application.properties` 后：

```bash
./run-yarn.sh
```

重点检查启动日志：

```text
KAFKA_INPUT_CONNECTION_OK
Creating Kafka input source
KAFKA_WINDOW_OPEN
KAFKA_WINDOW_FINALIZED
```

如果长时间只有 `KAFKA_WINDOW_OPEN` 而没有 `KAFKA_WINDOW_FINALIZED`，优先检查：

1. `collectTime` 是否持续向前推进；
2. `kafka.input.max.out.of.orderness.minutes` 是否过大；
3. 是否有 partition/subtask 长期不活跃且 idleness 未生效；
4. Kafka 数据是否已经停止产生。

## 10. 目录

```text
src/main/java/cn/ac/iie/topology/NetTrafficSentinel.java
src/main/java/cn/ac/iie/anomaly/source/KafkaSourceFactory.java
src/main/java/cn/ac/iie/anomaly/source/KafkaInputConnectivityCheck.java
src/main/java/cn/ac/iie/anomaly/source/MetricJsonParser.java
src/main/java/cn/ac/iie/anomaly/source/KafkaWindowAlertProcessFunction.java
src/main/java/cn/ac/iie/anomaly/rule/*
src/main/java/cn/ac/iie/anomaly/history/*
src/main/java/cn/ac/iie/anomaly/sink/*
```

原 Doris 版长文档被保留在 `docs/legacy-doris/`，仅用于历史参考，不适用于当前运行链路。

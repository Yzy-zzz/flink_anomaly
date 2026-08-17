# NetTrafficSentinel 小白部署与代码说明

> 适用环境：Java 8/11、Apache Flink 1.17.2、Apache Doris 2.1.x、YARN。
>
> 这份文档按“第一次接触该项目的人也能照着操作”的方式编写。

---

## 1. 这个程序是做什么的？

程序会一直运行在 YARN 上，每隔一段时间检查：

> “下一个 5 分钟数据窗口是不是已经完整写入 Doris 了？”

如果已经完整，就查询这个窗口的数据，并执行两条异常规则。

例如业务时区为 `Asia/Shanghai`，配置：

```properties
window.size.minutes=5
window.delay.minutes=1
```

在 20:11 左右，程序认为下面这个窗口已经安全闭合：

```text
[20:05:00, 20:10:00)
```

然后查询：

```text
collectTime >= 20:05:00
collectTime <  20:10:00
```

分析完成后，游标推进到：

```text
20:10:00
```

等到下一个窗口可以读取时，再处理：

```text
[20:10:00, 20:15:00)
```

因此，只需要提交一次 Flink 任务。

---

## 2. 为什么不用 Doris Flink Connector 一直读？

Doris Flink Connector 的读取 Source 是 bounded source。

可以理解为：

```text
它擅长：给一个查询范围 -> 把这一批数据读完 -> Source 结束

它不等同于：
Kafka Consumer -> 永远等待未来的新消息
```

所以要做一个不退出、永远每 5 分钟继续读新数据的任务，本工程采用：

```text
Flink 自定义 SourceFunction
        |
        +-- 每次生成一个 5 分钟 SQL
        |
        +-- JDBC 连接 Doris FE 9030
        |
        +-- ResultSet 逐行读取
        |
        +-- 边读取边更新 Sketch
        |
        +-- 整个窗口完成后输出告警
        |
        +-- 等下一个 5 分钟窗口
```

Doris 使用 MySQL 网络协议，因此可以通过 MySQL JDBC Driver 查询。

---

## 3. 为什么 JDBC 不会把几千万行全部放进内存？

普通 JDBC 驱动有可能先把整个 ResultSet 缓存到客户端内存。

本工程创建 Statement 时使用：

```java
connection.createStatement(
    ResultSet.TYPE_FORWARD_ONLY,
    ResultSet.CONCUR_READ_ONLY
);

statement.setFetchSize(Integer.MIN_VALUE);
```

这是 MySQL Connector/J 的 streaming ResultSet 模式。

程序读取一行、处理一行，不需要把整个 5 分钟窗口先变成一个 Java List。

逻辑是：

```text
Doris row 1  -> Sketch
Doris row 2  -> Sketch
Doris row 3  -> Sketch
...
Doris row N  -> Sketch
```

而不是：

```text
Doris -> List<几千万行> -> 再分析
```

---

## 4. 项目目录怎么看？

核心目录：

```text
NetTrafficSentinel/
├── pom.xml
├── application.properties
├── run-yarn.sh
├── build.sh
├── README.md
├── docs/
│   └── BEGINNER_DEPLOYMENT_GUIDE_CN.md
└── src/
    ├── main/
    │   ├── java/
    │   │   └── cn/ac/iie/
    │   │       ├── topology/
    │   │       │   └── NetTrafficSentinel.java
    │   │       └── anomaly/
    │   │           ├── config/
    │   │           ├── model/
    │   │           ├── rule/
    │   │           ├── sink/
    │   │           ├── sketch/
    │   │           ├── source/
    │   │           └── util/
    │   └── resources/
    │       └── application.properties
    └── test/
```

下面逐个解释。

---

# 5. 主类：NetTrafficSentinel.java

路径：

```text
src/main/java/cn/ac/iie/topology/NetTrafficSentinel.java
```

它是整个程序的入口，相当于普通 Java 程序的：

```java
public static void main(String[] args)
```

主要做 5 件事。

### 第 1 件：读取 application.properties

```java
AppConfig config = AppConfig.load(args);
```

提交命令中：

```bash
--config application.properties
```

就是告诉它读取这个文件。

### 第 2 件：创建 Flink 环境

```java
StreamExecutionEnvironment env =
    StreamExecutionEnvironment.getExecutionEnvironment();
```

并设置 streaming mode。

### 第 3 件：开启 checkpoint

checkpoint 主要用于保存：

```text
当前已经处理到哪个 5 分钟窗口
```

例如保存：

```text
nextWindowStart = 2026-08-17 20:15:00
```

任务内部发生失败并恢复时，可以继续从该游标处理。

### 第 4 件：创建 DorisPollingAlertSource

```java
.addSource(new DorisPollingAlertSource(config))
```

这个 Source 是持续运行的核心。

### 第 5 件：告警输出

告警先转 JSON：

```text
AlertRecord -> JSON String
```

再根据开关决定：

```text
日志
Kafka
日志 + Kafka
都不输出
```

---

# 6. 最重要的类：DorisPollingAlertSource.java

路径：

```text
src/main/java/cn/ac/iie/anomaly/source/DorisPollingAlertSource.java
```

它可以理解成一个无限循环：

```text
while (程序没有被停止) {
    判断下一个 5 分钟窗口是否已经闭合;

    if (还没闭合) {
        sleep;
        continue;
    }

    查询这个 5 分钟窗口;
    计算异常;
    输出告警;
    游标 += 5 分钟;
}
```

真实代码还加入了：

- Doris 查询失败重试
- checkpoint 游标恢复
- Flink metrics
- 取消任务时主动关闭 JDBC Statement/Connection
- 坏数据跳过
- SQL 查询进度日志

---

## 6.1 什么叫“游标”？

假设：

```text
nextWindowStart = 20:10
```

窗口大小是 5 分钟，那么它下一次查询：

```text
[20:10,20:15)
```

成功后：

```text
nextWindowStart = 20:15
```

下一次就是：

```text
[20:15,20:20)
```

所以这个时间就是持续处理的核心进度。

---

## 6.2 如果 Doris 查询失败怎么办？

例如正在处理：

```text
[20:10,20:15)
```

查询到一半 Doris 连接断了。

程序**不会**把游标改成 20:15。

仍然保持：

```text
nextWindowStart = 20:10
```

等待：

```properties
source.retry.interval.seconds=30
```

然后重新查询：

```text
[20:10,20:15)
```

默认连续失败：

```properties
source.retry.max.failures=10
```

次以后抛异常，让 Flink restart strategy 接管。

---

## 6.3 为什么不是每读一条 Doris 数据就发给下游？

这是故障恢复设计。

假设一个窗口 1000 万行。

如果已经把前 500 万行发给 Flink window operator，然后发生故障：

```text
checkpoint 中可能已经保存了前 500 万行的聚合状态
```

Source 恢复以后又从 Doris 把完整 1000 万行重读一次，就可能让前半部分重复统计。

所以当前实现采用：

```text
Doris 原始数据
    |
    v
Source 内部 Sketch
    |
    | 整个 ResultSet 完成
    v
最终 AlertRecord
    |
    v
Flink 下游 Sink
```

整个 5 分钟 SQL 没完成之前，不向下游发送半成品。

这样中途失败最多重新计算这一个窗口。

---

# 7. WindowRange.java

路径：

```text
src/main/java/cn/ac/iie/anomaly/config/WindowRange.java
```

负责计算时间窗口。

例如现在业务时间：

```text
20:11:30
```

配置：

```properties
window.size.minutes=5
window.delay.minutes=1
```

先减 landing delay：

```text
20:10:30
```

然后向下对齐到 5 分钟：

```text
20:10:00
```

因此最新闭合窗口的 end 是：

```text
20:10:00
```

最新完整窗口就是：

```text
[20:05,20:10)
```

---

# 8. Doris SQL 是什么样？

实际只读取业务需要的字段：

```sql
SELECT
    collectTime,
    srcIp,
    dstIp,
    protocol,
    connCount,
    c2sPkts,
    s2cPkts,
    c2sBytes,
    s2cBytes
FROM anomaly_detection.metric_endpoint
WHERE ...
```

没有读取：

```text
connTimeSeries
connBytesSeries
```

因为这两个 TEXT 字段对当前规则没有用，读取它们只会增加网络、反序列化和内存压力。

时间条件同时使用：

```text
dayTime
dayHourTime
collectTime
```

例如：

```sql
WHERE dayTime >= '2026-08-17'
  AND dayTime <= '2026-08-17'
  AND dayHourTime >= '2026-08-17 20:00:00'
  AND dayHourTime <  '2026-08-17 21:00:00'
  AND collectTime >= '2026-08-17 20:05:00'
  AND collectTime <  '2026-08-17 20:10:00'
```

`collectTime` 保证精确 5 分钟；前两个时间字段帮助 Doris 做分区/范围裁剪。

---

# 9. FiveMinuteWindowAnalyzer.java

路径：

```text
src/main/java/cn/ac/iie/anomaly/rule/FiveMinuteWindowAnalyzer.java
```

这个类拿到每一行 `MetricRecord` 后，同时给两条规则喂数据。

伪代码：

```java
for each Doris row {
    largeTrafficAccumulator.add(row);

    if (当前时间属于非工作时段) {
        offHoursAccumulator.add(row);
    }
}
```

窗口结束后：

```text
LargeTrafficDetector.detect(...)
OffHoursDetector.detect(...)
```

产生告警。

---

# 10. 规则一：异常大流量

## 10.1 输入指标

```text
currentBytes = c2sBytes + s2cBytes
currentPkts  = c2sPkts  + s2cPkts
```

连接 Key：

```text
srcIp | dstIp | protocol
```

例如：

```text
10.1.1.10|10.2.2.20|TCP
```

---

## 10.2 T-Digest 是干什么的？

它近似回答：

```text
这个 5 分钟窗口中，P99.9 流量大概是多少？
```

如果窗口有 100 万条：

```text
P99.9
```

可以简单理解为“最靠近顶部 0.1% 的流量分界线”。

它不需要把 100 万个数字全部排序保存在内存。

---

## 10.3 Count-Min Sketch 是干什么的？

正常办法可能写：

```java
Map<String, Long> pairByteSum
Map<String, Long> pairPktSum
Map<String, Long> pairCount
```

如果 IP Pair 数量特别大，这些 Map 会非常大。

CMS 改成固定二维数组：

```properties
rule.large.cms.depth=5
rule.large.cms.width=262144
```

三张 long CMS 估算内存：

```text
5 * 262144 * 8 * 3
≈ 30 MiB
```

无论 pair 数量继续增长，CMS 数组大小不增长。

代价是结果是近似值，并且有 hash collision 带来的高估误差。

---

## 10.4 candidate heap 为什么存在？

虽然 T-Digest 能告诉我们 P99.9 是多少，但 T-Digest 本身不能告诉你：

```text
到底是哪些 srcIp/dstIp 位于 P99.9？
```

所以同时保留一个固定大小的“大流量候选池”：

```properties
rule.large.candidate.capacity=20000
```

始终保留窗口内 bytes 最大的一部分记录。

不是保留全部记录。

---

## 10.5 当前异常判断

默认：

```properties
rule.large.bytes.quantile=0.999
rule.large.pkts.quantile=0.999
rule.large.bytes.baseline.multiplier=4.0
rule.large.pkts.baseline.multiplier=4.0
rule.large.extreme.multiplier=2.0
```

算法近似为：

```text
bytesThreshold = max(
    全窗口 bytes P99.9,
    当前 pair 基线 * 4
)

pktsThreshold = max(
    全窗口 pkts P99.9,
    当前 pair 基线 * 4
)
```

最终：

```text
currentBytes > bytesThreshold
AND
(
    currentPkts > pktsThreshold
    OR
    currentBytes > P99.9(bytes) * 2
)
```

---

## 10.6 baseline 到底是什么？

必须特别注意：

当前版本的：

```text
baseline_bytes
baseline_pkts
```

是**当前 5 分钟窗口内的近似 pair 基线**。

不是：

```text
过去 7 天平均
过去 7 天相同时间段
```

原因是当前持续任务只扫描新增的 5 分钟窗口，不重复读取 7 天原始数据。

如果业务以后明确要求“7 天历史基线”，建议下一版增加：

```text
跨窗口持久化 CMS / EWMA / Quantile Sketch
```

或者 Doris 单独维护 baseline 表。

---

# 11. 规则二：非工作时段活跃连接

配置：

```properties
rule.offhours.weekday.start.hour=20
rule.offhours.weekday.end.hour=8
```

定义：

```text
周一~周五：
20:00 <= time < 24:00
或者
00:00 <= time < 08:00

周六、周日：
全天
```

业务时区：

```properties
business.timezone=Asia/Shanghai
```

---

## 11.1 为什么不用 HashMap 排 Top100？

如果这样写：

```java
Map<Pair, Long> connectionCount
```

IP Pair 很多时 Map 可能非常大。

当前使用：

```text
Weighted Space-Saving Sketch
```

每条 Doris 聚合记录：

```text
item   = srcIp|dstIp|protocol
weight = connCount
```

例如：

```text
10.1.1.1 -> 10.2.2.2
connCount = 156856
```

一次 update 就把权重 156856 加进去，不需要循环 156856 次。

---

## 11.2 TopN 配置

```properties
rule.offhours.topn=100
rule.offhours.sketch.capacity=2048
```

为什么 capacity 不是 100？

因为近似算法需要更大的候选空间，才能降低第 90~110 名附近的排名抖动。

---

# 12. 告警 JSON

## Type 2

```json
{
  "logId": "STATIC_NET_TRAFFIC_ALERT",
  "collectTime": "2026-08-17 20:01:00",
  "srcIp": "10.1.1.1",
  "dstIp": "10.2.2.2",
  "protocol": "TCP",
  "anomalyType": 2,
  "anomalyDetail": {
    "current_bytes": 5211,
    "current_pkts": 211,
    "baseline_bytes": 2500,
    "baseline_pkts": 56
  },
  "remark1": "",
  "remark2": "",
  "vendorCode": "V001"
}
```

## Type 3

```json
{
  "logId": "STATIC_NET_TRAFFIC_ALERT",
  "collectTime": "2026-08-17 20:04:00",
  "srcIp": "10.1.1.1",
  "dstIp": "10.2.2.2",
  "protocol": "TCP",
  "anomalyType": 3,
  "anomalyDetail": {
    "topRank": 2,
    "TopN": 100,
    "conns": 156856
  },
  "remark1": "",
  "remark2": "",
  "vendorCode": "V001"
}
```

---

# 13. application.properties 逐项说明

## 13.1 Job

```properties
job.name=NET-TRAFFIC-ANOMALY
business.timezone=Asia/Shanghai
```

`job.name` 是 Flink Web UI 中看到的 Job 名。

---

## 13.2 5 分钟窗口

```properties
window.size.minutes=5
window.delay.minutes=1
```

一般不用修改 5。

`delay=1` 是给上游聚合数据一点落库时间。

如果你发现 Doris 在窗口结束后 1 分钟还没有完全写完，可以改为：

```properties
window.delay.minutes=2
```

甚至 3。

代价是告警变慢。

---

## 13.3 首次启动模式

默认：

```properties
source.start.mode=latest_closed
```

表示不补历史，直接从最新完整窗口开始。

如果需要补数：

```properties
source.start.mode=fixed
source.start.time=2026-08-17 20:00:00
```

注意时间必须对齐到 5 分钟，例如：

```text
20:00 OK
20:05 OK
20:10 OK
20:03 不允许
```

---

## 13.4 Source 等待和重试

```properties
source.poll.interval.seconds=15
source.retry.interval.seconds=30
source.retry.max.failures=10
```

`poll` 只是“没到下一个窗口时多久看一次时钟”，不是每 15 秒查询 Doris。

Doris 查询只会针对闭合的新窗口执行。

---

## 13.5 Doris

```properties
doris.jdbc.url=jdbc:mysql://10.166.10.37:9030/...
doris.table=anomaly_detection.metric_endpoint
doris.username=root
doris.password=...
```

FE HTTP 8031 在这个持续版本中不用于读取。

查询使用的是 FE MySQL query port：

```text
9030
```

---

## 13.6 本地日志开关

```properties
alert.output.log.enabled=true
```

YARN 模式中的“本地日志”实际上是 TaskManager/container 的日志。

---

## 13.7 Kafka 开关

```properties
alert.output.kafka.enabled=false
```

开启：

```properties
alert.output.kafka.enabled=true
kafka.bootstrap.servers=你的broker1:9092,你的broker2:9092
kafka.topic=anomaly_alert
```

---

## 13.8 Kafka delivery guarantee

默认：

```properties
kafka.delivery.guarantee=at_least_once
```

如果使用：

```properties
kafka.delivery.guarantee=exactly_once
```

必须保证：

```properties
checkpoint.enabled=true
```

并且 Kafka 事务相关配置/权限正确。

---

# 14. checkpoint 为什么很重要？

当前 Source checkpoint 的核心值是：

```text
nextWindowStart
```

如果没有可靠 checkpoint，整个 YARN Application 完全消失以后再手工重新提交，程序不知道上次处理到哪里。

默认 `latest_closed` 会从最新窗口开始，有可能跳过停机期间的窗口。

所以生产环境建议：

```properties
checkpoint.storage.path=hdfs:///flink-checkpoints/net-traffic-sentinel
```

这里不能随便照抄，必须确保你的 Hadoop 集群确实能访问这个 HDFS 路径。

---

# 15. 内部 restart 和“重新提交一个新任务”有什么区别？

这是新手非常容易混淆的一点。

## 情况 A：TaskManager 临时挂了

Flink JobManager 还活着，Flink 自动 restart。

通常会从最近 checkpoint 恢复。

## 情况 B：整个 YARN Application 被 kill

例如：

```bash
yarn application -kill application_xxx
```

然后你重新运行：

```bash
./run-yarn.sh
```

这相当于一个“新 Job”。

如果没有显式从 savepoint/checkpoint metadata 恢复，它不会自动知道旧 Job 的 cursor。

最简单的运维策略有两个：

1. 正常升级前做 savepoint，然后从 savepoint 启动。
2. 如果没有 savepoint，重新启动前把 `source.start.mode=fixed` 和 `source.start.time` 设置成你确认的最后窗口。

---

# 16. 环境检查

在服务器上执行：

```bash
java -version
```

期望 Java 8 或 Java 11。

检查 Maven：

```bash
mvn -version
```

检查 Flink：

```bash
/home/xgs/flink-1.17.2/bin/flink --version
```

检查 Hadoop/YARN：

```bash
yarn application -list
```

---

# 17. 先测试 Doris 是否能连接

如果机器上有 mysql client：

```bash
mysql -h 10.166.10.37 -P 9030 -u root -p
```

输入密码以后测试：

```sql
SELECT COUNT(*)
FROM anomaly_detection.metric_endpoint
WHERE collectTime >= '2026-08-17 20:00:00'
  AND collectTime <  '2026-08-17 20:05:00';
```

如果这里都连不上，先不要启动 Flink。

---

# 18. 编译步骤

进入项目目录：

```bash
cd NetTrafficSentinel
```

执行：

```bash
mvn clean package
```

成功后：

```bash
ls -lh target/NetTrafficSentinel-1.0-SNAPSHOT.jar
```

如果存在，就说明业务 JAR 已生成。

---

# 19. 为什么 Flink 依赖是 provided？

服务器已经有：

```text
/home/xgs/flink-1.17.2
```

因此 Flink 核心类不需要重复全部塞进业务 JAR。

但是这些依赖会 shade 进业务 JAR：

```text
MySQL Connector/J
Flink Kafka Connector
T-Digest
Jackson
```

---

# 20. 正式提交 YARN

建议先确认配置文件权限：

```bash
chmod 600 application.properties
```

然后：

```bash
./run-yarn.sh
```

脚本里面最终执行的是：

```bash
/home/xgs/flink-1.17.2/bin/flink run-application \
  -t yarn-application \
  -ys 1 \
  -p 1 \
  -Dlog4j.debug=true \
  -Denv.java.opts=-Duser.timezone=Asia/Shanghai \
  -Djobmanager.memory.process.size=8048m \
  -Dtaskmanager.memory.process.size=10240m \
  -Dtaskmanager.memory.managed.size=2048m \
  -Dyarn.application.name=NET-TRAFFIC-ANOMALY \
  -Dyarn.ship-files=/绝对路径/application.properties \
  -c cn.ac.iie.topology.NetTrafficSentinel \
  target/NetTrafficSentinel-1.0-SNAPSHOT.jar \
  --config application.properties
```

---

# 21. `yarn.ship-files` 是干什么的？

Flink/YARN container 不一定和你提交命令所在 shell 的当前目录相同。

所以：

```text
-Dyarn.ship-files=/xxx/application.properties
```

会把配置文件作为 YARN local resource 分发到 container。

程序再通过：

```bash
--config application.properties
```

读取它。

---

# 22. 怎么确认任务真的在运行？

查看 YARN：

```bash
yarn application -list
```

应该能看到类似：

```text
NET-TRAFFIC-ANOMALY
```

记录它的 application id，例如：

```text
application_123456789_0010
```

---

# 23. 怎么看日志？

```bash
yarn logs -applicationId application_123456789_0010
```

日志中应该周期性看到：

```text
Start Doris five-minute query
Doris query progress
Doris query analyzed
Window finished
```

例如：

```text
Window finished: [2026-08-17 20:05:00, 2026-08-17 20:10:00),
rows=12345678,
offHoursRows=12345678,
alerts=87,
nextCursor=2026-08-17 20:10:00
```

看到 `nextCursor` 正常增加，就是持续模式在工作。

---

# 24. 怎么只看告警日志？

当前 LocalLogSink 会把告警 JSON 写入日志。

可以：

```bash
yarn logs -applicationId application_xxx | grep anomalyType
```

或者根据你环境的日志平台检索。

---

# 25. 怎么验证 Kafka？

先打开：

```properties
alert.output.kafka.enabled=true
```

配置 broker：

```properties
kafka.bootstrap.servers=broker1:9092,broker2:9092
kafka.topic=anomaly_alert
```

如果服务器装了 Kafka CLI，可以消费：

```bash
kafka-console-consumer.sh \
  --bootstrap-server broker1:9092 \
  --topic anomaly_alert \
  --from-beginning
```

看到 JSON 即成功。

---

# 26. 怎么停止任务？

查 application id：

```bash
yarn application -list
```

强制停止：

```bash
yarn application -kill application_xxx
```

生产升级更推荐先做 savepoint，再停止。

---

# 27. 如果任务停了 2 小时，怎么补？

假设确认最后成功窗口结束时间是：

```text
18:30
```

重新提交前设置：

```properties
source.start.mode=fixed
source.start.time=2026-08-17 18:30:00
```

任务会依次处理：

```text
18:30~18:35
18:35~18:40
18:40~18:45
...
```

一直追到最新完整窗口。

追上以后不会退出，而是继续等待未来窗口。

---

# 28. 如果一个 5 分钟窗口处理超过 5 分钟怎么办？

程序不会同时并发处理多个窗口。

例如：

```text
20:00~20:05 处理用了 8 分钟
```

处理完成后发现：

```text
20:05~20:10
```

也已经闭合了，就会立即处理下一窗口，不额外等 5 分钟。

因此它会自动 catch up。

但是如果长期出现：

```text
每个 5 分钟窗口都需要 8 分钟
```

那么 backlog 会越来越大，说明吞吐不足，必须优化。

---

# 29. 当前最大的性能风险是什么？

不是 Sketch 内存。

更可能是：

```text
Doris -> 单 JDBC Query -> 单 Source subtask
```

你的数据量非常大，所以必须观察：

```text
一个 5 分钟窗口实际查询 + 分析耗时
```

如果稳定小于 5 分钟，当前方案可以持续跟上。

如果稳定超过 5 分钟，下一步建议做：

```text
Doris 多分片并发读取
        |
每个 shard 计算局部 Sketch
        |
merge Sketch
        |
输出最终告警
```

当前版本故意限制：

```properties
source.parallelism=1
```

避免没有做 query sharding 时多个 Source 全量重复扫描同一窗口。

不要直接把它改成 4，否则会把同一个窗口读 4 遍。

---

# 30. 10GB TaskManager 内存够不够？

检测结构本身通常很小。

默认：

```text
CMS 三张表：约 30 MiB
Candidate heap：20,000 条
Space-Saving：2,048 条
T-Digest：远小于全量排序数组
```

JDBC 使用 streaming ResultSet，所以不会为了 ResultSet 保存几千万条 Java 对象。

10GB 的主要余量会留给：

- Flink runtime
- JDBC/网络 buffer
- Kafka connector
- JVM heap / metaspace
- 临时对象和 GC

---

# 31. 为什么 `-p 1` 暂时保留？

你的原提交命令是：

```text
-p 1
```

当前实现也要求 Source parallelism=1。

所以第一版先保持：

```bash
-p 1
-ys 1
```

最容易控制行为。

后续如果确认单源吞吐不够，不应该只简单改 `-p 4`，而应该先实现 Doris 查询分片和 Sketch merge。

---

# 32. 常见错误：找不到配置文件

错误类似：

```text
Config file not found
```

检查：

```bash
ls -l application.properties
```

以及 `run-yarn.sh` 中：

```text
-Dyarn.ship-files
--config
```

是否指向正确文件。

---

# 33. 常见错误：Doris 登录失败

错误类似：

```text
Access denied
```

先用 mysql client 测试 9030。

检查：

```properties
doris.username
doris.password
doris.jdbc.url
```

---

# 34. 常见错误：Communications link failure

优先检查：

```bash
ping 10.166.10.37
nc -vz 10.166.10.37 9030
```

如果 YARN container 所在节点无法访问 FE，提交节点能访问也没有用。

---

# 35. 常见错误：查询超时

配置：

```properties
doris.jdbc.query.timeout.seconds=1800
```

默认 30 分钟。

如果一个 5 分钟查询真的需要几十分钟，不建议只是不断把 timeout 加大。

应该查看 Doris profile 和分区裁剪是否生效。

---

# 36. 常见错误：任务一直重启

查看 YARN/TaskManager 日志中的第一条异常。

Source 默认：

```properties
source.retry.max.failures=10
```

同一个窗口连续 SQL 失败 10 次后，会让 Flink 任务失败，再由：

```properties
restart.attempts=20
restart.delay.seconds=10
```

控制 Flink restart。

---

# 37. 常见问题：为什么一直没有 type=3？

检查当前 collectTime 是否属于：

```text
工作日 20:00~08:00
或周末
```

同时检查：

```properties
rule.offhours.enabled=true
```

---

# 38. 常见问题：为什么 type=2 很少？

默认阈值比较严格：

```properties
rule.large.bytes.quantile=0.999
rule.large.bytes.baseline.multiplier=4.0
```

可以先在测试环境降低，例如：

```properties
rule.large.bytes.quantile=0.99
rule.large.pkts.quantile=0.99
rule.large.bytes.baseline.multiplier=2.0
rule.large.pkts.baseline.multiplier=2.0
```

观察告警数量，再逐步调整。

不要在生产环境直接大幅降低而不评估告警洪峰。

---

# 39. 常见问题：candidate.capacity 要不要调大？

默认：

```properties
rule.large.candidate.capacity=20000
```

它决定有多少个最大 bytes 的候选记录能够进入最终 type=2 判断。

如果窗口行数特别大，同时 P99.9 尾部记录数远超过 20000，可能有部分尾部连接没有进入候选池。

可以提高：

```text
20000 -> 50000 -> 100000
```

但会增加 heap 和 finalization CPU。

---

# 40. 生产上线建议顺序

第一次不要直接：

```properties
alert.output.kafka.enabled=true
```

建议：

### 阶段 1

```properties
alert.output.log.enabled=true
alert.output.kafka.enabled=false
```

跑一段时间，只观察日志。

重点看：

```text
每窗口 rows
每窗口 elapsedMs
每窗口 alerts
是否有 query retry
是否有 backlog
```

### 阶段 2

确认阈值合理后：

```properties
alert.output.kafka.enabled=true
```

先接测试 topic。

### 阶段 3

确认下游消费正常，再切正式 topic。

---

# 41. 建议重点监控的指标

Source 注册了 Flink counter：

```text
processedWindows
queriedRows
emittedAlerts
badRows
queryFailures
```

在 Flink metric 系统接入 Prometheus 等系统以后，可以对这些指标做监控。

最重要的是：

```text
processedWindows 是否持续增加
queryFailures 是否持续增加
一个窗口耗时是否 > 5 分钟
```

---

# 42. 配置密码安全

根目录配置文件目前是部署文件，可能包含真实密码。

至少执行：

```bash
chmod 600 application.properties
```

并把它加入 Git 忽略规则。

不要把真实密码放进：

```text
src/main/resources/application.properties
```

因为 resources 会进入最终 JAR。

项目里的 classpath 配置使用 `CHANGE_ME_DORIS_PASSWORD` 占位符。

---

# 43. 推荐上线前检查清单

- [ ] Java 是 8 或 11
- [ ] Maven 能正常下载依赖
- [ ] Flink 是 1.17.2
- [ ] YARN 可正常提交任务
- [ ] YARN 节点能访问 Doris FE 9030
- [ ] Doris 5 分钟查询能命中时间分区/范围
- [ ] `business.timezone=Asia/Shanghai`
- [ ] `source.parallelism=1`
- [ ] `window.size.minutes=5`
- [ ] `checkpoint.storage.path` 已配置到可用 HDFS
- [ ] 首次上线只开日志输出观察
- [ ] Kafka broker/topic 已确认
- [ ] 配置文件权限为 600
- [ ] 一个 5 分钟窗口能够在 5 分钟以内稳定处理完

---

# 44. 一句话理解整个项目

这个程序本质上就是：

```text
一个永不退出的 Flink YARN Application
        +
一个带 checkpoint 游标的 5 分钟 Doris JDBC Poller
        +
固定内存的 T-Digest / Count-Min Sketch / Space-Saving
        +
日志和 Kafka 告警 Sink
```

第一次排查问题时，不要一次看所有类。

按下面顺序看即可：

```text
NetTrafficSentinel.java
        ↓
DorisPollingAlertSource.java
        ↓
FiveMinuteWindowAnalyzer.java
        ↓
LargeTrafficDetector.java / OffHoursDetector.java
        ↓
KafkaSinkFactory.java / LocalLogSink.java
```


---

# 45. 官方参考资料

下面这些官方文档对应本工程最关键的兼容性与实现选择：

- Apache Doris 2.1 Flink Doris Connector：说明 Doris Source 当前是 bounded stream，不支持 CDC 式持续读取，并说明读取字段/过滤条件的能力。
  https://doris.apache.org/docs/2.1/ecosystem/flink-doris-connector/
- Apache Doris 2.1 MySQL Protocol：说明 Doris FE query port 可使用 MySQL/JDBC 生态连接。
  https://doris.apache.org/docs/2.1/db-connect/database-connect/
- MySQL Connector/J ResultSet：说明 forward-only + read-only + `Integer.MIN_VALUE` fetch size 的 row streaming 模式。
  https://dev.mysql.com/doc/connectors/en/connector-j-reference-implementation-notes.html
- Flink 1.17 SourceFunction API：自定义 Source 与 checkpoint lock 的基础接口。
  https://nightlies.apache.org/flink/flink-docs-release-1.17/api/java/org/apache/flink/streaming/api/functions/source/SourceFunction.html
- Flink 1.17 Checkpoints：checkpoint storage、HDFS/FileSystemCheckpointStorage 与 retained checkpoint。
  https://nightlies.apache.org/flink/flink-docs-release-1.17/docs/ops/state/checkpoints/
- Flink 1.17 Kafka Sink：AT_LEAST_ONCE / EXACTLY_ONCE 与 transactional id。
  https://nightlies.apache.org/flink/flink-docs-release-1.17/docs/connectors/datastream/kafka/

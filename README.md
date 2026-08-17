# NetTrafficSentinel

`NetTrafficSentinel` 是一个基于 **Java 8 / Apache Flink 1.17.2 DataStream API** 的常驻网络流量异常检测程序。

它从 Doris `anomaly_detection.metric_endpoint` 持续读取已经闭合的 5 分钟数据窗口，使用固定内存 Sketch 算法检测：

1. `anomalyType=2`：异常大流量
2. `anomalyType=3`：非工作时段活跃连接 Top N

告警可以独立输出到 YARN 日志和 Kafka。

## 最重要的变化

这个版本**不是每 5 分钟重新提交一次 Flink 作业**。

提交一次以后，YARN Application 会一直运行：

```text
20:06 -> 处理 [20:00, 20:05)
20:11 -> 处理 [20:05, 20:10)
20:16 -> 处理 [20:10, 20:15)
...
```

如果某个窗口处理得比较慢，程序会自动按游标继续补后面的窗口，不会直接跳过。

由于 Doris Flink Connector 的 Doris Source 是 bounded source，不能像 Kafka 一样持续 tail Doris 表，本工程的持续读取改为 **Doris MySQL JDBC 协议 + 自定义 Flink SourceFunction**。每次 SQL 只读取一个 5 分钟区间，并通过 Connector/J streaming ResultSet 逐行处理，避免把 ResultSet 全量装入 JVM 内存。

## 项目名称

- Maven artifact：`NetTrafficSentinel`
- 主类：`cn.ac.iie.topology.NetTrafficSentinel`
- JAR：`NetTrafficSentinel-1.0-SNAPSHOT.jar`
- YARN Application：`NET-TRAFFIC-ANOMALY`

## 快速开始

### 1. 修改配置

编辑：

```text
application.properties
```

至少确认：

```properties
doris.jdbc.url=jdbc:mysql://10.166.10.37:9030/...
doris.table=anomaly_detection.metric_endpoint
doris.username=root
doris.password=你的密码

alert.output.log.enabled=true
alert.output.kafka.enabled=false

kafka.bootstrap.servers=localhost:9092
kafka.topic=anomaly_alert
```

生产环境强烈建议配置 HDFS checkpoint：

```properties
checkpoint.storage.path=hdfs:///flink-checkpoints/net-traffic-sentinel
```

### 2. 编译

```bash
mvn clean package
```

产物：

```text
target/NetTrafficSentinel-1.0-SNAPSHOT.jar
```

### 3. 提交 YARN

```bash
./run-yarn.sh
```

等价核心命令：

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
  -Dyarn.ship-files=/absolute/path/application.properties \
  -c cn.ac.iie.topology.NetTrafficSentinel \
  target/NetTrafficSentinel-1.0-SNAPSHOT.jar \
  --config application.properties
```

## 持续运行流程

```text
                         当前业务时间
                              |
                              v
                    计算最新安全闭合窗口
                              |
                 +------------+-------------+
                 | next window 已经闭合？   |
                 +------------+-------------+
                       | Yes          | No
                       v              v
              查询 Doris 5 分钟       等待 15 秒
                       |
               JDBC Streaming ResultSet
                       |
             +---------+----------+
             |                    |
             v                    v
       大流量 Sketch         非工作时段 Sketch
       T-Digest + CMS         Space-Saving
             |                    |
             +---------+----------+
                       |
                 生成告警列表
                       |
              checkpoint lock 内：
              1. 发出告警
              2. 游标推进 5 分钟
                       |
                       v
                  下一个窗口
```

## 内存策略

程序不会保存整个 5 分钟窗口的全部数据，也不会维护全量 IP Pair HashMap。

默认大流量规则：

```text
3 个 Count-Min Sketch：约 30 MiB
T-Digest：小型固定摘要
Candidate Heap：最多 20,000 条 MetricRecord
```

非工作时段规则：

```text
Weighted Space-Saving capacity = 2048
TopN = 100
```

因此检测侧内存上限主要由配置决定，而不是由窗口行数决定。

## 规则一：异常大流量

每行：

```text
currentBytes = c2sBytes + s2cBytes
currentPkts  = c2sPkts  + s2cPkts
pairKey      = srcIp + dstIp + protocol
```

窗口内维护：

- bytes T-Digest
- pkts T-Digest
- byteSum CMS
- pktSum CMS
- observationCount CMS
- 最大流量候选最小堆

默认阈值：

```text
currentBytes > max(P99.9(bytes), pairBaselineBytes * 4)
AND
(
  currentPkts > max(P99.9(pkts), pairBaselinePkts * 4)
  OR currentBytes > P99.9(bytes) * 2
)
```

同一 pair 在同一 5 分钟窗口最多输出一条 type=2 告警。

> 当前 `baseline_bytes/baseline_pkts` 是**同一个 5 分钟窗口中的近似行为基线**。它不是过去 7 天历史基线。若后续需要 7 天同时间段基线，应增加独立的长期 Sketch 状态或基线表。

## 规则二：非工作时段 Top N

非工作时间：

- 周一到周五：20:00~次日 08:00
- 周六、周日：全天
- 时区：Asia/Shanghai

每个 5 分钟窗口：

```text
item   = srcIp|dstIp|protocol
weight = connCount
```

使用 Weighted Space-Saving 近似计算 Top100。

## 输出开关

```properties
alert.output.log.enabled=true
alert.output.kafka.enabled=false
```

- `true / false`：只写 YARN TaskManager 日志
- `false / true`：只写 Kafka
- `true / true`：同时写日志和 Kafka
- `false / false`：仍计算，但输出丢弃

## Source 游标与故障恢复

Source 只在**完整读完并分析完一个 5 分钟窗口**后才发告警和推进游标。

如果 Doris 查询中途失败：

```text
游标不变
    -> 等待 source.retry.interval.seconds
    -> 重新查询同一个 5 分钟窗口
```

checkpoint 保存的核心 source state 是：

```text
nextWindowStart
```

例如：

```text
2026-08-17 20:15:00
```

表示下一次应该处理 `[20:15,20:20)`。

## 新部署从哪个窗口开始？

默认：

```properties
source.start.mode=latest_closed
```

第一次启动从最新闭合窗口开始。

需要从历史时间补数据：

```properties
source.start.mode=fixed
source.start.time=2026-08-17 20:00:00
```

程序会从该窗口开始连续追赶，追上实时窗口后自动进入等待模式。

恢复已有 checkpoint 时，checkpoint 游标优先于 `source.start.mode`。

## 详细文档

第一次接触 Flink / Maven / YARN，建议直接阅读：

**[docs/BEGINNER_DEPLOYMENT_GUIDE_CN.md](docs/BEGINNER_DEPLOYMENT_GUIDE_CN.md)**

里面包含逐文件代码说明、编译部署、配置解释、查看日志、Kafka 验证、checkpoint、停止与恢复、常见错误和性能调优。

## 安全提醒

根目录 `application.properties` 包含运行环境密码时：

```bash
chmod 600 application.properties
```

不要把包含密码的文件提交到 Git。

JAR 内部的 `src/main/resources/application.properties` 使用的是占位密码，不包含真实密码。

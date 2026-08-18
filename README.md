# NetTrafficSentinel

`NetTrafficSentinel` 是一个基于 **Java 8/11 + Apache Flink 1.17.2 DataStream API** 的常驻网络流量异常检测程序。

它从 Doris `anomaly_detection.metric_endpoint` 按 **5 分钟数据窗口**持续读取，检测：

1. `anomalyType=2`：异常大流量
2. `anomalyType=3`：非工作时段活跃连接 Top N

告警可以独立输出到 YARN/TaskManager 日志和 Kafka。

---

## 学习版文档

如果你不熟悉 Flink 或想从头理解 type=2 算法，优先阅读：

- `docs/NetTrafficSentinel代码与算法完全指南.docx`：完整 Word 手册；
- `docs/BEGINNER_CODE_AND_ALGORITHM_GUIDE_CN.md`：同内容 Markdown 版；
- `src/main/java`：关键主链路源码已经补充“导读”和算法中文注释。

建议阅读顺序：`NetTrafficSentinel -> DorisPollingAlertSource -> FiveMinuteWindowAnalyzer -> LargeTrafficAccumulator -> LargeTrafficDetector -> HistoricalBaselineStore -> AlertRecord`。

---

## 1. 本版本解决了什么问题

上一版存在两个现实问题：

- Doris 数据入库可能延迟数小时，不能用“当前系统时间 - 1 分钟”判断哪个窗口已经完整；
- `baseline_bytes/baseline_pkts` 主要来自当前 5 分钟窗口，不是真正的跨窗口历史基线。

本版本改成：

```text
Doris MAX(collectTime)
        -
稳定等待时间（默认 60 分钟）
        ↓
安全数据 Watermark
        ↓
每次只处理一个成熟的 5 分钟窗口
```

同时 type=2 改成：

```text
历史 Context t-digest（7 天时间桶）
                 +
有限 Pair EMA 历史（7 天 TTL + 20 万硬上限）
                 ↓
           异常大流量判断
```

**Context Sketch 不保存 IP Pair。**

真正保存 Pair 的只有 Pair EMA History，而且只学习每个 5 分钟窗口固定容量的大流量候选，因此状态不会随全量 IP Pair 基数无限增长。

---

## 2. 项目信息

- Maven artifact：`NetTrafficSentinel`
- 主类：`cn.ac.iie.topology.NetTrafficSentinel`
- JAR：`target/NetTrafficSentinel-1.0-SNAPSHOT.jar`
- YARN Application：`NET-TRAFFIC-ANOMALY`
- Flink：1.17.2
- Java：8 / 11
- Doris：2.1.x

---

## 3. 持续运行方式

任务只需要提交一次。

假设：

```text
系统时间               = 20:00
Doris MAX(collectTime) = 12:37
stable.delay           = 60 分钟
```

程序计算：

```text
12:37 - 60min = 11:37
floorTo5min   = 11:35
```

因此：

```text
safeWindowEnd = 11:35
```

程序最多只处理到：

```text
[11:30, 11:35)
```

不会去查询：

```text
[19:55, 20:00)
```

随着 Doris 数据继续入库：

```text
MAX=12:42 -> safeEnd=11:40 -> 处理 [11:35,11:40)
MAX=12:47 -> safeEnd=11:45 -> 处理 [11:40,11:45)
...
```

---

## 4. 空窗口不再自动跳过

默认：

```properties
source.empty.window.advance=false
source.empty.window.retry.seconds=300
```

如果某个已经被 Watermark 判断为“成熟”的窗口仍然：

```text
rows=0
```

程序会：

```text
保持 nextWindowStart 不变
        ↓
等待 300 秒
        ↓
重新检查 Doris Watermark
        ↓
重新查询同一个 5 分钟窗口
```

不会像旧版一样：

```text
rows=0
nextCursor += 5分钟
```

从而把迟到数据永久跳过去。

> 由于没有上游 COMPLETE 批次标记，`MAX(collectTime) - stable.delay` 仍然是工程折中，不可能数学上证明窗口 100% 完整。对于你们每天几十亿条数据的场景，“额外稳定期 + 空窗口不推进”是当前比较稳妥的实现。

---

## 5. type=2 新的历史基线

### 5.1 Context History

Context 定义：

```text
protocol + WORKDAY/WEEKEND + 5分钟时间槽
```

例如：

```text
TLS + WORKDAY + 20:20~20:25
UNKNOWN + WEEKEND + 09:00~09:05
```

Context Sketch 只保存：

```text
bytes 分布
pkts 分布
```

不保存：

```text
srcIp
dstIp
pairKey
```

默认保留：

```properties
history.context.retention.days=7
```

实现不是“一个永久增长的 t-digest”，而是：

```text
Context
  ├─ 2026-08-11 -> t-digest
  ├─ 2026-08-12 -> t-digest
  ├─ 2026-08-13 -> t-digest
  └─ ...
```

检测 8 月 17 日时，只合并之前保留期内的日期桶。

过期时直接删除整个旧日期桶。

### 5.2 防止异常值污染 Context

如果已经存在至少一个历史日期桶，当前窗口更新历史 Context 时，会把极端值截断在上一历史高分位附近：

```text
historyValue = min(currentValue, historicalHighQuantile)
```

这样一个 100MB/500MB 的异常不会直接把之后的正常历史分位数抬得很高。

### 5.3 Pair History

Pair：

```text
srcIp + dstIp + protocol
```

为了节约内存，历史状态不保存完整字符串，而使用稳定的 64 位 hash 作为 key。

Pair 状态保存：

```text
emaBytes
emaPkts
sampleCount
lastSeenEventTime
```

默认：

```properties
history.pair.ttl.days=7
history.pair.max.entries=200000
history.pair.ema.alpha=0.10
history.pair.min.samples=3
```

Pair EMA 更新：

```text
newEMA = oldEMA * 0.90 + current * 0.10
```

### 5.4 哪些 Pair 会进入历史

不会保存全量 Pair。

每个 5 分钟窗口只保留：

```properties
rule.large.candidate.capacity=20000
```

条最大流量 Candidate。

只有这些 bounded candidate 才可能更新 Pair EMA。

所以 Pair History 的增长有两层保护：

```text
每窗口 Candidate 上限 20000
            +
全局 Pair State 上限 200000
            +
7 天事件时间 TTL
```

### 5.5 异常 Pair 的受限学习

为了避免新 Pair 第一次就触发 type=2 后永远无法积累 Pair EMA，当前逻辑区分冷启动和成熟 Pair：

```text
Pair samples < history.pair.min.samples
    + 当前触发 type=2
    -> 允许学习，但 bytes/pkts 分别 cap 到有效 Context 的高分位阈值

Pair samples >= history.pair.min.samples
    + 当前触发 type=2
    -> 不更新 Pair EMA
```

默认开关：

```properties
history.pair.bootstrap.anomaly.capped.learning.enabled=true
```

这样既能让冷启动 Pair 逐步获得自己的 EMA，又继续阻止成熟 Pair 直接吸收异常大值。

### 5.6 baseline_bytes 的含义

优先级：

```text
Pair bounded samples >= 3
    -> baseline = Pair EMA

否则 Context 历史已经成熟
    -> baseline = Historical Context P50

否则（冷启动）
    -> baseline = 当前 5 分钟 Context P50
```

因此新 Pair 的 `baseline_bytes` 仍然可能比较小，这是因为它没有自身历史，只能退化到同协议/同时段的中位数。

但是最终告警阈值不会只用 baseline：

```text
bytesThreshold = max(
    Historical Context P99.9,
    baselineBytes * 4
)
```

因此 `baseline=200` 并不意味着 801 Bytes 就会报警。

---

## 6. type=2 判断公式

默认：

```text
bytesQ = 0.999
pktsQ  = 0.999
baselineMultiplier = 4
extremeMultiplier  = 2
```

判断：

```text
currentBytes > max(contextP99.9Bytes, baselineBytes * 4)
AND
(
    currentPkts > max(contextP99.9Pkts, baselinePkts * 4)
    OR
    currentBytes > contextP99.9Bytes * 2
)
```

同一个 Pair 在同一 5 分钟窗口最多输出一条 type=2 告警，选择最大 currentBytes 的那条。

---

## 7. type=3 非工作时段 TopN

非工作时间：

```text
周一~周五：20:00~24:00 + 00:00~08:00
周六/周日：全天
```

每个 5 分钟窗口：

```text
item   = srcIp|dstIp|protocol
weight = connCount
```

使用固定容量 Weighted Space-Saving：

```properties
rule.offhours.topn=100
rule.offhours.sketch.capacity=2048
```

输出近似 Top100。

---

## 8. Checkpoint 中保存什么

新版 checkpoint/savepoint 不只保存 cursor，还保存：

```text
nextWindowStart
Context 历史日期桶
Pair EMA 历史
```

窗口查询过程中不修改长期状态。

只有完整查询 + 完整检测成功以后，在 checkpoint 临界区一次性执行：

```text
1. emit alerts
2. apply Context History update
3. apply Pair EMA update
4. nextWindowStart += 5分钟
```

如果 JDBC 查询中途失败，以上四件事都不会发生。

---

## 9. 强烈建议配置 HDFS checkpoint

不要长期使用日志中的：

```text
Checkpoint storage is set to 'jobmanager'
```

生产配置：

```properties
checkpoint.storage.path=hdfs:///flink-checkpoints/net-traffic-sentinel
```

默认 checkpoint 周期已经调整为：

```properties
checkpoint.interval.ms=300000
```

即 5 分钟一次，因为新版 checkpoint 中包含历史基线状态，没必要每分钟完整 snapshot 一次。

---

## 10. 编译

```bash
cd NetTrafficSentinel
mvn clean package
```

产物：

```text
target/NetTrafficSentinel-1.0-SNAPSHOT.jar
```

---

## 11. YARN 提交

```bash
./run-yarn.sh
```

等价命令：

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

---

## 12. 第一次上线怎么选 source.start.mode

### 只想从 Doris 当前成熟位置开始

```properties
source.start.mode=latest_data
```

这时历史 Context 属于冷启动，需要积累至少：

```properties
history.context.min.days=2
```

个历史日期后才使用跨日 Context P99.9。

### 想从某个历史时间补算

```properties
source.start.mode=fixed
source.start.time=2026-08-15 00:00:00
```

程序会从这个窗口开始，一直追到当前 Doris safe watermark，然后继续常驻。

注意：历史补算期间也会产生告警。如果不想把补历史的告警发送 Kafka，建议先：

```properties
alert.output.log.enabled=true
alert.output.kafka.enabled=false
```

等追上以后再启 Kafka。

---

## 13. 重要升级提示

如果旧版本已经把 cursor 推进到了“系统当前时间附近”，但 Doris 实际数据只到上午，那么旧 cursor 中间可能已经跳过迟到数据。

**升级到本版本时，不建议直接把旧版本 cursor 当成可信进度。**

建议：

1. 新建 YARN Application；
2. 根据 Doris 实际数据范围选择 `source.start.mode=fixed`；
3. 从一个确认安全的历史时间重新补算；
4. 等追上新的 Doris Watermark 后继续常驻。

---

## 14. 推荐重点观察的日志

### Doris Watermark

```text
Doris data watermark updated:
maxCollectTime=2026-08-17 12:37:00,
stableDelay=60m,
safeWindowEnd=2026-08-17 11:35:00
```

### 开始窗口

```text
Start Doris five-minute query:
window=[2026-08-17 11:30:00, 2026-08-17 11:35:00)
```

### 窗口完成

```text
Window finished:
rows=...
alerts=...
pairHistoryEntries=...
contextHistoryBuckets=...
nextCursor=...
```

### 空窗口

```text
Empty Doris window ... Cursor is NOT advanced
```

如果大量出现这一行，应考虑：

```properties
source.doris.stable.delay.minutes
```

是否仍然太小。

---

## 15. 目录

```text
src/main/java/cn/ac/iie/topology/NetTrafficSentinel.java
    Flink 作业入口

src/main/java/cn/ac/iie/anomaly/source/DorisPollingAlertSource.java
    Doris Watermark、5 分钟轮询、JDBC streaming、checkpoint state

src/main/java/cn/ac/iie/anomaly/history/HistoricalBaselineStore.java
    7 天 Context 时间桶 + Pair EMA + TTL/容量淘汰 + checkpoint snapshot

src/main/java/cn/ac/iie/anomaly/history/ContextKey.java
    protocol + 工作日类型 + 时间槽

src/main/java/cn/ac/iie/anomaly/rule/FiveMinuteWindowAnalyzer.java
    单个 5 分钟窗口统一分析

src/main/java/cn/ac/iie/anomaly/rule/LargeTrafficAccumulator.java
    当前窗口 Context t-digest + 固定 Candidate Heap

src/main/java/cn/ac/iie/anomaly/rule/LargeTrafficDetector.java
    type=2 历史阈值/Pair EMA 判断

src/main/java/cn/ac/iie/anomaly/rule/OffHoursDetector.java
    type=3 TopN
```

更详细部署与排错请看：

```text
docs/BEGINNER_DEPLOYMENT_GUIDE_CN.md
```

---

## 12. 从上一版升级到 V2

V2 的 Source Operator State 已经发生变化：除了 5 分钟 cursor，还新增了 Context t-digest 历史桶和 Pair EMA 历史状态，并使用新的 `*-v2` state name / operator UID。

因此第一次升级建议：

1. 停止旧版 YARN Application；
2. **不要直接使用旧版 checkpoint 强行恢复 V2**；
3. 根据 Doris 当前数据进度选择一个确认安全的历史 5 分钟边界；
4. 第一次启动时配置：

```properties
source.start.mode=fixed
source.start.time=2026-08-15 20:00:00
```

5. 让 V2 从该时间开始补跑；追到 `safeWindowEnd` 后会自动进入等待状态并持续运行；
6. V2 自己产生 checkpoint/savepoint 后，后续再用 V2 状态恢复。

如果不需要补历史，也可以保持：

```properties
source.start.mode=latest_data
```

此时首次启动会从 Doris 当前安全 Watermark 前一个完整 5 分钟窗口开始。

> 根目录 `application.properties` 是部署用外置配置，当前保留了你提供的 Doris 连接信息；`src/main/resources/application.properties` 中密码仍是 `CHANGE_ME_DORIS_PASSWORD`。请不要把真实密码版本提交到公共代码仓库，部署服务器上建议限制配置文件读取权限。

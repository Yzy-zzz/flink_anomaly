# NetTrafficSentinel 代码与算法完全指南

> 适用版本：已加入 type=2 完整告警证据 + Pair EMA 冷启动受限学习（CAPPED_BOOTSTRAP）的版本  
> 目标读者：会一点 Java，但不熟悉 Flink、t-digest、EMA、Checkpoint 的开发/运维人员

---

## 0. 先给你一张“全项目地图”

如果你只想先建立直觉，把整个项目理解成下面这条流水线：

```text
                         Flink Job

 application.properties
          |
          v
 NetTrafficSentinel.main()
          |
          v
 DorisPollingAlertSource  <---- Flink Checkpoint 恢复游标和历史基线
          |
          | 每次查询一个成熟的 [start, end) 5分钟 Doris 数据窗口
          v
 FiveMinuteWindowAnalyzer
          |
          +----------------------------+
          |                            |
          v                            v
 anomalyType=2                    anomalyType=3
 异常大流量                      非工作时间 Top-N
          |                            |
          v                            v
 LargeTrafficAccumulator         OffHoursAccumulator
          |                            |
          v                            v
 LargeTrafficDetector            OffHoursDetector
          |
          +--> 读取 HistoricalBaselineStore
          |      |- Context t-digest 历史
          |      `- Pair EMA 历史
          |
          +--> 输出 AlertRecord
          +--> 生成本窗口历史更新计划
          |
          v
 Source 在 checkpointLock 内：
   1. ctx.collect(alert)
   2. historyStore.apply(...)
   3. nextWindowStart = window.end
          |
          v
 AlertJsonMapper
          |
          +--> LocalLogSink
          `--> Kafka Sink（可选）
```

对第一次阅读代码的人，最重要的一句话是：

**这个项目是 Flink 常驻作业，但 type=2/type=3 的 5 分钟统计窗口不是 Flink `window()` API 创建的，而是 Source 自己按 5 分钟范围查询 Doris 后在 JVM 内存中分析。**

---

## 1. 学完这份文档你应该能回答什么

读完后，你应该可以独立回答：

1. Flink 在这个项目里到底负责什么？
2. 为什么 Source 并不是“来一条 Kafka 消息处理一条”，而是循环查询 Doris？
3. `MAX(collectTime) - stable.delay` 是什么？为什么需要它？
4. `nextWindowStart` 为什么必须进入 Checkpoint？
5. `Context` 和 `Pair` 有什么区别？
6. `baseline_bytes=100` 到底从哪里来的？
7. P50/P90/P99.9 是什么？t-digest 为什么能节省内存？
8. EMA 是什么？`alpha=0.1` 意味着什么？
9. type=2 真正阈值为什么不是 `baseline * 4`？
10. 什么情况下产生 anomalyType=2？
11. `CAPPED_BOOTSTRAP` 为什么能解决“Pair 永远学不到”的问题？
12. 新版告警里的每个 evidence 字段应该怎么看？
13. Flink 崩溃重启后，Pair EMA 和 Context 历史为什么还能恢复？
14. 哪几个日志最适合排查误报、冷启动和状态丢失？

---

# 第一部分：只学本项目真正用到的 Flink

## 2. Flink 是什么？本项目为什么要用它？

Flink 是一个长时间运行的数据处理框架。对本项目而言，不需要一开始就学习复杂的 Event Time、Watermark、KeyedStream、Window Function。先理解四个概念就够了。

### 2.1 Job

整个 `NetTrafficSentinel` 就是一个 Flink Job。程序提交到 YARN 后，会持续运行，不是执行一次就退出。

主类：

```text
cn.ac.iie.topology.NetTrafficSentinel
```

### 2.2 Source

Source 是数据从哪里进入 Flink。

本项目的 Source：

```text
DorisPollingAlertSource
```

它不是普通的“读取一条、发一条”Source。它内部自己完成：

```text
查询 Doris 数据进度
-> 决定哪个 5 分钟窗口可以处理
-> JDBC 流式读取这个窗口
-> 在内存里跑异常算法
-> 输出 AlertRecord
```

所以本项目的 Source 很“重”，不仅负责读取，也承担了窗口调度和算法执行。

### 2.3 Operator / Map

Source 输出 `AlertRecord` 后，经过：

```java
.map(new AlertJsonMapper())
```

把 Java 对象转换为 JSON。

这里没有复杂业务算法，只是序列化。

### 2.4 Sink

Sink 是处理结果写到哪里。

本项目有：

- `LocalLogSink`：写 TaskManager/YARN 日志；
- Kafka Sink：配置开启后写 Kafka；
- `NoOpStringSink`：两个输出都关闭时避免拓扑没有下游。

---

## 3. Parallelism 为什么固定为 1？

配置：

```properties
source.parallelism=1
```

代码也强制检查：

```java
if (getRuntimeContext().getNumberOfParallelSubtasks() != 1) {
    throw new IllegalStateException(...);
}
```

原因是当前实现只有一个全局窗口游标：

```text
nextWindowStart
```

如果直接把 Source 并行度改成 2，那么两个 Source 实例可能同时查询相同的 Doris 窗口、各自维护自己的历史基线，造成重复告警和状态分裂。

所以：

**当前版本的 `source.parallelism=1` 是正确性约束，不是随便调大的性能参数。**

如果未来真的要扩容，需要先设计 Doris 查询分片、Context Sketch 合并、Pair 状态分片，再提高并行度。

---

## 4. Checkpoint 用一句话怎么理解？

可以把 Checkpoint 理解为：

> Flink 定期给“程序跑到哪里 + 算法记住了什么”拍一个一致性快照。

本项目要保存三类状态：

```text
1. nextWindowStart                 下一个要处理的 Doris 5分钟窗口
2. PairSnapshot                    Pair EMA 历史
3. ContextSnapshot                 Context t-digest 历史
```

对应 Source 中三个 `ListState`：

```java
cursorState
pairHistoryState
contextHistoryState
```

### 4.1 snapshotState()

Checkpoint 发生时：

```java
snapshotState(...)
```

把当前内存状态写到 Flink Operator State。

### 4.2 initializeState()

任务故障恢复时：

```java
initializeState(...)
```

如果 `context.isRestored()` 为 true，就恢复游标、Pair EMA 和 Context 历史。

### 4.3 为什么 checkpoint.storage.path 很重要？

如果生产环境没有配置一个所有 YARN 容器都能访问的可靠持久存储，那么任务重新提交或某些故障恢复场景可能无法拿回历史状态。

生产建议使用类似：

```properties
checkpoint.storage.path=hdfs:///flink-checkpoints/net-traffic-sentinel
```

实际路径要以你们集群规范为准。

启动时重点观察：

```text
Restored Doris window cursor from checkpoint: ...
Restored historical state: pairEntries=..., contextBuckets=...
```

如果你本来认为有历史，但看到：

```text
pairEntries=0, contextBuckets=0
```

就要优先检查 Checkpoint/恢复链路。

---

# 第二部分：数据为什么按 5 分钟处理

## 5. 本项目的 5 分钟窗口不是 Flink Window API

常规 Flink 教程会看到：

```java
.keyBy(...)
.window(...)
```

本项目没有这么做。

这里的窗口是：

```text
Doris SQL WHERE collectTime >= start
          AND collectTime < end
```

例如：

```text
[2026-08-13 00:00:00, 2026-08-13 00:05:00)
```

采用左闭右开 `[start, end)`，这样：

```text
00:04:59 属于第一个窗口
00:05:00 属于下一个窗口
```

不会重复。

窗口对象在：

```text
WindowRange.java
```

---

## 6. 为什么不能直接处理“系统当前时间前 5 分钟”？

因为 Doris 数据可能晚到。

例如：

```text
机器当前时间：20:00
Doris 实际最新 collectTime：12:37
```

如果程序直接处理：

```text
[19:55, 20:00)
```

可能得到 0 行，但这些数据其实只是还没入 Doris，而不是业务真的没有数据。

因此项目用 Doris 自己的数据进度决定处理到哪里。

---

## 7. 安全数据水位怎么算？

核心公式：

```text
safeWindowEnd
= floorToWindow(
    MAX(collectTime) - source.doris.stable.delay.minutes
  )
```

默认：

```properties
source.doris.stable.delay.minutes=60
window.size.minutes=5
```

假设：

```text
MAX(collectTime) = 12:37
```

先减 60 分钟：

```text
11:37
```

再向下对齐 5 分钟：

```text
11:35
```

于是：

```text
safeWindowEnd = 11:35
```

程序最多处理：

```text
[11:30, 11:35)
```

实现位置：

```text
DorisPollingAlertSource.refreshDataWatermarkIfNeeded()
```

---

## 8. nextWindowStart 是什么？

它就是“消费游标”。

如果：

```text
nextWindowStart = 2026-08-13 00:10:00
```

下一次处理：

```text
[00:10, 00:15)
```

窗口成功后：

```java
nextWindowStart = window.getEnd();
```

变成：

```text
00:15
```

如果 Doris 查询失败，代码明确不推进游标，因此下一次仍重试同一个窗口。

---

## 9. 为什么空窗口默认也不推进？

配置：

```properties
source.empty.window.advance=false
```

当“看起来已经成熟”的窗口查询结果仍为 0 行时，项目默认：

```text
不推进 cursor
-> 等待
-> 刷新 Doris 水位
-> 再查同一个窗口
```

这是为了降低数据晚到导致永久漏处理的概率。

注意：`MAX(collectTime)-delay` 是工程上的稳定性策略，不是数据库提供的“批次已完整”证明。

---

# 第三部分：一个窗口从 Doris 到告警到底经历什么

## 10. queryAndAnalyze() 的生命周期

文件：

```text
DorisPollingAlertSource.java
```

方法：

```text
queryAndAnalyze(WindowRange window)
```

执行顺序：

```text
1. 生成 Doris SQL
2. new FiveMinuteWindowAnalyzer(...)
3. JDBC forward-only 流式读取
4. 每读一行 -> MetricRecord
5. analyzer.add(record, collectTime)
6. 全部读完 -> analyzer.finish()
7. 得到：alerts + historyUpdate
8. 返回 run() 主循环
9. 在 checkpointLock 内：输出告警 + 更新历史 + 推进 cursor
```

### 10.1 为什么流式 JDBC 很重要？

5 分钟窗口可能包含非常多记录。

代码不会：

```text
SELECT 完 -> 把几百万行放进 List -> 再计算
```

而是：

```text
ResultSet.next()
-> 读一行
-> 更新摘要结构
-> 下一行
```

这正是后面 t-digest、固定容量候选堆、Space-Saving 的意义：**尽量让内存占用与总行数解耦。**

---

## 11. MetricRecord 表示什么？

文件：

```text
model/MetricRecord.java
```

关键字段：

```text
collectTime
srcIp
dstIp
protocol
connCount
c2sPkts / s2cPkts
c2sBytes / s2cBytes
```

算法用到：

```text
totalBytes = c2sBytes + s2cBytes
totalPkts  = c2sPkts  + s2cPkts
```

Pair 定义：

```text
srcIp + dstIp + protocol
```

例如：

```text
49.163.77.37|122.188.57.53|UNKNOWN
```

历史状态实际不保存这个完整字符串，而是使用稳定 64 位 hash。

---

# 第四部分：anomalyType=2 异常大流量算法

## 12. 先把 type=2 浓缩成 8 行伪代码

```text
for 每个“大流量候选记录”:
    context = 选历史Context；历史不成熟则用当前窗口Context
    pair = 查询这个 src/dst/protocol 的 Pair EMA

    baseline = Pair成熟 ? Pair EMA : Context P50

    bytesThreshold = max(Context高分位bytes, baselineBytes * 4)
    pktsThreshold  = max(Context高分位pkts,  baselinePkts  * 4)

    if currentBytes > bytesThreshold
       and (currentPkts > pktsThreshold
            or currentBytes > Context高分位bytes * 2):
        产生 anomalyType=2
```

然后再决定是否学习进 Pair EMA。

---

## 13. 为什么先有 LargeTrafficAccumulator？

如果窗口里有 3000 万行，把每一行都留到最后再判断会非常耗内存。

因此：

```text
LargeTrafficAccumulator
```

只保留两种信息。

### 13.1 每个 Context 的 t-digest

它用来近似回答：

```text
P50 是多少？
P90 是多少？
P99.9 是多少？
```

不需要保存所有原始数值。

### 13.2 最大流量候选的固定容量最小堆

配置：

```properties
rule.large.candidate.capacity=20000
```

含义：

**窗口所有行仍然都会被读取并进入 Context 分布统计，但是最终只保留 totalBytes 最大的 20000 条记录用于 Pair 级 type=2 检测和 Pair 学习。**

最小堆工作方式：

```text
堆未满 -> 直接加入
堆已满 -> 新记录 bytes > 堆顶最小值？
           是：踢掉堆顶，加入新记录
           否：忽略该记录作为 Pair 候选
```

这样最终堆里近似保存“本窗口最大 20000 条”。

---

## 14. Context 到底是什么？

代码：

```text
ContextKey.of(protocol, collectTime, slotMinutes)
```

默认：

```properties
history.context.slot.minutes=5
```

Context 由三部分组成：

```text
protocol + WORKDAY/WEEKEND + 5分钟槽位
```

例如：

```text
2026-08-13 00:02
protocol=UNKNOWN
2026-08-13 是工作日
```

归到：

```text
UNKNOWN + WORKDAY + slot0
```

其中：

```text
slot0 = 00:00~00:05
slot1 = 00:05~00:10
...
```

### 14.1 非常重要：Context 不包含 IP

所以：

```text
A -> B, UNKNOWN, 00:02
C -> D, UNKNOWN, 00:03
E -> F, UNKNOWN, 00:04
```

都共享：

```text
UNKNOWN + WORKDAY + slot0
```

这正是之前多条告警都出现 `baseline_bytes=100 / baseline_pkts=1` 的直接原因之一。

---

## 15. P50、P90、P99.9 是什么？

以 bytes 为例，把一个 Context 的流量从小到大排序。

### 15.1 P50

50% 的样本小于等于它，也就是中位数。

如果：

```text
P50 bytes = 100
```

说明这个 Context 至少一半记录总字节数大约不超过 100。

P50 在本项目主要作为“普通水平 baseline”。

### 15.2 P90

90% 样本小于等于它。

新版告警把它输出，主要用于人工理解分布，不直接决定最终 type=2 条件。

### 15.3 P99.9

默认配置：

```properties
rule.large.bytes.quantile=0.999
rule.large.pkts.quantile=0.999
```

也就是高分位阈值。

它可以理解为：

> 绝大多数正常/常见记录都应该低于这个水平，只有分布最右侧很少一部分会超过。

真正的 `context_high_quantile_bytes` / `context_high_quantile_pkts` 就来自这里。

---

## 16. t-digest 是什么？为什么不用 List 排序？

如果要精确计算 P99.9，最直观方式是：

```text
把所有数存起来 -> 排序 -> 取第 99.9% 位置
```

但海量数据会占用巨大内存。

`t-digest` 是一种分位数摘要结构。它不保留全部原始值，而是用压缩后的统计摘要近似回答分位数查询。

本项目中：

```text
rawBytes / rawPkts
```

用于当前窗口真实分布。

```text
historyBytes / historyPkts
```

用于准备写入长期历史分布。

二者故意分开，因为历史更新需要防污染。

---

## 17. Context 历史为什么按“日期桶”保存？

`HistoricalBaselineStore` 结构可以想象成：

```text
Context: UNKNOWN|WORKDAY|slot0
    2026-08-10 -> bytes t-digest + pkts t-digest
    2026-08-11 -> bytes t-digest + pkts t-digest
    2026-08-12 -> bytes t-digest + pkts t-digest
```

配置：

```properties
history.context.retention.days=7
history.context.min.days=2
```

检测 8 月 13 日时，会合并保留期内 **8 月 13 日之前** 的日期桶。

当前日期不会用于自己的历史判断。

只有贡献历史日期数达到 `min.days=2`，`ContextStats.usable` 才为 true。

### 17.1 一个容易忽略的细节

这里的 `min.days` 是“历史日期数”，不是“历史 5 分钟窗口数”。

同一个 Context（例如工作日 00:00~00:05）通常每天出现一次，因此至少需要两个之前日期的同类 Context 才正式启用历史 Context。

---

## 18. Context 冷启动时怎么办？

`LargeTrafficDetector.effectiveContext()`：

```text
历史 Context usable
    -> 用历史 Context
否则
    -> 用当前 5 分钟窗口自己的分布
```

因此新版告警会出现：

```text
context_source = HISTORICAL
```

或：

```text
context_source = CURRENT_WINDOW
```

如果是后者，baseline source 也可能是：

```text
CURRENT_CONTEXT_P50
```

这就是“当前窗口 P50 兜底”。

---

## 19. Context 历史怎么防止被巨大异常污染？

假设已有历史：

```text
历史 P99.9 bytes = 1 MB
```

当前突然出现：

```text
500 MB
```

如果直接把 500 MB 加入历史分布，会把未来分位数慢慢抬高。

代码在写 Context 历史前做封顶：

```text
historyValue = min(currentValue, historicalHighQuantile)
```

于是：

```text
min(500 MB, 1 MB) = 1 MB
```

`t-digest rawBytes` 仍然看到真实 500 MB，用于当前窗口判断；

`t-digest historyBytes` 只学习被截断后的 1 MB，用于未来历史。

这是“检测值”和“学习值分离”的重要设计。

---

## 20. Pair EMA 是什么？

Context 是“同类流量的群体画像”。

Pair EMA 是“这个具体 IP 对自己的历史画像”。

Pair：

```text
srcIp + dstIp + protocol
```

例如某文件服务器每天就是比别人流量大，那么 Context P50 可能永远偏低，Pair 自己的 EMA 更能描述它的正常水平。

---

## 21. EMA 公式怎么理解？

配置：

```properties
history.pair.ema.alpha=0.10
```

公式：

```text
newEMA = oldEMA * (1-alpha) + current * alpha
```

也就是：

```text
newEMA = oldEMA * 0.9 + current * 0.1
```

假设：

```text
旧 EMA = 1,000,000 bytes
新学习样本 = 2,000,000 bytes
```

那么：

```text
new EMA
= 1,000,000 * 0.9 + 2,000,000 * 0.1
= 1,100,000
```

不会因为一次波动立刻从 1MB 跳到 2MB。

`alpha` 越小，EMA 越稳定、适应新趋势越慢；越大，越敏感。

---

## 22. Pair 什么时候算成熟？

配置：

```properties
history.pair.min.samples=3
```

逻辑：

```text
pair.sampleCount >= 3
-> pairMature = true
-> baseline 可以使用 Pair EMA
```

否则：

```text
仍使用 Context P50
```

### 22.1 很重要：sampleCount 不等于“3 个不同的 5 分钟窗口”

当前代码的 `PairSample` 来源是候选记录。

如果同一个 Pair 在一个窗口里有多条记录同时进入候选集合，`apply()` 可以在同一个窗口里连续学习多次，sampleCount 也会连续增加。

因此：

```text
pair_sample_count
```

准确含义是“这个 Pair 被写入 EMA 的学习样本数量”，不能简单解释成“历史窗口数量”。

---

## 23. baseline 的三级选择逻辑

这是排查 type=2 最关键的一段。

### 情况 A：Pair 已成熟

```text
baseline_source = PAIR_EMA
baseline_bytes  = Pair EMA bytes
baseline_pkts   = Pair EMA pkts
```

### 情况 B：Pair 未成熟，但 Context 历史成熟

```text
baseline_source = HISTORICAL_CONTEXT_P50
baseline = 历史 Context P50
```

### 情况 C：Pair 未成熟，Context 历史也冷启动

```text
baseline_source = CURRENT_CONTEXT_P50
baseline = 当前 5分钟 Context P50
```

所以看到：

```text
baseline_bytes=100
baseline_pkts=1
```

第一反应不应该是“代码是不是把 100/1 写死了”，而应该先看：

```text
baseline_source
pair_sample_count
context_source
context_historical_days
```

---

## 24. type=2 真正阈值怎么算？

配置默认：

```properties
rule.large.bytes.baseline.multiplier=4.0
rule.large.pkts.baseline.multiplier=4.0
rule.large.extreme.multiplier=2.0
```

### 24.1 Bytes 阈值

```text
bytes_threshold
= max(
    context_high_quantile_bytes,
    baseline_bytes * 4
  )
```

### 24.2 Packets 阈值

```text
pkts_threshold
= max(
    context_high_quantile_pkts,
    baseline_pkts * 4
  )
```

### 24.3 Extreme Bytes 阈值

```text
extreme_bytes_threshold
= context_high_quantile_bytes * 2
```

因此即便：

```text
baseline_bytes = 100
```

也不表示：

```text
> 400 bytes 就报警
```

如果 Context P99.9 是 1,500,000 bytes，那么：

```text
bytes_threshold
= max(1,500,000, 100*4)
= 1,500,000
```

新版告警之所以要输出 `bytes_threshold`，就是为了避免这种误读。

---

## 25. 最终 type=2 条件

先算：

```text
bytes_anomaly = current_bytes > bytes_threshold
pkts_anomaly  = current_pkts  > pkts_threshold
extreme_bytes = current_bytes > extreme_bytes_threshold
```

最终：

```text
bytes_anomaly
AND
(pkts_anomaly OR extreme_bytes)
```

才产生 anomalyType=2。

为什么不是只看 bytes？

因为：

- bytes 和 pkts 同时异常，可信度更高；
- 如果 bytes 已经极端大，即使 pkts 没超过自己的阈值，也仍允许告警。

---

## 26. 为什么旧逻辑会出现 Pair EMA “永远学不到”？

旧逻辑核心思想：

```text
异常样本不能学习
```

本意正确，因为攻击流量不能直接写入正常基线。

但会出现自锁：

```text
Pair 没历史
-> baseline 用很小的 Context P50
-> 第一次就报警
-> 因为报警所以不学习
-> Pair sampleCount 仍为 0
-> 下一次还是用 Context P50
-> 再报警
-> 再不学习
-> 永远无法达到 min.samples
```

这就是“学习饿死”。

---

## 27. 新版 CAPPED_BOOTSTRAP 怎么修？

配置：

```properties
history.pair.bootstrap.anomaly.capped.learning.enabled=true
```

当：

```text
Pair 未成熟
AND
当前记录被判 type=2
```

不再完全跳过，而是：

```text
learningBytes = min(currentBytes, contextHighQuantileBytes)
learningPkts  = min(currentPkts,  contextHighQuantilePkts)
```

告警输出：

```text
pair_learning_mode = CAPPED_BOOTSTRAP
```

以及：

```text
pair_learning_cap_bytes
pair_learning_cap_pkts
```

### 示例

假设：

```text
current_bytes = 5 MB
Context P99.9 = 1 MB
```

告警仍然基于真实的 5 MB 产生。

但 Pair EMA 学习：

```text
min(5 MB, 1 MB) = 1 MB
```

于是 Pair 能逐渐拥有自己的历史，又不会直接把 5 MB 异常值完整灌进 EMA。

---

## 28. Pair 成熟后为什么仍然不学习异常？

一旦：

```text
pair_sample_count >= pair_min_samples
```

Pair 已经有一个可用的自身 EMA。

这时如果又发生异常：

```text
pair_learning_mode = SKIP_ANOMALOUS_MATURE
```

不写入 EMA。

这是为了保留防污染能力。

所以新策略不是“异常都学习”，而是：

```text
冷启动异常 -> 封顶学习
成熟异常   -> 不学习
正常样本   -> 正常学习
```

---

## 29. NORMAL / CAPPED_BOOTSTRAP / SKIP 各代表什么？

| pair_learning_mode | 含义 | 是否写 Pair EMA |
|---|---|---|
| `NORMAL` | 该 Pair 没有被当前窗口判为异常 | 写真实候选值 |
| `CAPPED_BOOTSTRAP` | Pair 未成熟但触发异常，启用受限学习 | 写封顶值 |
| `SKIP_ANOMALOUS_MATURE` | Pair 已成熟且异常 | 不写 |
| `SKIP_ANOMALOUS` | Pair 未成熟异常，但受限学习配置关闭 | 不写 |

---

## 30. 同一 Pair 一窗多条告警吗？

`LargeTrafficDetector` 内：

```text
bestAlertByPair
```

会对同一个完整 Pair key 做去重，只保留 `currentBytes` 最大的那条告警。

所以一个 5 分钟候选集合里同一个 Pair 多条记录都异常时，最终 type=2 告警通常只输出该 Pair 最大 bytes 的一条。

但请注意：

**告警去重不等于学习样本也只保留一条。**

Pair 学习循环仍会遍历候选记录，因此前面强调的 `sampleCount` 是“学习样本数”而非严格的窗口数。

---

# 第五部分：为什么你之前看到 baseline 全是 100 / 1

## 31. 用你那批日志走一遍

你之前的记录集中在：

```text
2026-08-13 00:00~00:04
```

并且：

```text
protocol=UNKNOWN
```

假设这些 Pair 都还没有达到：

```text
pair_sample_count >= 3
```

那么它们不会用 Pair EMA。

2026-08-13 是工作日，00:00~00:04 都属于 slot0，所以 Context 一样：

```text
UNKNOWN + WORKDAY + slot0
```

如果这个 Context 当时还没有至少 2 个历史日期，则使用：

```text
CURRENT_CONTEXT_P50
```

而当前 5 分钟窗口里这个 Context 的：

```text
P50 bytes = 100
P50 pkts  = 1
```

那么所有未成熟 Pair 自然都会看到：

```text
baseline_bytes = 100
baseline_pkts  = 1
```

这不是常量，而是它们共享的 Context P50。

---

## 32. protocol=UNKNOWN 为什么值得重点关注？

因为 Context 的第一维就是 protocol。

如果大量不同业务都变成：

```text
UNKNOWN
```

那么：

```text
心跳小包
扫描小流
普通业务
文件传输
数据库访问
其它协议
```

可能全部混在一个 Context 分布里。

大量小流会把 P50 压得很低，例如：

```text
100 bytes / 1 packet
```

虽然 P99.9 能一定程度保护阈值，但 Context 的业务同质性仍会下降。

因此新版告警看到 `protocol=UNKNOWN` 时，建议同时排查协议识别/字段映射。

---

# 第六部分：新版 type=2 告警怎么读

## 33. 告警字段按 5 组看，不要从头到尾硬读

### 第一组：当前值

```text
current_bytes
current_pkts
```

回答：这条记录实际有多大？

### 第二组：baseline

```text
baseline_source
baseline_bytes
baseline_pkts
pair_sample_count
pair_min_samples
```

回答：普通水平参考来自哪里？Pair 有没有成熟？

### 第三组：Context 分布

```text
context_source
context_historical_days
context_p50_bytes
context_p50_pkts
context_p90_bytes
context_p90_pkts
context_bytes_quantile
context_pkts_quantile
context_high_quantile_bytes
context_high_quantile_pkts
```

回答：同类流量整体分布是什么样？高分位是多少？历史是否成熟？

### 第四组：真正判定阈值

```text
bytes_baseline_multiplier
pkts_baseline_multiplier
bytes_threshold
pkts_threshold
extreme_multiplier
extreme_bytes_threshold
bytes_anomaly
pkts_anomaly
extreme_bytes
```

回答：最终为什么命中规则？

### 第五组：Pair 如何学习

```text
pair_learning_mode
pair_learning_cap_bytes
pair_learning_cap_pkts
```

回答：这次告警发生后，Pair EMA 会不会被更新？更新多少？

---

## 34. baseline_source 四步排查法

看到 type=2，先只看：

```text
baseline_source
```

### PAIR_EMA

说明 Pair 已成熟。

继续看：

```text
pair_sample_count
baseline_bytes
bytes_threshold
```

### HISTORICAL_CONTEXT_P50

说明 Pair 未成熟，但 Context 有足够历史天数。

继续看：

```text
context_historical_days
context_p50_bytes
context_high_quantile_bytes
```

### CURRENT_CONTEXT_P50

说明 Pair 和 Context 都在冷启动阶段。

继续看：

```text
pair_sample_count
context_historical_days
pair_learning_mode
```

如果长期一直 CURRENT_CONTEXT_P50，就要排查：

- Checkpoint 是否反复丢失；
- Pair 是否真的有候选学习样本；
- Context 是否积累到 `min.days`；
- 时间/日期是否按预期推进。

---

## 35. 一个“示意告警”怎么解释

下面数字仅用于说明读法：

```json
{
  "current_bytes": 5047588,
  "current_pkts": 3404,
  "baseline_source": "CURRENT_CONTEXT_P50",
  "baseline_bytes": 100,
  "baseline_pkts": 1,
  "pair_sample_count": 0,
  "pair_min_samples": 3,
  "context_source": "CURRENT_WINDOW",
  "context_historical_days": 0,
  "context_high_quantile_bytes": 1200000,
  "context_high_quantile_pkts": 900,
  "bytes_threshold": 1200000.0,
  "pkts_threshold": 900.0,
  "extreme_bytes_threshold": 2400000.0,
  "bytes_anomaly": true,
  "pkts_anomaly": true,
  "extreme_bytes": true,
  "pair_learning_mode": "CAPPED_BOOTSTRAP",
  "pair_learning_cap_bytes": 1200000,
  "pair_learning_cap_pkts": 900
}
```

读法：

```text
Pair 没历史 -> sampleCount=0
Context 也没成熟 -> CURRENT_WINDOW
所以 baseline 用当前 Context P50 = 100/1

真正 bytesThreshold 不是 400，而是 1,200,000
当前 5,047,588 > 1,200,000 -> bytes_anomaly=true
当前 3,404 > 900 -> pkts_anomaly=true
同时 5,047,588 > 2,400,000 -> extreme_bytes=true

因此满足：
bytesAnomaly && (pktsAnomaly || extremeBytes)

Pair 仍在冷启动 -> CAPPED_BOOTSTRAP
本次最多向 EMA 学 1,200,000 bytes / 900 pkts
```

---

# 第七部分：历史状态什么时候真正更新

## 36. 为什么不是 analyzer.add() 时就改历史？

如果边读 Doris 边修改长期历史，会出现：

```text
已经读了 300 万行并写进历史
-> JDBC 网络异常
-> 整个窗口失败
-> 下次重试这个窗口
-> 前 300 万行又写一次
```

历史会被重复污染。

所以项目采用两阶段思路：

```text
查询阶段：
只读取旧历史 + 计算当前窗口临时结果

窗口完整成功后：
生成 WindowUpdate
-> Source 在 checkpointLock 内 historyStore.apply()
```

这就是 `FiveMinuteWindowAnalyzer.finish()` 返回 `WindowAnalysisResult` 的意义。

---

## 37. checkpointLock 为什么包住“告警 + 历史 + 游标”？

代码把：

```text
ctx.collect(alert)
historyStore.apply(...)
nextWindowStart = windowEnd
```

放在：

```java
synchronized (ctx.getCheckpointLock())
```

里面。

目的是让 Flink 做状态快照时，不要正好拍到：

```text
历史已经更新
但 cursor 还没推进
```

或者：

```text
cursor 已推进
但历史还没更新
```

这样的中间状态。

对于初学者，可以把它理解成：

> “处理完一个窗口”的几个关键动作要尽量表现成一个不可被 Checkpoint 从中间切开的整体。

---

# 第八部分：anomalyType=3 简明说明

## 38. type=3 做什么？

规则：非工作时段活跃连接 Top-N。

`FiveMinuteWindowAnalyzer.add()` 先通过：

```text
TimeUtils.isOffHours(...)
```

筛出非工作时段记录。

然后：

```text
OffHoursAccumulator
```

使用 `WeightedSpaceSavingSketch` 近似保存连接数最大的 Pair。

窗口结束：

```text
OffHoursDetector
```

取 Top N，输出 anomalyType=3。

如果你当前只排查 type=2，可以暂时跳过：

```text
OffHoursAccumulator
OffHoursDetector
WeightedSpaceSavingSketch
```

---

# 第九部分：配置文件怎么读

## 39. 最值得先懂的配置

### Job / 时间

```properties
job.name=NET-TRAFFIC-ANOMALY
business.timezone=Asia/Shanghai
window.size.minutes=5
```

### Doris 数据成熟度

```properties
source.watermark.poll.interval.seconds=60
source.watermark.lookback.days=3
source.doris.stable.delay.minutes=60
source.empty.window.advance=false
```

### type=2 高分位和倍率

```properties
rule.large.bytes.quantile=0.999
rule.large.pkts.quantile=0.999
rule.large.bytes.baseline.multiplier=4.0
rule.large.pkts.baseline.multiplier=4.0
rule.large.extreme.multiplier=2.0
```

### 候选和 t-digest

```properties
rule.large.candidate.capacity=20000
rule.large.tdigest.compression=200.0
```

`candidate.capacity` 越大：Pair 检测覆盖更多大流量记录，但内存和后续计算增加。

`t-digest compression` 越高通常精度/内存也更高，不建议没有压测就随意大幅调节。

### Context History

```properties
history.context.retention.days=7
history.context.slot.minutes=5
history.context.min.days=2
history.context.tdigest.compression=100.0
```

### Pair EMA

```properties
history.pair.ttl.days=7
history.pair.max.entries=200000
history.pair.ema.alpha=0.10
history.pair.min.samples=3
history.pair.bootstrap.anomaly.capped.learning.enabled=true
```

### Checkpoint

```properties
checkpoint.enabled=true
checkpoint.interval.ms=300000
checkpoint.timeout.ms=600000
checkpoint.min.pause.ms=30000
checkpoint.storage.path=
```

生产环境应结合集群配置填写可靠的持久化路径。

### 安全提醒

Doris/Kafka 凭证不建议直接提交到源码仓库。生产部署可使用外部 properties、环境/秘密管理方案，并限制配置文件权限。

---

# 第十部分：推荐的源码阅读顺序

## 40. 第一遍：只建立整体流程

按顺序：

```text
1. topology/NetTrafficSentinel.java
2. source/DorisPollingAlertSource.java
3. rule/FiveMinuteWindowAnalyzer.java
4. model/MetricRecord.java
5. model/AlertRecord.java
```

目标：知道数据从哪里来，到哪里去。

不要一上来研究 t-digest。

---

## 41. 第二遍：只看 type=2

```text
1. history/ContextKey.java
2. rule/LargeTrafficAccumulator.java
3. history/ContextStats.java
4. history/HistoricalBaselineStore.java
5. rule/LargeTrafficDetector.java
6. model/AlertRecord.largeTraffic()
```

目标：能自己手算一次告警。

---

## 42. 第三遍：理解状态恢复

```text
DorisPollingAlertSource.initializeState()
DorisPollingAlertSource.snapshotState()
HistoricalBaselineStore.snapshotPairs()
HistoricalBaselineStore.snapshotContexts()
HistoricalBaselineStore.restorePair()
HistoricalBaselineStore.restoreContext()
```

目标：理解“为什么任务重启以后基线还在”。

---

## 43. 第四遍：再看性能优化结构

```text
TDigest
PriorityQueue top candidates
WeightedSpaceSavingSketch
CountMinSketch
```

目标：理解为什么海量窗口不需要把全量明细放内存。

---

# 第十一部分：断点调试建议

## 44. 想追一条 type=2 告警，断点放哪里？

推荐：

```text
1. DorisPollingAlertSource.queryAndAnalyze()
   看 MetricRecord 是否正确读取

2. FiveMinuteWindowAnalyzer.add()
   看 contextKey

3. LargeTrafficDetector.detect()
   看 pairSampleCount / pairMature

4. baseline 选择 if/else
   看 baselineSource

5. bytesThreshold / pktsThreshold
   手算一次

6. if (bytesAnomaly && (...))
   看为什么进入

7. CAPPED_BOOTSTRAP 分支
   看 capBytes / capPkts

8. HistoricalBaselineStore.apply()
   看 EMA 最终如何变化
```

---

## 45. 本地调试时建议观察的变量

```text
record.pairKey()
candidate.getContextKey()
contextSelection.historicalUsable
contextSelection.historicalDays
context.p50Bytes
context.thresholdBytes
pair.sampleCount
pair.emaBytes
baselineSource
baselineBytes
bytesThreshold
currentBytes
bytesAnomaly
pktsAnomaly
extremeBytes
pairLearningMode
```

---

# 第十二部分：日志排障手册

## 46. 情况 A：baseline 长期都是 100/1

依次看：

```text
baseline_source
pair_sample_count
context_source
context_historical_days
protocol
```

### 如果 baseline_source=CURRENT_CONTEXT_P50

说明 Pair 未成熟 + Context 也未成熟。

检查：

```text
任务是否刚启动？
Checkpoint 是否恢复？
contextHistoricalDays 为什么达不到 2？
Pair 有没有通过 CAPPED_BOOTSTRAP 学习？
```

### 如果 baseline_source=HISTORICAL_CONTEXT_P50

说明 Context 历史就是 100/1，可能确实由大量小流组成。

检查：

```text
protocol 是否大量 UNKNOWN？
Context 粒度是否过粗？
```

### 如果 baseline_source=PAIR_EMA 仍是 100/1

这时重点看 Pair 的学习样本本身是否长期极小，或者恢复/EMA 更新是否符合预期。

---

## 47. 情况 B：一直 CAPPED_BOOTSTRAP，不转 PAIR_EMA

看：

```text
pair_sample_count
pair_min_samples
```

如果 sampleCount 不增加：

1. 确认配置 `history.pair.bootstrap.anomaly.capped.learning.enabled=true`；
2. 确认窗口最终成功执行了 `historyStore.apply()`；
3. 确认任务没有每次都从空状态重启；
4. 看 Pair 是否因为 TTL/容量被淘汰；
5. 注意候选集合只保留 top `candidate.capacity`，该 Pair 是否真的持续进入候选集合。

---

## 48. 情况 C：PAIR_EMA 突然又变回 Context

可能原因：

```text
Pair 超过 TTL 没有再次学习
Pair 状态超过 max.entries 被淘汰
Checkpoint 没恢复
Pair key 发生变化（例如 protocol 从 UNKNOWN 变成 TCP）
```

Pair key 中 protocol 是组成部分：

```text
A|B|UNKNOWN
```

和：

```text
A|B|TCP
```

是两个不同 Pair。

---

## 49. 情况 D：有告警但看不懂为什么

新版直接核对：

```text
current_bytes > bytes_threshold ?
current_pkts  > pkts_threshold  ?
current_bytes > extreme_bytes_threshold ?
```

然后验证：

```text
bytes_anomaly && (pkts_anomaly || extreme_bytes)
```

不需要再根据 baseline 猜测真实阈值。

---

# 第十三部分：测试代码应该怎么看

## 50. 为什么测试比直接读实现更适合？

测试通常是：

```text
给定输入
-> 执行算法
-> 断言输出
```

比生产代码少很多基础设施噪声。

重点先看：

```text
src/test/java/cn/ac/iie/anomaly/rule/LargeTrafficDetectorTest.java
```

尤其关注两个场景：

```text
冷启动异常 -> CAPPED_BOOTSTRAP
成熟 Pair 异常 -> SKIP_ANOMALOUS_MATURE
```

然后再看：

```text
HistoricalBaselineStoreTest
FiveMinuteWindowAnalyzerTest
```

---

# 第十四部分：容易混淆的概念

## 51. baseline 和 threshold 不是一回事

```text
baseline = “一般情况下这个 Pair/Context 大概多大”
threshold = “超过多少才认为异常”
```

threshold 会结合：

```text
Context 高分位
baseline * multiplier
```

因此日志里的 baseline 小，不代表报警门槛就小。

---

## 52. Context 历史和 Pair 历史不是一回事

### Context

```text
群体画像
protocol + 工作日类型 + 时间槽
t-digest
P50/P90/P99.9
按日期桶保留
```

### Pair

```text
个体画像
srcIp + dstIp + protocol
EMA
sampleCount
TTL + 最大条目数
```

它们一起用，解决两个矛盾：

```text
新 Pair 没历史 -> 需要群体基线兜底
稳定的大流量业务 Pair -> 需要自己的个体基线
```

---

## 53. Doris “数据水位”与 Flink Event-Time Watermark 不是同一个概念

项目日志里说的安全数据 Watermark 是业务层自己计算的：

```text
MAX(collectTime) - stable delay
```

不是 Flink DataStream API 中通过 `WatermarkStrategy` 生成的 Event-Time Watermark。

两者思想类似：都在回答“数据进度到哪里了”；

但实现机制完全不同。

---

# 第十五部分：当前架构的边界和后续优化点

## 54. Source 同时做查询和算法，优点与代价

优点：

```text
实现直观
窗口顺序天然可控
历史更新容易与 cursor 放在同一一致性区域
```

代价：

```text
source.parallelism=1
Source 代码职责较重
单个窗口查询时间会直接决定处理吞吐
```

未来规模继续增长时，可以考虑把“Doris 读取”和“算法计算”拆成更典型的 Flink 算子，但那会引入分片、聚合、状态一致性等额外复杂度。

---

## 55. Context 粒度未来可以继续优化

当前：

```text
protocol + dayType + slot
```

如果 `protocol=UNKNOWN` 占比很高，Context 区分能力会下降。

未来可评估引入：

```text
服务/端口
业务标签
方向
资产类型
```

但维度越细，单个 Context 样本越少、冷启动越慢，状态数量也更多。

因此不是“越细越好”，需要结合数据分布验证。

---

## 56. Pair sampleCount 的语义未来可以考虑收紧

当前 sampleCount 是“学习样本数”，同一 5 分钟窗口里同一 Pair 可能贡献多个候选样本。

如果业务希望：

```text
min.samples=3
```

严格表示“至少 3 个不同的历史窗口”，需要未来把 Pair 学习聚合成：

```text
每 Pair 每窗口只学习一次
```

例如学习该 Pair 当前窗口最大值、平均值或某个分位数。

这是算法语义选择，不是本次 CAPPED_BOOTSTRAP 修复必须修改的内容，但理解当前 `sampleCount` 含义非常重要。

---

# 第十六部分：一页速查表

## 57. type=2 最重要的对象

| 对象 | 一句话作用 |
|---|---|
| `MetricRecord` | Doris 一行指标 |
| `ContextKey` | protocol + 工作日类型 + 时间槽 |
| `LargeTrafficAccumulator` | 当前窗口分布摘要 + 最大候选 |
| `ContextStats` | P50/P90/高分位摘要 |
| `HistoricalBaselineStore` | Context t-digest + Pair EMA 长期历史 |
| `LargeTrafficDetector` | 选择 baseline、算阈值、判异常、决定学习模式 |
| `AlertRecord` | 把判定证据组装成最终 JSON |

---

## 58. type=2 最重要的公式

```text
baseline = Pair成熟 ? PairEMA : ContextP50

bytesThreshold = max(ContextBytesHighQ, baselineBytes * bytesMultiplier)
pktsThreshold  = max(ContextPktsHighQ,  baselinePkts  * pktsMultiplier)

extremeBytesThreshold = ContextBytesHighQ * extremeMultiplier

alert = bytesAnomaly && (pktsAnomaly || extremeBytes)
```

冷启动异常学习：

```text
learnBytes = min(currentBytes, ContextBytesHighQ)
learnPkts  = min(currentPkts,  ContextPktsHighQ)
```

EMA：

```text
newEMA = oldEMA*(1-alpha) + sample*alpha
```

---

## 59. type=2 排障最先看这 10 个字段

```text
baseline_source
baseline_bytes
baseline_pkts
pair_sample_count
context_source
context_historical_days
context_high_quantile_bytes
bytes_threshold
pkts_threshold
pair_learning_mode
```

---

# 第十七部分：术语表

## 60. Glossary

**Source**：Flink 数据入口。  
**Sink**：Flink 数据输出。  
**Operator**：流上的处理步骤，例如 Map。  
**Parallelism**：一个算子同时运行多少个并行实例。  
**State**：作业运行中需要长期记住的数据。  
**Checkpoint**：Flink 对 State 的周期性一致性快照。  
**Cursor**：本项目中下一个 Doris 5 分钟窗口的开始时间。  
**Context**：protocol + 工作日/周末 + 时间槽形成的群体类别。  
**Pair**：srcIp + dstIp + protocol。  
**Baseline**：普通流量参考水平。  
**Threshold**：真正触发异常比较的阈值。  
**Quantile / 分位数**：数据分布位置，如 P50/P99.9。  
**t-digest**：近似计算分位数的压缩摘要结构。  
**EMA**：指数移动平均，使个体基线平滑更新。  
**TTL**：状态超过指定时间未更新后视为过期。  
**CAPPED_BOOTSTRAP**：Pair 冷启动异常时，只学习被 Context 高分位封顶后的值。  
**Checkpoint Lock**：Source 与 Checkpoint 协调一致状态的锁。  
**Watermark（本项目业务语义）**：根据 Doris MAX(collectTime) 估计的安全处理进度。  

---

# 附录 A：关键文件职责表

| 文件 | 建议阅读优先级 | 职责 |
|---|---:|---|
| `topology/NetTrafficSentinel.java` | ★★★★★ | 作业入口、Flink 拓扑、Checkpoint、Sink |
| `source/DorisPollingAlertSource.java` | ★★★★★ | Doris 水位、窗口游标、JDBC、状态恢复、窗口提交 |
| `rule/FiveMinuteWindowAnalyzer.java` | ★★★★★ | 单窗口算法总调度 |
| `rule/LargeTrafficDetector.java` | ★★★★★ | type=2 核心判定 |
| `rule/LargeTrafficAccumulator.java` | ★★★★★ | t-digest 与候选 Top-K |
| `history/HistoricalBaselineStore.java` | ★★★★★ | Context 历史与 Pair EMA |
| `history/ContextKey.java` | ★★★★☆ | Context 分组规则 |
| `history/ContextStats.java` | ★★★★☆ | Context 分位摘要 |
| `model/AlertRecord.java` | ★★★★☆ | type=2 证据 JSON |
| `model/MetricRecord.java` | ★★★★☆ | 输入数据模型 |
| `config/WindowRange.java` | ★★★☆☆ | 5 分钟范围与 Doris 条件 |
| `rule/OffHoursDetector.java` | ★★☆☆☆ | type=3 Top-N |
| `sketch/*` | ★★☆☆☆ | 近似重频算法底层实现 |

---

# 附录 B：推荐学习路径

如果你每天只投入 30 分钟：

```text
第1天：看第0~10章 + NetTrafficSentinel.java
第2天：DorisPollingAlertSource.run() 和 queryAndAnalyze()
第3天：ContextKey + LargeTrafficAccumulator
第4天：HistoricalBaselineStore 的 Context 历史
第5天：Pair EMA + LargeTrafficDetector baseline 选择
第6天：阈值公式 + CAPPED_BOOTSTRAP
第7天：Checkpoint snapshot/restore + 用测试代码单步调试
```

读代码时不要追求一次记住所有 Flink API。先沿着一条告警的生命周期走通：

```text
Doris row
-> MetricRecord
-> Context/Candidate
-> baseline
-> threshold
-> AlertRecord
-> history update
-> Checkpoint
```

这条链路能自己讲清楚之后，再补 Flink 的系统知识会容易很多。

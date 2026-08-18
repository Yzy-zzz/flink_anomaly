# NetTrafficSentinel 部署与代码说明（历史基线 + Doris 延迟入库版）

本文假设你以前没有维护过 Flink 长任务，也尽量按“照着做即可”的方式解释。

---

# 1. 这个程序做什么

数据来源：

```text
Doris
anomaly_detection.metric_endpoint
```

数据本身已经按分钟聚合。

程序每次处理一个 5 分钟区间，例如：

```text
11:30:00 <= collectTime < 11:35:00
```

然后检测两类异常：

```text
2 = 异常大流量
3 = 非工作时段活跃连接 Top100
```

结果可以：

```text
写 TaskManager/YARN 日志
写 Kafka anomaly_alert
```

两个输出开关彼此独立。

---

# 2. 为什么不能按照系统当前时间直接查询

你们 Doris 存在明显延迟。

比如：

```text
系统现在 20:00
但是 Doris 今天 12:00 左右的数据才刚入库
```

如果程序认为：

```text
20:00 已经到了
=> 19:55~20:00 一定有数据
```

就会不停查到：

```text
rows=0
```

旧版还会把 cursor 向前推进，这样晚几个小时补进 Doris 的数据就永远不会再查。

新版不再这样做。

---

# 3. 新版 Doris 数据 Watermark

程序定期执行类似：

```sql
SELECT MAX(collectTime)
FROM anomaly_detection.metric_endpoint
WHERE dayTime >= '最近几天'
  AND dayTime <= '今天';
```

假设结果：

```text
MAX(collectTime)=12:37
```

这个结果只能说明：

```text
Doris 至少已经出现了 12:37 的记录
```

不能保证：

```text
12:36 以前所有迟到数据 100% 已经到齐
```

所以还要减一个稳定等待时间。

默认：

```properties
source.doris.stable.delay.minutes=60
```

于是：

```text
12:37 - 60分钟 = 11:37
```

再向下取 5 分钟边界：

```text
11:35
```

所以程序只允许处理：

```text
windowEnd <= 11:35
```

---

# 4. 如果 Doris 延迟变化很大怎么办

这个参数越大：

```properties
source.doris.stable.delay.minutes
```

数据越有机会完整，但告警越晚。

可以从 60 分钟开始。

如果日志仍经常看到：

```text
Empty Doris window ... Cursor is NOT advanced
```

说明：

```text
MAX collectTime 已经很靠后
但前面的窗口还没有真正到齐/甚至完全没到
```

可以改为：

```properties
source.doris.stable.delay.minutes=90
```

或者：

```properties
source.doris.stable.delay.minutes=120
```

由于你们本身就可能延迟数小时，多等 30~60 分钟通常比永久漏数据更重要。

---

# 5. 为什么 MAX(collectTime) 仍然不是绝对完整标记

举例：

```text
12:30 的数据已经到
12:10 还有部分批次没到
```

这时：

```text
MAX=12:30
```

但是 12:10 并不完整。

由于上游无法提供：

```text
12:10 COMPLETE
```

这种元数据，所以 Flink 端不可能百分之百知道窗口已经完整。

新版采用两道保险：

```text
MAX collectTime - stableDelay
              +
rows=0 时不推进 cursor
```

这是没有 COMPLETE 标记时的工程折中。

---

# 6. 空窗口处理

默认：

```properties
source.empty.window.advance=false
source.empty.window.retry.seconds=300
```

当一个窗口返回：

```text
rows=0
```

程序不会：

```text
cursor += 5min
```

而是：

```text
保留原 cursor
等待 300 秒
再次查 Doris Watermark
再次查询原窗口
```

对于每天 20~30 亿条的表，一个真正完整的 5 分钟窗口完全没有任何数据的概率通常很低，所以这个策略适合当前场景。

如果未来业务确实允许某些 5 分钟窗口完全没数据，可以改：

```properties
source.empty.window.advance=true
```

但这会重新引入“迟到空窗口被跳过”的风险。

---

# 7. type=2 为什么换了算法

旧版 `baseline_bytes` 可能出现：

```text
current_bytes=19404636
baseline_bytes=109
```

原因是旧 baseline 主要来自同一个 5 分钟窗口中的 CMS/中位数 fallback。

它不是：

```text
这个 IP Pair 过去正常是多少
```

新版把长期历史拆成两层：

```text
Context History
+
Pair History
```

---

# 8. Context History 是什么

Context 不是 IP。

ContextKey：

```text
protocol + WORKDAY/WEEKEND + 5分钟时间槽
```

例如：

```text
TLS + WORKDAY + 20:20~20:25
TLS + WEEKEND + 20:20~20:25
UNKNOWN + WORKDAY + 09:00~09:05
```

Context 中只记录：

```text
这一类连接的 bytes 分布
这一类连接的 pkts 分布
```

它不知道：

```text
61.1.1.1 -> 10.1.1.1
```

因此“Context Sketch 会不会永久保存所有 IP Pair”的答案是：

```text
不会，它根本不保存 IP Pair。
```

---

# 9. Context 为什么按日期分桶

t-digest 很适合：

```text
add
merge
quantile
```

但是已经合并好的 Sketch 不适合精确删除：

```text
7 天前的某一批原始行
```

所以不能做：

```text
一个永远增长的 t-digest
然后删除 7 天前数据
```

新版是：

```text
TLS|WORKDAY|20:20
    ├── 2026-08-11 digest
    ├── 2026-08-12 digest
    ├── 2026-08-13 digest
    ├── 2026-08-14 digest
    ├── 2026-08-15 digest
    ├── 2026-08-16 digest
    └── 2026-08-17 digest
```

过期直接扔掉整个日期桶。

默认：

```properties
history.context.retention.days=7
```

检测当前日期时，当前日期桶不会参与当前阈值计算，而是使用前面的历史日期。

---

# 10. Context 什么时候开始真正生效

默认：

```properties
history.context.min.days=2
```

意思是至少已经有两个历史日期，才把跨日 Context 当作成熟基线。

如果只有 0 或 1 个历史日期：

```text
Context cold start
```

暂时使用当前 5 分钟窗口自己的分布作为阈值。

这样第一次启动不会因为没有历史直接把大量连接报成异常。

---

# 11. Context 如何更新

一个窗口完整读取以后：

```text
先使用旧历史检测当前窗口
```

检测完成后才生成：

```text
Context History Update
```

最终在 checkpoint 临界区提交。

不能：

```text
先把当前窗口加到历史
再判断当前窗口
```

否则异常会抬高自己的阈值。

---

# 12. Context 如何减少异常污染

如果历史 Context 已经成熟，例如历史：

```text
P99.9 bytes = 3MB
```

当前来了：

```text
500MB
```

历史更新不会直接把 500MB 原值加入正常 Sketch，而是限制在历史高分位附近。

代码概念：

```text
historyBytes = min(currentBytes, oldHistoricalP99.9)
```

这样异常值仍然不会无限抬高之后的 Context 基线。

---

# 13. Pair History 是什么

Pair：

```text
srcIp + dstIp + protocol
```

例如：

```text
61.213.176.11|172.217.221.188|TLS
```

Pair History 保存：

```text
emaBytes
emaPkts
sampleCount
lastSeenEventTime
```

为了降低内存，不保存完整 Pair 字符串作为长期 HashMap key，而保存稳定 64 位 hash。

算法允许极低概率 hash collision，这是为了换取固定状态和更低内存。

---

# 14. Pair 会不会一直保存

不会。

有两种淘汰机制。

第一种：TTL。

```properties
history.pair.ttl.days=7
```

如果这个 Pair 连续 7 个“数据事件日”没有再次被学习：

```text
Pair baseline 视为过期
```

第二种：硬容量。

```properties
history.pair.max.entries=200000
```

无论业务产生多少 Pair，长期 PairHistory 最多保留约 20 万条。

超过后淘汰最久没有被更新的 Pair。

---

# 15. 为什么 TTL 用数据时间，而不是机器时间

你们数据会晚几个小时入库。

假如事件发生：

```text
12:00
```

但是机器到：

```text
20:00
```

才处理。

如果用 processing time 计算 7 天 TTL，会把“入库延迟”混进业务生命周期。

新版 Pair TTL 使用：

```text
collectTime / window event time
```

更符合历史流量语义。

---

# 16. Pair EMA 怎么更新

默认：

```properties
history.pair.ema.alpha=0.10
```

公式：

```text
new = old * 0.90 + current * 0.10
```

例如：

```text
old baseline = 200000
current      = 220000
```

新 baseline：

```text
202000
```

EMA 比全历史普通平均值更容易适应业务正常增长。

---

# 17. 为什么不是所有 Pair 都学习 EMA

每天几十亿条数据，不可能维护：

```text
Map<所有srcIp-dstIp-protocol, PairHistory>
```

每个 5 分钟窗口只保留固定数量的大流量 Candidate：

```properties
rule.large.candidate.capacity=20000
```

Pair EMA 只从这些 heavy candidate 中学习。

长期状态再由：

```properties
history.pair.max.entries=200000
```

做硬限制。

---

# 18. 异常 Pair 是否更新 EMA

受限更新。

如果某 Pair 在当前 5 分钟窗口内任何 Candidate 被判断为 type=2：

```text
Pair samples < history.pair.min.samples
    -> 冷启动受限学习：bytes/pkts cap 到有效 Context 高分位阈值后再写 EMA

Pair samples >= history.pair.min.samples
    -> 当前整个窗口都不回灌 EMA
```

默认开关：

```properties
history.pair.bootstrap.anomaly.capped.learning.enabled=true
```

这样解决“新 Pair 一触发异常就永远没有样本”的自锁，同时成熟 Pair 仍不会直接吸收异常大值。

---

# 19. baseline_bytes 最终是什么

优先级：

```text
1. Pair samples >= history.pair.min.samples
   -> Pair EMA

2. Pair 没足够样本，但 Context History 成熟
   -> Context historical P50

3. Context 也冷启动
   -> 当前 5 分钟 Context P50
```

默认：

```properties
history.pair.min.samples=3
```

因此第一次看见一个新 Pair 时，baseline 很小仍然可能是正常现象。

它表达的是：

```text
这个 Pair 自身没有历史，只能用同类连接中位数作为 fallback
```

真正决定报警还要看历史高分位。

---

# 20. type=2 最终阈值

默认：

```properties
rule.large.bytes.quantile=0.999
rule.large.pkts.quantile=0.999
rule.large.bytes.baseline.multiplier=4.0
rule.large.pkts.baseline.multiplier=4.0
rule.large.extreme.multiplier=2.0
```

大致逻辑：

```text
bytesThreshold = max(contextP99.9Bytes, baselineBytes * 4)
pktsThreshold  = max(contextP99.9Pkts, baselinePkts * 4)
```

告警：

```text
currentBytes > bytesThreshold
AND
(
    currentPkts > pktsThreshold
    OR
    currentBytes > contextP99.9Bytes * 2
)
```

---

# 21. type=3 算法没有变

非工作时段：

```text
工作日：20:00~08:00
周末：全天
```

每个 5 分钟窗口：

```text
item=srcIp|dstIp|protocol
weight=connCount
```

使用 Weighted Space-Saving 保存固定数量 Heavy Hitters。

默认：

```properties
rule.offhours.topn=100
rule.offhours.sketch.capacity=2048
```

---

# 22. 一个完整窗口的执行顺序

非常重要，顺序如下：

```text
1. 读取现有 Context / Pair 历史
2. JDBC streaming 查询 Doris 5 分钟
3. 当前窗口只更新临时 Sketch / Candidate Heap
4. 计算 type=2 / type=3
5. 生成 WindowHistoryUpdate
6. 查询完整成功后进入 checkpoint lock
7. 输出告警
8. 更新 Context 历史
9. 更新 Pair EMA
10. cursor += 5 分钟
```

如果第 2~5 步中途失败：

```text
长期历史没有变化
cursor 没有变化
```

任务重启后重新读同一个窗口即可。

---

# 23. Checkpoint 保存哪些状态

新版 Source Operator State：

```text
next-doris-five-minute-window-v2
large-traffic-pair-history-v2
large-traffic-context-history-v2
```

分别对应：

```text
下一个 5 分钟窗口游标
Pair EMA 状态
Context 日期桶 t-digest 状态
```

Context t-digest 在 checkpoint 中使用紧凑二进制序列化，不保存原始行。

---

# 24. 为什么 checkpoint 改为 5 分钟

旧版主要保存一个 cursor，所以 1 分钟 checkpoint 很轻。

新版最多可能有：

```text
20万 Pair History
+
多天 Context Bucket
```

因此默认改为：

```properties
checkpoint.interval.ms=300000
checkpoint.timeout.ms=600000
checkpoint.min.pause.ms=30000
```

如果实测 checkpoint 很快，可以再缩短。

---

# 25. 生产一定要配置 HDFS checkpoint

如果日志看到：

```text
Checkpoint storage is set to 'jobmanager'
```

说明没有配置外部持久化。

修改：

```properties
checkpoint.storage.path=hdfs:///flink-checkpoints/net-traffic-sentinel
```

目录必须：

```text
YARN JobManager 能访问
TaskManager 能访问
Flink/Hadoop 配置能解析 HDFS
```

否则整个 Application 丢失时历史基线也丢失。

---

# 26. 配置文件最重要的一组参数

```properties
window.size.minutes=5

source.start.mode=latest_data
source.watermark.poll.interval.seconds=60
source.watermark.lookback.days=3
source.doris.stable.delay.minutes=60
source.empty.window.advance=false
source.empty.window.retry.seconds=300

rule.large.candidate.capacity=20000

history.context.retention.days=7
history.context.slot.minutes=5
history.context.min.days=2
history.context.tdigest.compression=100

history.pair.ttl.days=7
history.pair.max.entries=200000
history.pair.ema.alpha=0.10
history.pair.min.samples=3
```

---

# 27. source.start.mode 怎么选择

## latest_data

```properties
source.start.mode=latest_data
```

第一次启动时：

```text
先得到 Doris safeWindowEnd
然后从最新安全窗口开始
```

适合：

```text
只关心从现在开始持续检测
```

缺点：Context 历史需要冷启动积累。

## fixed

```properties
source.start.mode=fixed
source.start.time=2026-08-15 00:00:00
```

适合：

```text
从一个历史时间补算
同时预热 Context/Pair history
```

注意：会产生历史告警。

---

# 28. 第一次从旧版本升级建议

旧版本可能已经出现：

```text
12:00 数据还没到
但 cursor 已经跑到 20:00
```

这意味着中间窗口被跳过。

因此建议不要把旧 cursor 直接当成新版本的真实进度。

推荐：

```text
停止旧任务
    ↓
选择一个确定安全的历史时间
    ↓
source.start.mode=fixed
    ↓
先只开日志 Sink
    ↓
提交新 Application
    ↓
观察追数
    ↓
追上 Doris safe watermark 后再开 Kafka
```

---

# 29. 编译前检查

```bash
java -version
mvn -version
```

建议：

```text
JDK 8 或 JDK 11
Maven 3.x
```

Flink 安装：

```text
/home/xgs/flink-1.17.2
```

---

# 30. 编译

进入目录：

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

---

# 31. 为什么 Flink 依赖是 provided

`pom.xml` 中 Flink runtime 依赖由集群自己的：

```text
/home/xgs/flink-1.17.2
```

提供。

业务 JAR 会打入：

```text
MySQL Connector/J
Kafka Connector
t-digest
Jackson
```

避免把整套 Flink runtime 重复塞进业务 JAR。

---

# 32. 提交 YARN

最简单：

```bash
chmod +x run-yarn.sh
./run-yarn.sh
```

脚本会执行：

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

# 33. 为什么 source.parallelism 必须是 1

当前版本：

```properties
source.parallelism=1
```

一个 Source 负责严格顺序：

```text
window1
window2
window3
```

如果直接改成 4：

```text
4 个 Source 都会执行相同 SQL
```

会产生重复检测和重复历史更新。

如果以后一个 5 分钟窗口处理时间超过 5 分钟，需要做的是：

```text
Doris 分片查询
 -> 每分片局部 Sketch
 -> Sketch Merge
 -> 单点历史状态提交
```

而不是直接改 `source.parallelism=4`。

---

# 34. 如何确认 Watermark 正常

看日志：

```text
Doris data watermark updated:
maxCollectTime=...
stableDelay=60m
safeWindowEnd=...
cursor=...
```

检查：

```text
safeWindowEnd
```

是否明显早于 Doris MAX 约 60 分钟。

---

# 35. 如何确认 5 分钟窗口正常

看：

```text
Start Doris five-minute query:
window=[11:30,11:35)
```

完成：

```text
Window finished:
rows=12345678
alerts=12
nextCursor=11:35
```

下一次应该：

```text
[11:35,11:40)
```

不会跳号。

---

# 36. 如何看历史状态是否在增长

每个窗口日志包含：

```text
pairHistoryEntries=...
contextHistoryBuckets=...
```

正常初期：

```text
pairHistoryEntries 持续增加
```

接近：

```properties
history.pair.max.entries=200000
```

后会稳定在上限附近。

Context Bucket 随日期和 Context 数增长，但旧日期会被淘汰。

---

# 37. baseline 仍然很小怎么办

先区分 Pair 是不是有历史。

如果某 Pair 第一次出现：

```text
sampleCount < 3
```

baseline 会退化为 Context P50。

网络中大量连接可能只有：

```text
几十~几百 Bytes
1~几个 Packet
```

所以 Context P50 很小并不一定是 bug。

真正判断是否异常还必须同时超过：

```text
Context P99.9
```

如果你希望 `baseline_bytes` 更偏向“重流量连接自己的正常值”，可以：

1. 增大 `rule.large.candidate.capacity`，让更多 heavy pair 被学习；
2. 保持 `history.pair.min.samples=3`；
3. 运行一段时间，让 Pair EMA 累积。

---

# 38. 如何判断 Candidate Capacity 是否太小

默认：

```properties
rule.large.candidate.capacity=20000
```

如果一个窗口有极大量重流量 Pair，可能某个重要 Pair 没进入前 2 万。

可以尝试：

```properties
rule.large.candidate.capacity=50000
```

代价：

```text
当前窗口堆内存增加
Pair 更新计算增加
```

但仍然不会保存全部行。

---

# 39. Pair max.entries 怎么调

默认：

```properties
history.pair.max.entries=200000
```

如果 TaskManager heap 很充足，而且希望更多 Pair 有自己的 EMA：

```properties
history.pair.max.entries=500000
```

不要一上来改成几百万。

先观察：

```text
TaskManager heap
GC
checkpoint duration
checkpoint size
```

再调。

---

# 40. stable delay 怎么调

建议观察至少一天。

记录：

```text
系统时间
Doris MAX(collectTime)
空窗口次数
```

如果 `rows=0` 重试很多：

```text
stable delay 太小
```

如果始终没有空窗口但业务觉得告警太晚：

```text
可以尝试从 60 降到 45 / 30
```

一次不要调太激进。

---

# 41. Kafka 开关

先只打日志：

```properties
alert.output.log.enabled=true
alert.output.kafka.enabled=false
```

稳定以后：

```properties
alert.output.log.enabled=true
alert.output.kafka.enabled=true
```

Kafka：

```properties
kafka.bootstrap.servers=localhost:9092
kafka.topic=anomaly_alert
```

---

# 42. 告警 JSON

Type 2：

```json
{
  "logId":"STATIC_NET_TRAFFIC_ALERT",
  "collectTime":"2026-08-17 11:33:00",
  "srcIp":"1.1.1.1",
  "dstIp":"2.2.2.2",
  "protocol":"TLS",
  "anomalyType":2,
  "anomalyDetail":{
    "current_bytes":10000000,
    "current_pkts":12000,
    "baseline_source":"PAIR_EMA",
    "baseline_bytes":200000,
    "baseline_pkts":250,
    "pair_sample_count":5,
    "pair_min_samples":3,
    "context_source":"HISTORICAL",
    "context_historical_days":5,
    "context_p50_bytes":100,
    "context_p50_pkts":1,
    "context_p90_bytes":80000,
    "context_p90_pkts":120,
    "context_bytes_quantile":0.999,
    "context_pkts_quantile":0.999,
    "context_high_quantile_bytes":1500000,
    "context_high_quantile_pkts":1800,
    "bytes_baseline_multiplier":4.0,
    "pkts_baseline_multiplier":4.0,
    "bytes_threshold":1500000.0,
    "pkts_threshold":1800.0,
    "extreme_multiplier":2.0,
    "extreme_bytes_threshold":3000000.0,
    "bytes_anomaly":true,
    "pkts_anomaly":true,
    "extreme_bytes":true,
    "pair_learning_mode":"SKIP_ANOMALOUS_MATURE"
  },
  "remark1":"",
  "remark2":"",
  "vendorCode":"V001"
}
```

Type 3：

```json
{
  "anomalyType":3,
  "anomalyDetail":{
    "topRank":2,
    "TopN":100,
    "conns":156856
  }
}
```

---

# 43. 重要代码文件怎么读

第一次只看以下顺序。

## 43.1 NetTrafficSentinel.java

作用：

```text
加载配置
创建 Flink Environment
开启 checkpoint
创建 Source
连接日志/Kafka Sink
```

## 43.2 DorisPollingAlertSource.java

这是最重要的运行控制类。

负责：

```text
查询 Doris MAX collectTime
计算 safe watermark
控制 nextWindowStart
执行 5 分钟 SQL
rows=0 重试
checkpoint 保存历史
```

## 43.3 FiveMinuteWindowAnalyzer.java

负责把一行 Doris Metric 同时送入：

```text
Large Traffic
Off-hours TopN
```

## 43.4 HistoricalBaselineStore.java

负责长期状态：

```text
Context 时间桶
Pair EMA
7天过期
20万容量淘汰
checkpoint snapshot/restore
```

## 43.5 LargeTrafficAccumulator.java

只属于当前一个 5 分钟窗口。

保存：

```text
Context t-digest
Candidate min-heap
```

窗口结束后对象即可释放。

## 43.6 LargeTrafficDetector.java

负责：

```text
读取历史 Context
读取 Pair EMA
选择 baseline
计算阈值
生成 type=2
生成允许学习的 PairSample
```

---

# 44. 内存里不会保存什么

不会保存：

```text
整个 ResultSet
5分钟全部 MetricRecord
全部 srcIp-dstIp HashMap
7天所有原始记录
```

JDBC 使用 row streaming：

```text
ResultSet.next()
 -> 处理一行
 -> 更新 Sketch/Heap
 -> 丢掉原始行
```

---

# 45. 内存里主要保存什么

长期：

```text
最多 20万 Pair EMA
7天 Context t-digest 日期桶
```

单窗口：

```text
最多 2万 Candidate MetricRecord
当前 Context Sketch
Space-Saving 2048 项
```

因此内存与配置上限相关，而不是与 5 分钟原始行数线性相关。

---

# 46. checkpoint 失败怎么办

先看日志是否：

```text
Checkpoint expired before completing
```

或者 HDFS 权限错误。

首先检查：

```properties
checkpoint.timeout.ms=600000
checkpoint.storage.path=hdfs:///...
```

再检查：

```text
checkpoint duration
checkpoint size
JobManager/TaskManager GC
HDFS 写入速度
```

如果 Pair 状态很大，可以：

```properties
history.pair.max.entries=100000
```

做对比。

---

# 47. Doris 查询超过 5 分钟怎么办

例如日志：

```text
elapsedMs=420000
```

表示一个 5 分钟窗口用了 7 分钟。

长期会追不上。

不要直接：

```properties
source.parallelism=4
```

下一阶段需要真正实现：

```text
按 hash(srcIp) 或 Doris tablet 分片
        ↓
多个 JDBC reader
        ↓
局部 Candidate/Sketch
        ↓
merge
        ↓
单次历史提交
```

---

# 48. 常见日志解释

## No restored cursor

```text
No restored cursor. Initial Doris window cursor=...
```

第一次启动/没有恢复 checkpoint，正常。

## Restored historical state

```text
Restored historical state: pairEntries=..., contextBuckets=...
```

说明历史基线从 checkpoint/savepoint 恢复。

## Empty window

```text
Cursor is NOT advanced
```

新版保护机制，正常但如果频繁发生需调 stable delay。

## Doris operation failed

网络/JDBC/SQL 临时失败。

cursor 不推进，会重试原窗口。

---

# 49. 配置密码安全

根目录：

```text
application.properties
```

是运行时外置文件，可以包含真实密码。

JAR 内：

```text
src/main/resources/application.properties
```

必须使用：

```properties
doris.password=CHANGE_ME_DORIS_PASSWORD
```

否则密码会打进 JAR。

建议服务器：

```bash
chmod 600 application.properties
```

---

# 50. 上线前检查清单

- [ ] Java 8/11
- [ ] Maven 正常
- [ ] Flink 1.17.2
- [ ] Doris FE 9030 网络可达
- [ ] Doris 查询账号权限正常
- [ ] `business.timezone=Asia/Shanghai`
- [ ] `window.size.minutes=5`
- [ ] `source.parallelism=1`
- [ ] `source.doris.stable.delay.minutes` 已按真实延迟配置
- [ ] `source.empty.window.advance=false`
- [ ] `checkpoint.storage.path` 已设置 HDFS
- [ ] 初次上线 Kafka 先关闭
- [ ] 一个 5 分钟窗口处理时间小于 5 分钟
- [ ] 观察 `pairHistoryEntries`
- [ ] 观察 `contextHistoryBuckets`
- [ ] 观察 checkpoint duration/size
- [ ] 观察 Watermark 是否持续前进

---

# 51. 推荐第一版生产配置

```properties
window.size.minutes=5

source.start.mode=fixed
# 改成你确认需要重新补算的起始时间
source.start.time=2026-08-15 00:00:00
source.watermark.poll.interval.seconds=60
source.watermark.lookback.days=3
source.doris.stable.delay.minutes=60
source.empty.window.advance=false
source.empty.window.retry.seconds=300
source.parallelism=1

rule.large.candidate.capacity=20000
rule.large.bytes.quantile=0.999
rule.large.pkts.quantile=0.999

history.context.retention.days=7
history.context.slot.minutes=5
history.context.min.days=2
history.context.tdigest.compression=100

history.pair.ttl.days=7
history.pair.max.entries=200000
history.pair.ema.alpha=0.10
history.pair.min.samples=3

alert.output.log.enabled=true
alert.output.kafka.enabled=false

checkpoint.enabled=true
checkpoint.interval.ms=300000
checkpoint.timeout.ms=600000
checkpoint.storage.path=hdfs:///flink-checkpoints/net-traffic-sentinel
```

追上以后再：

```properties
alert.output.kafka.enabled=true
```

---

# 52. 最后一句话理解新版

```text
Doris 的真实数据进度决定“什么时候查”
        +
7天 Context Sketch 决定“同类流量通常多大”
        +
有限 Pair EMA 决定“这个连接自己通常多大”
        +
5分钟 Space-Saving 决定“非工作时段谁最活跃”
        +
Flink checkpoint 保存 cursor 和历史基线
```

这就是新版 NetTrafficSentinel 的核心。

---

# 26. 从旧版升级到本版本时怎么做

这一节非常重要。

旧版状态主要是：

```text
nextWindowStart
```

本版本状态变成：

```text
nextWindowStart
+ Context t-digest 日期桶
+ Pair EMA 历史
```

而且状态名称和 Operator UID 使用了 V2 名称。因此，第一次切换到这个版本时，最简单、最不容易出错的方法是把它当作一个新的 YARN Application 启动。

## 26.1 推荐升级步骤

先停止旧任务。

然后在 Doris 中找一个你确认已经有完整数据的 5 分钟时间边界，例如：

```text
2026-08-15 20:00:00
```

修改：

```properties
source.start.mode=fixed
source.start.time=2026-08-15 20:00:00
```

重新提交 V2。

程序会：

```text
20:00~20:05
20:05~20:10
20:10~20:15
...
```

一直补到 Doris 的安全 Watermark，然后自动等待后续数据。

当 V2 已经稳定运行并产生自己的 checkpoint/savepoint 后，以后的 V2 重启再使用 V2 自己的状态恢复。

## 26.2 不想补历史怎么办

配置：

```properties
source.start.mode=latest_data
```

程序会先查询 Doris：

```sql
MAX(collectTime)
```

减去：

```properties
source.doris.stable.delay.minutes
```

得到安全 Watermark，然后从安全 Watermark 前一个完整 5 分钟窗口开始。

## 26.3 配置文件密码安全

项目根目录的：

```text
application.properties
```

是实际部署用的外置配置文件，当前包含你提供的 Doris 连接配置。

而：

```text
src/main/resources/application.properties
```

使用：

```properties
doris.password=CHANGE_ME_DORIS_PASSWORD
```

不要把带真实密码的外置配置上传到 GitHub、GitLab 等公共仓库。

Linux 上可以执行：

```bash
chmod 600 application.properties
```

让配置文件仅对当前账号可读写。

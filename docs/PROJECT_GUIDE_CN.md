# NetTrafficSentinel 项目详解（面向小白）

> 本文档旨在帮助初学者从零开始理解 NetTrafficSentinel 项目的整体架构、核心逻辑和运行方式。
> 如果你是第一次接触 Flink、Kafka 或网络异常检测，这份文档就是为你写的。

---

## 目录

1. [这个项目是干什么的？](#1-这个项目是干什么的)
2. [用到了哪些技术？（技术栈一览）](#2-用到了哪些技术技术栈一览)
3. [项目目录结构总览](#3-项目目录结构总览)
4. [核心概念速查表](#4-核心概念速查表)
5. [数据是怎么流动的？（完整数据管道）](#5-数据是怎么流动的完整数据管道)
6. [逐模块详解](#6-逐模块详解)
   - 6.1 [启动入口：NetTrafficSentinel.java](#61-启动入口nettrafficsentineljava)
   - 6.2 [配置加载：AppConfig.java](#62-配置加载appconfigjava)
   - 6.3 [Kafka 数据源：KafkaSourceFactory + MetricJsonParser](#63-kafka-数据源kafkasourcefactory--metricjsonparser)
   - 6.4 [事件时间与水位线：Watermark 机制](#64-事件时间与水位线watermark-机制)
   - 6.5 [核心分析引擎：KafkaWindowAlertProcessFunction](#65-核心分析引擎kafkawindowalertprocessfunction)
   - 6.6 [五分钟窗口分析器：FiveMinuteWindowAnalyzer](#66-五分钟窗口分析器fiveminutewindowanalyzer)
   - 6.7 [规则一·大流量检测：LargeTrafficDetector](#67-规则一大流量检测largetrafficdetector)
   - 6.8 [规则二·非工作时间检测：OffHoursDetector](#68-规则二非工作时间检测offhoursdetector)
   - 6.9 [历史基线存储：HistoricalBaselineStore](#69-历史基线存储historicalbaselinestore)
   - 6.10 [告警输出：AlertRecord + KafkaSink](#610-告警输出alertrecord--kafkasink)
7. [两种异常检测规则详解](#7-两种异常检测规则详解)
   - 7.1 [Type=2 大流量异常](#71-type2-大流量异常)
   - 7.2 [Type=3 非工作时间异常](#72-type3-非工作时间异常)
8. [配置文件完全指南](#8-配置文件完全指南)
9. [如何构建和运行](#9-如何构建和运行)
10. [Checkpoint 容错机制](#10-checkpoint-容错机制)
11. [常见问题 FAQ](#11-常见问题-faq)
12. [术语表](#12-术语表)

---

## 1. 这个项目是干什么的？

**一句话概括：** NetTrafficSentinel 是一个实时网络流量异常检测系统，它从 Kafka 消息队列中读取网络流量数据，用统计算法自动发现"流量异常大"或"非工作时间异常活跃"的网络连接，并输出告警。

### 生活化类比

想象你是一个小区的保安，需要监控所有进出小区的车辆：

- **正常情况**：每天进出 100 辆车，每辆车平均运载 1 吨货物
- **异常情况 1（大流量）**：突然有一辆车运了 100 吨货物，明显不正常 → **Type=2 告警**
- **异常情况 2（非工作时间）**：凌晨 3 点突然有大量车辆进出 → **Type=3 告警**

NetTrafficSentinel 就是网络世界的"智能保安"，7×24 小时自动监控。

### 输入和输出

```
输入：Kafka Topic "METRIC-ENDPOINT"（网络流量指标数据，JSON 格式）
  ↓ 经过实时分析处理
输出：Kafka Topic "anomaly_alert"（异常告警，JSON 格式）
```

---

## 2. 用到了哪些技术？（技术栈一览）

| 技术 | 版本 | 作用 | 小白理解 |
|------|------|------|----------|
| **Java** | 8 | 编程语言 | 项目的"母语" |
| **Apache Flink** | 1.17.2 | 流处理引擎 | 实时数据处理的"发动机"，保证数据不丢、不错 |
| **Apache Kafka** | (通过 Flink 连接) | 消息队列 | 数据的"传送带"，连接数据源和本系统 |
| **t-digest** | 3.3 | 分位数统计算法 | 用于计算"99.9% 的流量都在 X 以下" |
| **Jackson** | 2.14.2 | JSON 解析库 | 把 JSON 字符串变成 Java 对象 |
| **Maven** | - | 构建工具 | 管理依赖、编译打包的"管家" |
| **JUnit** | 4.13.2 | 单元测试框架 | 验证代码逻辑是否正确 |
| **Kerberos** | - | 认证协议 | Kafka 集群的安全"门卫" |

### 什么是 Flink？（给完全不了解的同学）

Apache Flink 是一个**流处理引擎**。你可以把它想象成一条永不停歇的"数据处理流水线"：

- 数据像水流一样源源不断地流进来
- Flink 保证每条数据都被处理（不丢数据）
- 如果机器挂了，Flink 能从上次中断的地方继续（容错）
- 支持按"事件发生的时间"而不是"数据到达的时间"来处理（事件时间语义）

### 什么是 Kafka？（给完全不了解的同学）

Apache Kafka 是一个**消息队列**。你可以把它想象成一个巨大的"公告板"：

- 生产者往上面贴消息（写数据）
- 消费者从上面读消息（读数据）
- 多个消费者可以独立读取同一份消息（广播）
- 消息会保留一段时间，不会读完就消失

---

## 3. 项目目录结构总览

```
NetTrafficSentinel/
├── pom.xml                          # Maven 构建配置（项目的"身份证"）
├── build.sh                         # Linux/Mac 构建脚本
├── application.properties           # 外部配置文件（部署时使用）
├── README.md                        # 原始项目说明文档
├── src/
│   ├── main/
│   │   ├── java/cn/ac/iie/
│   │   │   ├── topology/            # 【启动入口】
│   │   │   │   └── NetTrafficSentinel.java    # main() 所在，整个程序从这里开始
│   │   │   │
│   │   │   └── anomaly/             # 【核心业务逻辑】
│   │   │       ├── config/          # 配置相关
│   │   │       │   ├── AppConfig.java         # 配置加载器
│   │   │       │   └── WindowRange.java       # 时间窗口工具
│   │   │       │
│   │   │       ├── model/           # 数据模型
│   │   │       │   ├── MetricRecord.java      # 输入数据对象（网络指标）
│   │   │       │   └── AlertRecord.java       # 输出数据对象（告警）
│   │   │       │
│   │   │       ├── source/          # 数据源处理
│   │   │       │   ├── KafkaSourceFactory.java          # 创建 Kafka 消费者
│   │   │       │   ├── KafkaInputConnectivityCheck.java  # 启动前连接检查
│   │   │       │   ├── MetricJsonParser.java             # JSON 解析器
│   │   │       │   └── KafkaWindowAlertProcessFunction.java  # 核心！窗口分析主逻辑
│   │   │       │
│   │   │       ├── rule/            # 异常检测规则
│   │   │       │   ├── FiveMinuteWindowAnalyzer.java  # 窗口分析器（协调器）
│   │   │       │   ├── LargeTrafficAccumulator.java   # 大流量数据收集器
│   │   │       │   ├── LargeTrafficDetector.java      # 大流量检测器
│   │   │       │   ├── OffHoursAccumulator.java       # 非工作时间数据收集器
│   │   │       │   ├── OffHoursDetector.java          # 非工作时间检测器
│   │   │       │   └── WindowAnalysisResult.java      # 分析结果
│   │   │       │
│   │   │       ├── history/         # 历史基线存储
│   │   │       │   ├── HistoricalBaselineStore.java   # 长期记忆（内存状态）
│   │   │       │   ├── ContextKey.java                # 上下文键
│   │   │       │   ├── ContextStats.java              # 上下文统计值
│   │   │       │   └── TDigestSerde.java              # t-digest 序列化
│   │   │       │
│   │   │       ├── sketch/          # 概率数据结构
│   │   │       │   ├── CountMinSketch.java            # 频率估算器
│   │   │       │   └── WeightedSpaceSavingSketch.java  # 重 hitters 追踪器
│   │   │       │
│   │   │       ├── sink/            # 输出相关
│   │   │       │   ├── KafkaSinkFactory.java          # 创建 Kafka 生产者
│   │   │       │   ├── AlertJsonMapper.java           # 告警对象转 JSON
│   │   │       │   └── LocalLogSink.java              # 本地日志输出（测试用）
│   │   │       │
│   │   │       └── util/            # 工具类
│   │   │           ├── AlertLogIdGenerator.java       # 告警 ID 生成器
│   │   │           ├── ConnectionKey.java             # 连接对编码
│   │   │           └── TimeUtils.java                 # 时间工具
│   │   │
│   │   └── resources/
│   │       └── application.properties   # 内嵌默认配置
│   │
│   └── test/                        # 单元测试
│       └── java/cn/ac/iie/anomaly/
│           ├── config/WindowRangeTest.java
│           ├── history/HistoricalBaselineStoreTest.java
│           ├── rule/FiveMinuteWindowAnalyzerTest.java
│           ├── rule/LargeTrafficDetectorTest.java
│           ├── sketch/CountMinSketchTest.java
│           ├── sketch/WeightedSpaceSavingSketchTest.java
│           ├── source/MetricJsonDecoderTest.java
│           ├── util/AlertLogIdGeneratorTest.java
│           └── util/TimeUtilsTest.java
```

---

## 4. 核心概念速查表

在阅读代码之前，先了解这些核心概念：

| 概念 | 含义 | 类比 |
|------|------|------|
| **MetricRecord** | 一条网络流量指标记录 | 一条"车辆进出记录" |
| **AlertRecord** | 一条异常告警 | 一条"可疑行为报告" |
| **Pair** | 一对 srcIp+dstIp+protocol | 一条"固定的车辆运输路线" |
| **Context** | 协议+工作日/周末+时间段 | "同类型路线在同一时段的整体画像" |
| **EMA** | 指数移动平均（Exponential Moving Average） | 一种"越近越重视"的平均算法 |
| **t-digest** | 一种概率数据结构 | 用很少的内存估算"99.9% 的数据都在 X 以下" |
| **Watermark** | 水位线 | "我认为这个时间之前的数据都到齐了" |
| **Window** | 时间窗口 | 5 分钟一个分析批次 |
| **Checkpoint** | 检查点 | Flink 的"自动存档"功能 |
| **KeyedProcessFunction** | Flink 的有状态处理函数 | 带记忆的"数据处理器" |

### 什么是 EMA（指数移动平均）？

EMA 是一种加权平均算法，**越新的数据权重越大**：

```
新 EMA = 旧 EMA × (1 - α) + 新数据 × α
```

其中 α = 0.1（默认值），意味着：
- 新数据占 10% 权重
- 历史数据占 90% 权重
- 这样基线会缓慢适应正常变化，但不会被单次异常带偏

### 什么是 t-digest？

t-digest 是一种**近似分位数计算**的数据结构。想象你有 100 万条数据，想知道"99.9% 的数据都在多少以下"——精确排序太慢，t-digest 用很少的内存就能给出近似答案。

在本项目中，t-digest 用于计算：
- **P50**（中位数）：50% 的流量都在这个值以下
- **P90**：90% 的流量都在这个值以下
- **P99.9**：99.9% 的流量都在这个值以下（用于检测极端异常）

---

## 5. 数据是怎么流动的？（完整数据管道）

```
┌─────────────────────────────────────────────────────────────────┐
│                        Kafka 集群                                │
│  Topic: METRIC-ENDPOINT（网络流量指标数据）                        │
└───────────────────────────┬─────────────────────────────────────┘
                            │ JSON 字符串
                            ▼
┌─────────────────────────────────────────────────────────────────┐
│  Step 1: KafkaSourceFactory                                     │
│  创建 Kafka 消费者，连接集群，开始接收消息                           │
└───────────────────────────┬─────────────────────────────────────┘
                            │ 原始 JSON 字符串
                            ▼
┌─────────────────────────────────────────────────────────────────┐
│  Step 2: MetricJsonParser（JSON 解析器）                         │
│  - 验证 JSON 格式是否正确                                         │
│  - 提取 collectTime, srcIp, dstIp, protocol 等字段               │
│  - 丢弃格式错误的数据                                              │
│  - 将 JSON 转为 MetricRecord 对象                                │
└───────────────────────────┬─────────────────────────────────────┘
                            │ MetricRecord 对象
                            ▼
┌─────────────────────────────────────────────────────────────────┐
│  Step 3: Watermark（水位线分配）                                  │
│  - 以 collectTime 作为事件时间                                    │
│  - 允许最多 6 小时的乱序                                          │
│  - 告诉 Flink："我认为 X 时间之前的数据都到齐了"                    │
└───────────────────────────┬─────────────────────────────────────┘
                            │ MetricRecord + 事件时间
                            ▼
┌─────────────────────────────────────────────────────────────────┐
│  Step 4: KeyBy（按键分区）                                       │
│  - 所有数据都按 key=0 分区（保证全局有序）                          │
│  - 这样历史基线状态是全局共享的                                    │
└───────────────────────────┬─────────────────────────────────────┘
                            │ 全局有序的 MetricRecord 流
                            ▼
┌─────────────────────────────────────────────────────────────────┐
│  Step 5: KafkaWindowAlertProcessFunction（核心分析引擎）          │
│  ┌─────────────────────────────────────────────────────────┐    │
│  │  5 分钟事件时间窗口 [13:00, 13:05)                       │    │
│  │  ┌─────────────────────────────────────────────────┐    │    │
│  │  │  LargeTrafficAccumulator（大流量收集器）          │    │    │
│  │  │  - 为每个 Context 维护 t-digest 草图             │    │    │
│  │  │  - 用最小堆保留 Top-N 候选                       │    │    │
│  │  └─────────────────────────────────────────────────┘    │    │
│  │  ┌─────────────────────────────────────────────────┐    │    │
│  │  │  OffHoursAccumulator（非工作时间收集器）          │    │    │
│  │  │  - 用 WeightedSpaceSaving 追踪 IP 对连接数       │    │    │
│  │  └─────────────────────────────────────────────────┘    │    │
│  └─────────────────────────────────────────────────────────┘    │
│                            │                                    │
│                            ▼ （窗口关闭时）                      │
│  ┌─────────────────────────────────────────────────────────┐    │
│  │  FiveMinuteWindowAnalyzer.finish()                      │    │
│  │  ├─ LargeTrafficDetector.detect()  → Type=2 告警        │    │
│  │  ├─ OffHoursDetector.detect()      → Type=3 告警        │    │
│  │  └─ HistoricalBaselineStore.apply() → 更新历史基线       │    │
│  └─────────────────────────────────────────────────────────┘    │
└───────────────────────────┬─────────────────────────────────────┘
                            │ AlertRecord 对象
                            ▼
┌─────────────────────────────────────────────────────────────────┐
│  Step 6: AlertJsonMapper + KafkaSinkFactory                     │
│  - AlertRecord → JSON 字符串                                    │
│  - 发送到 Kafka Topic: anomaly_alert                             │
└───────────────────────────┬─────────────────────────────────────┘
                            │ JSON 字符串
                            ▼
┌─────────────────────────────────────────────────────────────────┐
│  下游系统消费告警（如 SIEM、告警平台、可视化大屏等）                 │
└─────────────────────────────────────────────────────────────────┘
```

---

## 6. 逐模块详解

### 6.1 启动入口：NetTrafficSentinel.java

**文件位置：** `src/main/java/cn/ac/iie/topology/NetTrafficSentinel.java`

这是整个程序的 `main()` 方法所在。它的职责是**组装整条数据处理流水线**。

**核心流程：**

```java
// 1. 加载配置
AppConfig config = AppConfig.load(args);

// 2. 启动前检查 Kafka 连接和权限
int discoveredPartitions = KafkaInputConnectivityCheck.verify(config);

// 3. 创建 Flink 执行环境
StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

// 4. 创建 Kafka 数据源
KafkaSource<String> kafkaSource = KafkaSourceFactory.create(config);

// 5. 组装处理流水线
rawJson → MetricJsonParser → Watermark → KeyBy(0) → KafkaWindowAlertProcessFunction → AlertJsonMapper → KafkaSink

// 6. 启动执行
env.execute("NET-TRAFFIC-ANOMALY");
```

**关键设计决策：**
- Source（数据源）并行度可以自动适配 Kafka 分区数
- 分析算子（anomaly analyzer）固定并行度 = 1，保证全局历史状态有序
- 配置校验在提交前完成，避免"跑起来才发现配置错误"

**配置优先级：**
```
命令行 --config 参数 > 当前目录的 application.properties > classpath 内嵌配置 > JVM -D 系统属性
```

### 6.2 配置加载：AppConfig.java

**文件位置：** `src/main/java/cn/ac/iie/anomaly/config/AppConfig.java`

负责从 properties 文件中读取配置项，支持多来源合并。

**使用方式：**
```java
// 读取字符串配置
String topic = config.get("kafka.input.topic", "METRIC-ENDPOINT");

// 读取整数配置
int windowMinutes = config.getInt("window.size.minutes", 5);

// 读取布尔配置
boolean enabled = config.getBoolean("rule.large.enabled", true);

// 读取浮点数配置
double alpha = config.getDouble("history.pair.ema.alpha", 0.10d);
```

每个 `get()` 调用都有一个默认值（第二个参数），当配置文件中没有该项时使用默认值。

### 6.3 Kafka 数据源：KafkaSourceFactory + MetricJsonParser

#### KafkaSourceFactory

**文件位置：** `src/main/java/cn/ac/iie/anomaly/source/KafkaSourceFactory.java`

负责创建 Flink 的 KafkaSource，配置包括：
- Broker 地址列表
- Topic 名称
- Group ID（消费者组标识）
- 安全认证（Kerberos SASL）
- 起始 offset 策略（latest/earliest/committed/timestamp）

#### MetricJsonParser

**文件位置：** `src/main/java/cn/ac/iie/anomaly/source/MetricJsonParser.java`

这是一个 `RichFlatMapFunction`，负责将原始 JSON 字符串转换为 `MetricRecord` 对象。

**容错规则（坏数据不会让程序崩溃）：**

| 情况 | 处理方式 |
|------|----------|
| JSON 格式错误 | 丢弃 |
| collectTime 缺失或无法解析 | 丢弃 |
| srcIp/dstIp 不是合法 IP | 丢弃 |
| protocol 缺失 | 按空字符串处理 |
| bytes/pkts/connCount 缺失或为 null | 按 0 处理 |
| bytes/pkts/connCount 为负数 | 按 0 处理 |

**监控指标：**
- `kafka-input-good-json-rows`：成功解析的记录数
- `kafka-input-bad-json-rows`：解析失败的记录数

### 6.4 事件时间与水位线：Watermark 机制

**这是理解整个系统时间语义的关键概念。**

#### 什么是事件时间（Event Time）？

事件时间是**数据实际产生的时间**，不是数据到达系统的时间。

```
数据产生时间（collectTime）：13:00:00
数据到达系统时间：13:05:30（可能因为网络延迟）
→ 系统按 13:00:00 来处理这条数据
```

#### 什么是水位线（Watermark）？

水位线是 Flink 的一个机制，表示"**我认为这个时间点之前的数据都已经到齐了**"。

```
当前水位线 = 13:05:00
→ 意味着系统认为 13:05:00 之前的数据都到了
→ 13:00:00 - 13:05:00 这个窗口可以关闭了
→ 窗口内的数据可以开始分析了
```

#### 乱序容忍

现实世界中数据可能延迟到达。本项目配置了：

```properties
kafka.input.max.out.of.orderness.minutes=360  # 允许最多 6 小时乱序
```

这意味着：
- 如果窗口是 [13:00, 13:05)，系统会等到 19:05 才关闭这个窗口
- 6 小时内到达的迟到数据仍然会被纳入分析
- 超过 6 小时的迟到数据会被丢弃（计入 `kafka-analysis-too-late-rows`）

#### 空闲检测

```properties
kafka.input.watermark.idle.timeout.minutes=2
```

如果某个 Kafka 分区 2 分钟没有新数据，会被标记为"空闲"，避免它拖住全局水位线。

### 6.5 核心分析引擎：KafkaWindowAlertProcessFunction

**文件位置：** `src/main/java/cn/ac/iie/anomaly/source/KafkaWindowAlertProcessFunction.java`

这是整个系统最核心的类。它是一个 Flink 的 `KeyedProcessFunction`，同时实现了 `CheckpointedFunction` 接口。

#### 核心职责

1. **管理多个 5 分钟窗口**：由于允许 6 小时乱序，可能同时有多个窗口处于"打开"状态
2. **接收数据并分配到对应窗口**：每条数据根据 collectTime 归属到对应的 5 分钟窗口
3. **窗口关闭时触发分析**：当水位线越过窗口结束时间，执行检测逻辑
4. **状态持久化**：通过 Checkpoint 保存所有内部状态

#### processElement() 详解

每收到一条 MetricRecord 时执行：

```java
public void processElement(MetricRecord record, Context ctx, Collector<AlertRecord> out) {
    // 1. 计算这条数据属于哪个 5 分钟窗口
    LocalDateTime windowStart = WindowRange.floorToWindow(collectLocalTime, windowMinutes);
    long windowEndMillis = window.getEnd().atZone(zoneId).toInstant().toEpochMilli();

    // 2. 如果窗口已经关闭（水位线已越过），丢弃这条"太晚"的数据
    if (windowEndMillis <= currentWatermark) {
        lateRows.add(1L);
        return;  // 丢弃
    }

    // 3. 找到或创建窗口的分析器
    WindowHolder holder = openWindowsByEndMillis.get(windowEndMillis);
    if (holder == null) {
        holder = new WindowHolder(window, new FiveMinuteWindowAnalyzer(...));
        openWindowsByEndMillis.put(windowEndMillis, holder);
        ctx.timerService().registerEventTimeTimer(windowEndMillis);  // 注册定时器
    }

    // 4. 将数据加入窗口分析器
    holder.analyzer.add(record, collectLocalTime);
}
```

#### onTimer() 详解

当水位线越过窗口结束时间时，Flink 自动触发：

```java
public void onTimer(long timestamp, OnTimerContext ctx, Collector<AlertRecord> out) {
    // 1. 取出窗口分析器
    WindowHolder holder = openWindowsByEndMillis.remove(timestamp);

    // 2. 执行分析，得到告警列表
    WindowAnalysisResult analysis = holder.analyzer.finish();

    // 3. 为每条告警分配 logId，然后输出
    for (AlertRecord alert : analysis.getAlerts()) {
        alert.assignLogId(alertLogIdGenerator.nextId());
        out.collect(alert);
    }

    // 4. 更新历史基线
    historyStore.apply(analysis.getHistoryUpdate(), holder.window.getEnd(), zoneId);
}
```

#### Checkpoint 机制

`snapshotState()` 和 `initializeState()` 实现了 Flink 的 CheckpointedFunction 接口：

- **snapshotState()**：将 Pair 历史、Context 历史、打开的窗口状态、logId 序列号保存到 Flink 状态后端
- **initializeState()**：从 Checkpoint 恢复所有状态

**保存的状态包括：**
- Pair EMA 历史（srcIp+dstIp+protocol 的指数移动平均）
- Context t-digest 历史（按天的分位数草图）
- 未关闭窗口的累积器快照
- logId 日内序列号

### 6.6 五分钟窗口分析器：FiveMinuteWindowAnalyzer

**文件位置：** `src/main/java/cn/ac/iie/anomaly/rule/FiveMinuteWindowAnalyzer.java`

这是一个**协调器**，负责在一个 5 分钟窗口内协调两个检测规则。

#### 生命周期

```
创建 → add() × N 条数据 → finish() → 输出告警 + 历史更新
```

#### add() 方法

每收到一条数据：

```java
public void add(MetricRecord record, LocalDateTime collectLocalTime) {
    rowCount++;

    // 规则一：大流量检测
    if (largeEnabled) {
        String contextKey = ContextKey.of(record.getProtocol(), collectLocalTime, contextSlotMinutes);
        ContextStats stats = contextStatsCache.get(contextKey);  // 缓存命中
        if (stats == null) {
            stats = historyStore.contextStats(contextKey, currentDate, bytesQuantile, pktsQuantile);
            contextStatsCache.put(contextKey, stats);
        }
        largeAccumulator.add(record, contextKey, stats);
    }

    // 规则二：非工作时间检测
    if (offHoursEnabled && TimeUtils.isOffHours(collectLocalTime, weekdayStartHour, weekdayEndHour)) {
        offHoursAccumulator.add(record);
        offHoursRowCount++;
    }
}
```

#### finish() 方法

窗口关闭时执行：

```java
public WindowAnalysisResult finish() {
    List<AlertRecord> alerts = new ArrayList<>();
    HistoricalBaselineStore.WindowUpdate historyUpdate = new HistoricalBaselineStore.WindowUpdate();

    // 规则一：大流量检测
    if (largeEnabled) {
        largeAccumulator.refreshHistoricalStats(...);
        LargeTrafficDetector.DetectionResult largeResult = largeDetector.detect(largeAccumulator, historyStore);
        alerts.addAll(largeResult.getAlerts());
        // ... 收集 Pair 学习样本和 Context 更新
    }

    // 规则二：非工作时间检测
    if (offHoursEnabled && offHoursRowCount > 0L) {
        alerts.addAll(offHoursDetector.detect(offHoursAccumulator));
    }

    return new WindowAnalysisResult(alerts, historyUpdate);
}
```

### 6.7 规则一·大流量检测：LargeTrafficDetector

**文件位置：** `src/main/java/cn/ac/iie/anomaly/rule/LargeTrafficDetector.java`

**这是整个系统最核心的检测逻辑。**

#### 检测流程（4 步）

```
Step 1: 选择基线
  ├── 成熟 Pair（样本数 ≥ 3）→ 使用 Pair EMA
  └── 不成熟 Pair → 使用 Context P50

Step 2: 计算阈值
  bytes_threshold = max(Context P99.9, baseline_bytes × 4.0)
  pkts_threshold  = max(Context P99.9, baseline_pkts × 4.0)
  extreme_bytes_threshold = Context P99.9 × 2.0

Step 3: 判定异常
  条件：current_bytes > bytes_threshold
        AND (current_pkts > pkts_threshold OR current_bytes > extreme_bytes_threshold)

Step 4: 决定是否学习
  ├── 正常 Pair → 学习真实值
  ├── 不成熟异常 Pair → 学习"被 Context 阈值封顶后的值"（冷启动修复）
  └── 成熟异常 Pair → 不学习（避免污染基线）
```

#### 为什么阈值不是简单的 baseline × 4？

```java
double bytesThreshold = Math.max(context.getThresholdBytes(), baselineBytes * bytesBaselineMultiplier);
```

如果某个 Pair 的 baseline 只有 100 字节，那 baseline × 4 = 400 字节，这个阈值太低了，正常的波动都会触发告警。

所以取 `max(Context P99.9, baseline × 4)`：
- 如果 Context P99.9 = 1MB，那阈值就是 1MB（更合理）
- 如果 baseline = 10MB，那阈值 = 40MB（baseline 的 4 倍）

#### 冷启动修复（Bootstrap Capped Learning）

**问题**：一个新 Pair 第一次出现就是异常流量，如果直接学习这个异常值，它永远无法达到"成熟"（需要 3 个样本）。

**解决方案**：允许不成熟的异常 Pair 学习，但学习值被限制在 Context 高分位阈值以内：

```java
// 例如：实际异常值 = 5MB，Context P99.9 = 1MB
// → EMA 只学习 1MB，而不是 5MB
long capBytes = learningCap(context.getThresholdBytes(), baselineBytes, currentBytes);
pairSamples.add(new PairSample(pairHash, Math.min(record.totalBytes(), cap.bytes), ...));
```

这样经过几次学习后，Pair 就能达到成熟状态，之后就可以用自己的 EMA 作为基线了。

### 6.8 规则二·非工作时间检测：OffHoursDetector

**文件位置：** `src/main/java/cn/ac/iie/anomaly/rule/OffHoursDetector.java`

#### 非工作时间定义

```properties
rule.offhours.weekday.start.hour=20   # 工作日晚上 8 点开始
rule.offhours.weekday.end.hour=8      # 到第二天早上 8 点
# 周末全天都算非工作时间
```

#### 检测逻辑

在非工作时间，使用 `WeightedSpaceSavingSketch` 追踪每个 IP 对的连接数，然后输出 Top-N（默认 100）连接数最多的 IP 对作为告警。

```java
public List<AlertRecord> detect(OffHoursAccumulator acc) {
    List<WeightedSpaceSavingSketch.Entry> top = acc.getSketch().topN(topN);
    int rank = 1;
    for (WeightedSpaceSavingSketch.Entry entry : top) {
        // 生成告警：rank=1 是连接数最多的
        alerts.add(AlertRecord.offHours(..., rank, topN, entry.getEstimate()));
        rank++;
    }
    return alerts;
}
```

### 6.9 历史基线存储：HistoricalBaselineStore

**文件位置：** `src/main/java/cn/ac/iie/anomaly/history/HistoricalBaselineStore.java`

这是系统的"长期记忆"，存储两种历史数据。

#### Context 历史（群体画像）

```
Key: protocol + WORKDAY/WEEKEND + time_slot
     例如: "UNKNOWN + WORKDAY + slot0"
Value: 按天存储的 t-digest 桶（bytes 和 pkts 分别存储）
保留: 最近 7 天
最少: 需要 2 天数据才"可用"
```

**用途**：计算 P50、P90、P99.9 分位数，作为检测阈值的参考。

#### Pair 历史（个体画像）

```
Key: FNV-1a 64-bit hash(srcIp|dstIp|protocol)
Value: EMA(bytes), EMA(pkts), sampleCount, lastSeenMillis
TTL: 7 天没更新自动过期
上限: 最多 200,000 条（LRU 淘汰）
EMA α: 0.10
成熟: 需要至少 3 个样本
```

**用途**：为每个具体的 IP 对维护自己的流量基线。

#### EMA 更新公式

```java
history.emaBytes = history.emaBytes * (1 - 0.10) + currentBytes * 0.10;
```

这意味着：
- 历史值占 90% 权重
- 新样本占 10% 权重
- 基线会缓慢适应正常变化
- 单次异常不会大幅影响基线

### 6.10 告警输出：AlertRecord + KafkaSink

#### AlertRecord 结构

**Type=2（大流量）告警 JSON：**
```json
{
  "logId": "2026082412345600000000000000000001",
  "collectTime": "2026-08-24 13:05:00",
  "srcIp": "1.224.99.3",
  "dstIp": "183.93.235.217",
  "protocol": "UNKNOWN",
  "anomalyType": 2,
  "anomalyDetail": {
    "current_bytes": 5242880,
    "current_pkts": 1000,
    "baseline_bytes": 102400,
    "baseline_pkts": 50,
    "bytes_threshold": 1048576,
    "pkts_threshold": 200,
    "extreme_bytes_threshold": 2097152,
    "anomaly_reason": "BYTES_AND_PKTS"
  },
  "vendorCode": "V001",
  "remark1": "",
  "remark2": ""
}
```

**Type=3（非工作时间）告警 JSON：**
```json
{
  "logId": "2026082412345600000000000000000002",
  "collectTime": "2026-08-24 03:15:00",
  "srcIp": "10.0.0.1",
  "dstIp": "10.0.0.2",
  "protocol": "TCP",
  "anomalyType": 3,
  "anomalyDetail": {
    "topRank": 1,
    "TopN": 100,
    "conns": 5000
  },
  "vendorCode": "V001",
  "remark1": "",
  "remark2": ""
}
```

#### logId 格式

```
yyyyMMdd + 6位设备ID + 18位日内序列号 = 32位唯一ID
例如: 20260824 + 123456 + 000000000000000001
```

---

## 7. 两种异常检测规则详解

### 7.1 Type=2 大流量异常

#### 检测目标

发现**单个 IP 对的流量突然远超正常水平**的情况。

#### 核心公式

```
阈值 = max(Context P99.9, Pair 基线 × 4.0)

触发条件：
  current_bytes > bytes_threshold
  AND (
    current_pkts > pkts_threshold          ← 字节和包数都异常
    OR current_bytes > extreme_bytes_threshold  ← 或者字节极端异常
  )
```

#### 图解

```
正常流量分布：
  P50  ──────── 100 KB（一半的流量在这个水平以下）
  P90  ──────── 500 KB
  P99.9 ─────── 1 MB（99.9% 的流量在这个水平以下）

某个 Pair 的 EMA 基线：
  ema_bytes = 200 KB

阈值计算：
  bytes_threshold = max(1MB, 200KB × 4) = max(1MB, 800KB) = 1MB

异常判定：
  如果 current_bytes = 2MB > 1MB → 触发告警
  如果 current_bytes = 500KB < 1MB → 不告警
```

#### 为什么需要两个条件？

```
条件1: bytes 异常 AND pkts 异常
  → 说明"数据量大，而且包数也多"，可能是 DDoS 攻击

条件2: bytes 极端异常（P99.9 × 2）
  → 说明"数据量极其大"，即使包数正常也告警
  → 可能是大量数据传输（数据泄露？）
```

### 7.2 Type=3 非工作时间异常

#### 检测目标

发现**非工作时间段内连接数异常多的 IP 对**。

#### 核心逻辑

1. 在非工作时间（工作日 20:00-08:00 + 整个周末），统计每个 IP 对的连接数
2. 使用 `WeightedSpaceSavingSketch`（一种概率数据结构）高效追踪 Top-N
3. 输出连接数最多的 Top-100 IP 对作为告警

#### 为什么用概率数据结构？

精确统计每个 IP 对的连接数需要大量内存（可能有几百万个 IP 对）。`WeightedSpaceSavingSketch` 用固定大小的内存（2048 个槽位）就能近似追踪 Top-N，虽然不是 100% 精确，但足够用于异常检测。

---

## 8. 配置文件完全指南

### 配置文件位置

| 文件 | 作用 | 优先级 |
|------|------|--------|
| 命令行 `--config` 参数 | 指定外部配置文件路径 | 最高 |
| 当前目录 `application.properties` | 部署时使用的配置 | 高 |
| classpath `application.properties` | 内嵌默认配置 | 低 |
| JVM `-D` 系统属性 | 单项覆盖 | 最低 |

### 关键配置项说明

#### Job 配置

```properties
# 作业名称（显示在 Flink Web UI 中）
job.name=NET-TRAFFIC-ANOMALY

# 业务时区（影响窗口对齐和非工作时间判断）
business.timezone=Asia/Shanghai

# 窗口大小（必须是 60 的正因子：1, 2, 3, 5, 6, 10, 15, 20, 30, 60）
window.size.minutes=5
```

#### Kafka 输入配置

```properties
# Topic 名称
kafka.input.topic=METRIC-ENDPOINT

# 消费者组 ID（必须唯一，不能和其他 Flink 作业共用）
kafka.input.group.id=net-traffic-sentinel-metric-endpoint-v1

# 起始位置：
#   latest   = 只消费启动后的新消息（默认，安全选择）
#   earliest = 从 Topic 保留的最早消息开始
#   committed = 从上次提交的 offset 开始
#   timestamp = 从指定时间开始
kafka.input.start.mode=latest

# 允许的最大乱序时间（影响窗口关闭时机）
kafka.input.max.out.of.orderness.minutes=360

# Source 并行度（0 = 自动适配 Kafka 分区数）
kafka.input.source.parallelism=0
kafka.input.source.parallelism.max=4
```

#### 异常检测规则配置

```properties
# 大流量检测
rule.large.enabled=true
rule.large.bytes.quantile=0.999        # 使用 P99.9 作为高分位
rule.large.bytes.baseline.multiplier=4.0  # 基线乘数
rule.large.extreme.multiplier=2.0      # 极端异常乘数
rule.large.candidate.capacity=20000    # 候选集容量

# 非工作时间检测
rule.offhours.enabled=true
rule.offhours.topn=100                 # 输出 Top-100
rule.offhours.weekday.start.hour=20    # 工作日非工作时间开始
rule.offhours.weekday.end.hour=8       # 工作日非工作时间结束
```

#### 历史基线配置

```properties
# Context 历史（群体画像）
history.context.retention.days=7       # 保留天数
history.context.min.days=2             # 最少天数才"可用"
history.context.slot.minutes=5         # 时间槽大小

# Pair 历史（个体画像）
history.pair.ttl.days=7                # TTL（过期时间）
history.pair.max.entries=200000        # 最大条目数
history.pair.ema.alpha=0.10            # EMA 学习率
history.pair.min.samples=3             # 成熟所需样本数
```

#### Checkpoint 配置

```properties
checkpoint.enabled=true
checkpoint.interval.ms=300000          # 5 分钟一次
checkpoint.timeout.ms=900000           # 超时 15 分钟
checkpoint.storage.path=hdfs:///flink-checkpoints/net-traffic-sentinel-kafka
restart.attempts=20                    # 最多重试 20 次
restart.delay.seconds=10               # 每次重试间隔 10 秒
```

---

## 9. 如何构建和运行

### 环境要求

- **Java**: 8 或 11
- **Maven**: 3.x
- **Flink**: 1.17.2（YARN 集群已安装）
- **Kafka**: 集群可用，且有 Kerberos 认证

### 构建

```bash
# Linux/Mac
./build.sh

# 或者直接用 Maven
mvn clean test package

# Windows
build.bat
```

构建产物：`target/NetTrafficSentinel-1.0-SNAPSHOT.jar`（Fat JAR，包含所有依赖）

### 运行（YARN 模式）

```bash
# 确保当前目录有 application.properties
./run-yarn.sh
```

### 启动日志检查

正常启动应看到：

```
KAFKA_INPUT_CONNECTION_OK topic=METRIC-ENDPOINT partitions=N ...
Creating Kafka input source
```

正常运行应看到：

```
KAFKA_WINDOW_OPEN window=[13:00, 13:05), openWindows=1
KAFKA_WINDOW_FINALIZED window=[13:00, 13:05), rows=1234, alerts=5
```

### 常见启动问题

| 问题 | 可能原因 | 解决方案 |
|------|----------|----------|
| `KAFKA_INPUT_CONNECTION_FAIL` | Kafka 连接失败 | 检查 broker 地址、Kerberos 票据 |
| 长时间只有 `KAFKA_WINDOW_OPEN` 没有 `KAFKA_WINDOW_FINALIZED` | 水位线没有前进 | 检查 collectTime 是否持续递增、乱序容忍时间是否过大 |
| `too-late` 记录很多 | 数据延迟超过容忍时间 | 增大 `kafka.input.max.out.of.orderness.minutes` |

---

## 10. Checkpoint 容错机制

### 什么是 Checkpoint？

Checkpoint 是 Flink 的**自动存档**功能。每隔一段时间（默认 5 分钟），Flink 会把所有算子的内部状态保存到 HDFS。

### 保存了什么？

```
Checkpoint 内容：
├── KafkaSource offsets（消费到哪个位置了）
├── 未关闭窗口的累积器快照（等待迟到数据的窗口）
├── Pair EMA 历史（每个 IP 对的基线）
├── Context t-digest 历史（群体画像）
├── logId 日内序列号
└── Flink event-time timers（定时器状态）
```

### 故障恢复

当 TaskManager 崩溃时：
1. Flink 自动重启作业
2. 从最近的 Checkpoint 恢复所有状态
3. KafkaSource 从 Checkpoint 保存的 offset 开始重新消费
4. 窗口分析器恢复到中断前的状态
5. 继续处理，不丢数据

### 重要提醒

- **不要用旧 Doris 版本的 Checkpoint 恢复新 Kafka 版本**（状态格式不兼容）
- **首次部署请使用新的 Checkpoint 路径**
- Checkpoint 路径：`hdfs:///flink-checkpoints/net-traffic-sentinel-kafka`

---

## 11. 常见问题 FAQ

### Q1: 为什么分析算子并行度是 1？

因为历史基线状态（Pair EMA 和 Context t-digest）需要全局有序更新。如果并行度 > 1，不同算子实例之间需要同步状态，增加复杂度和延迟。

Source 和解析阶段可以并行（自动适配 Kafka 分区数），但分析阶段保持单并行度。

### Q2: 为什么允许 6 小时的乱序？

因为不知道数据的最大延迟。6 小时是一个保守的默认值。代价是：
- 告警输出会延迟（最坏情况等 6 小时）
- 更多窗口同时打开，占用更多内存
- Checkpoint 体积更大

如果确定数据延迟不会超过 1 小时，可以减小这个值。

### Q3: 为什么用 EMA 而不是简单平均？

EMA（指数移动平均）的特点是**越新的数据权重越大**：
- 最近的数据占 10% 权重
- 历史数据占 90% 权重
- 基线会缓慢适应正常的变化趋势
- 单次异常不会大幅影响基线

简单平均要么对所有数据一视同仁（响应慢），要么只看最近 N 个（不稳定）。

### Q4: 为什么需要"冷启动修复"？

如果一个新 IP 对第一次出现就是异常流量：
- 没有历史基线 → 使用 Context P50 作为基线
- 检测到异常 → 正常情况下不会学习异常值
- 但如果不学习，这个 IP 对永远无法"成熟"（需要 3 个样本）
- 永远不成熟 → 永远使用 Context 基线 → 可能持续误报或漏报

冷启动修复允许学习"被 Context 阈值封顶后的值"，让 IP 对能够逐步成熟。

### Q5: 两个 Flink 作业读同一个 Topic 会互相抢数据吗？

不会，只要使用**不同的 group.id**。Kafka 的消费者组机制保证：
- 同一 group.id 内的消费者共享分区（互相分配）
- 不同 group.id 的消费者独立消费（各自完整读取）

### Q6: 为什么不把历史状态存到数据库？

设计选择是**全内存 + Checkpoint**：
- 优点：读写极快，无外部依赖，简化部署
- 缺点：受限于 TaskManager 内存大小
- 通过 Pair 最大条目数（200,000）和 Context 保留天数（7 天）控制内存使用

---

## 12. 术语表

| 术语 | 英文 | 含义 |
|------|------|------|
| 流处理 | Stream Processing | 实时处理连续到达的数据 |
| 事件时间 | Event Time | 数据实际发生的时间 |
| 处理时间 | Processing Time | 数据被系统处理的时间 |
| 水位线 | Watermark | "我认为这个时间之前的数据都到齐了" |
| 窗口 | Window | 一段时间内的数据集合 |
| 并行度 | Parallelism | 同时运行的算子实例数 |
| 算子 | Operator | Flink 处理流水线中的一个步骤 |
| Checkpoint | 检查点 | Flink 的自动存档机制 |
| 状态后端 | State Backend | Checkpoint 的存储方式（HDFS 等） |
| Consumer Group | 消费者组 | Kafka 中一组协同消费的消费者 |
| Offset | 偏移量 | 消费者在分区中读到的位置 |
| EMA | Exponential Moving Average | 指数移动平均 |
| t-digest | - | 一种近似分位数计算的概率数据结构 |
| P50/P90/P99.9 | - | 第 50/90/99.9 百分位数 |
| Heavy Hitter | - | 频率最高的元素 |
| Fat JAR | - | 包含所有依赖的 JAR 文件 |
| KeyedProcessFunction | - | Flink 的有状态处理函数 |
| RichFlatMapFunction | - | Flink 的无状态转换函数 |

---

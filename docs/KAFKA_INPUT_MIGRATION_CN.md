# Kafka 输入改造说明

## 改造范围

- 删除 Doris JDBC 轮询 Source；
- 删除 MySQL Connector 运行依赖；
- 新增 Flink 1.17.2 `KafkaSource<String>`；
- 输入 Topic 默认 `METRIC-ENDPOINT`；
- 输入/输出 Kafka Topic 配置分离；
- 新增 Kafka 启动连接、Topic、partition、READ/group 权限预检；
- JSON 转换为 `MetricRecord`；
- 使用 `collectTime` 作为 Flink event-time；
- 使用 bounded-out-of-orderness Watermark 处理数小时迟到；
- 保持 5 分钟完整窗口后统一调用 `FiveMinuteWindowAnalyzer.finish()`；
- Checkpoint 新增未闭合窗口摘要；
- 不迁移旧 Doris checkpoint 中的历史基线。

## 输入字段映射

| Kafka JSON | MetricRecord | 规则 |
|---|---|---|
| collectTime | collectTime / collectTimestamp | 必填，业务时间 |
| srcIp | srcIp | 必填 |
| dstIp | dstIp | 必填 |
| protocol | protocol | 缺失为空字符串 |
| connCount | connCount | null/缺失/负数 -> 0 |
| c2sPkts | c2sPkts | null/缺失/负数 -> 0 |
| s2cPkts | s2cPkts | null/缺失/负数 -> 0 |
| c2sBytes | c2sBytes | null/缺失/负数 -> 0 |
| s2cBytes | s2cBytes | null/缺失/负数 -> 0 |

不使用：`connBytesSeries`、`connTimeSeries`、`dayHourTime`、`dayTime`。

## 多 Flink 消费同一个 Topic

Kafka consumer group 是隔离边界。NetTrafficSentinel 必须有独立 group.id。

不同 group.id：两个 Flink 作业分别完整读取同一 Topic。

相同 group.id：两个作业共同瓜分 Topic partitions，会导致各自数据不完整。

## 延迟参数

默认：

```properties
kafka.input.max.out.of.orderness.minutes=360
```

因为目前真实延迟上限未知，先用 6 小时。上线后应观察 `kafka-analysis-too-late-rows` 和实际 `collectTime` 延迟，再决定是否改为 8/12/24 小时。

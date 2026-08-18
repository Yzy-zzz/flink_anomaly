# V2 修改说明

本版本针对 Doris 延迟入库和 type=2 baseline 语义完成以下修改：

1. Source 从“系统时间驱动”改为“Doris 数据 Watermark 驱动”。
2. Watermark = `floor5min(MAX(collectTime) - stableDelay)`。
3. 成熟窗口 `rows=0` 默认不推进 cursor，定时重试同一窗口。
4. 删除 type=2 的 Count-Min Sketch baseline 计算。
5. 新增 7 天按日期分桶的 Context t-digest History。
6. ContextKey = `protocol + WORKDAY/WEEKEND + time-slot`，不保存 IP Pair。
7. 新增 bounded Pair EMA History：64 位 Pair hash、事件时间 TTL、最大容量淘汰。
8. Pair 异常窗口采用分阶段学习：冷启动异常样本按 Context 高分位 cap 后学习，成熟 Pair 异常窗口不更新 EMA。
9. type=2 baseline 优先 Pair EMA，其次历史 Context P50，最后冷启动当前窗口 P50。
10. 长期历史更新与 alert emit、cursor advance 在完整窗口成功后一次提交。
11. checkpoint 新增 Context/Pair 历史 state。
12. checkpoint 默认周期调整为 5 分钟，并强烈建议使用 HDFS checkpoint storage。
13. Source/Map/Sink 增加稳定 UID，方便未来 savepoint 状态映射。
14. 删除旧 CountMinSketch 源码及测试，避免误导。
15. 更新 README 和小白部署文档。


## 2026-08-17 type=2 可观测性与 Pair 冷启动修复

1. type=2 `anomalyDetail` 增加 baseline 来源、Pair 样本数、Context 来源/P50/P90/高分位、实际 bytes/pkts 阈值、extreme 阈值和各判定布尔值。
2. 增加 `pair_learning_mode`，可直接看出当前异常 Pair 是 `CAPPED_BOOTSTRAP`、`SKIP_ANOMALOUS_MATURE` 还是禁用受限学习后的 `SKIP_ANOMALOUS`。
3. 新增 `history.pair.bootstrap.anomaly.capped.learning.enabled=true`。冷启动 Pair 触发 type=2 时，不再完全丢弃样本，而是将 bytes/pkts cap 到有效 Context 高分位后写入 Pair EMA。
4. Pair 达到 `history.pair.min.samples` 后，如果再次触发 type=2，仍不学习异常值，保留原有防基线污染策略。

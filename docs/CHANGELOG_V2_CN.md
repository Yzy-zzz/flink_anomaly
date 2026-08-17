# V2 修改说明

本版本针对 Doris 延迟入库和 type=2 baseline 语义完成以下修改：

1. Source 从“系统时间驱动”改为“Doris 数据 Watermark 驱动”。
2. Watermark = `floor5min(MAX(collectTime) - stableDelay)`。
3. 成熟窗口 `rows=0` 默认不推进 cursor，定时重试同一窗口。
4. 删除 type=2 的 Count-Min Sketch baseline 计算。
5. 新增 7 天按日期分桶的 Context t-digest History。
6. ContextKey = `protocol + WORKDAY/WEEKEND + time-slot`，不保存 IP Pair。
7. 新增 bounded Pair EMA History：64 位 Pair hash、事件时间 TTL、最大容量淘汰。
8. Pair 异常窗口不更新 EMA。
9. type=2 baseline 优先 Pair EMA，其次历史 Context P50，最后冷启动当前窗口 P50。
10. 长期历史更新与 alert emit、cursor advance 在完整窗口成功后一次提交。
11. checkpoint 新增 Context/Pair 历史 state。
12. checkpoint 默认周期调整为 5 分钟，并强烈建议使用 HDFS checkpoint storage。
13. Source/Map/Sink 增加稳定 UID，方便未来 savepoint 状态映射。
14. 删除旧 CountMinSketch 源码及测试，避免误导。
15. 更新 README 和小白部署文档。

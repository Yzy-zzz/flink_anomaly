# Doris 读取降压改造说明

本版本针对 `metric_endpoint` 约 700~800 万行 / 5 分钟的数据量，只修改 Flink 侧读取方式，不要求修改 Doris 表结构。

## 已改动

1. 业务窗口仍为 5 分钟，但默认按 60 秒切片查询；5 个切片共用同一个 `FiveMinuteWindowAnalyzer`，最后只 `finish()` 一次。
2. `dayTime`、`dayHourTime` 改为等值条件，`collectTime` 使用切片范围条件，匹配 `(dayTime, dayHourTime, collectTime)` 排序键前缀。
3. JDBC Session 默认执行：
   - `SET parallel_pipeline_task_num = 1`
   - `SET num_scanner_threads = 1`
4. 查询字段从 9 列降为 7 列，在 Doris SQL 中直接合并 `totalPkts` 和 `totalBytes`。
5. 相邻切片默认 sleep 1 秒。
6. 追历史时，完整 5 分钟窗口之间默认 sleep 5 秒；追到 safe watermark 后不额外等待。
7. 保留 MySQL JDBC streaming：`setFetchSize(Integer.MIN_VALUE)`。
8. 每个切片日志记录实际行数和耗时，便于上线后调参。

## 默认配置

```properties
source.doris.query.slice.seconds=60
source.doris.slice.pause.millis=1000
source.catchup.pause.seconds=5

doris.query.resource.limit.enabled=true
doris.query.parallelism=1
doris.query.scanner.threads=1
```

按当前数据量，60 秒切片平均约 140~160 万行/SQL。如果仍然影响共享 Doris，优先把 `source.doris.query.slice.seconds` 调成 `30`，即约 70~80 万行/SQL。不要使用 `LIMIT/OFFSET` 分页。

## 上线后重点观察

日志：

```text
Doris slice completed: ... rows=... elapsedMs=...
Doris window analyzed: ... slices=... rows=... elapsedMs=...
```

建议完整 5 分钟业务窗口总处理时间尽量控制在 2~3 分钟，最多尽量低于 4 分钟，以保留追赶余量。

## 构建

项目原构建方式不变：

```bash
./build.sh
# 或
mvn clean package
```

本次生成环境没有安装 Maven，因此未能在生成环境执行完整 Maven 构建；已用 JDK 对本次修改中无外部依赖的核心类进行了编译和 smoke check。

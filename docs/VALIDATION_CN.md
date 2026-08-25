# 工程校验说明

本次修正版专门针对“旧 Doris Source 残留导致 Maven 仍编译 `DorisPollingAlertSource.java`”的问题重新检查。

已完成：

- Kafka 版活跃源码目录中不存在 `DorisPollingAlertSource.java`；
- `WindowRange` 恢复 `toDorisFilter()` / `toDorisFilter(start,end)` 作为 deprecated 编译兼容层；
- `pom.xml` 的 `maven-compiler-plugin` 增加 `**/DorisPollingAlertSource.java` 排除规则，防止覆盖旧目录时残留 Source 参与 Kafka 版编译；
- `pom.xml` XML 解析检查；
- 根目录与 `src/main/resources/application.properties` 一致性检查；
- properties 重复键检查；
- 使用 `javac --release 8` 对全部 `src/main/java` 做 Java 8 编译级检查；
- 使用同一 Java 8 目标对全部 `src/test/java` 做编译级检查；
- 对 Flink 1.17 KafkaSource / Watermark / Checkpoint 以及 Kafka Consumer 关键方法签名做 API 对照检查；
- 运行项目现有 22 个逻辑测试场景的离线测试夹具，22/22 通过；
- 额外模拟“把 Kafka 版覆盖到原 Doris 工程”的场景：故意保留旧 `DorisPollingAlertSource.java`，全部 main/test Java 仍通过 Java 8 编译级检查；
- `build.sh`、`run-yarn.sh` 进行 shell 语法检查；
- 输入 Topic / 输出 Topic 使用 `kafka.input.topic` / `kafka.output.topic` 分离；
- 提供的 `METRIC-ENDPOINT` JSON 样例覆盖在 `MetricJsonDecoderTest` 中。

## 关于 Maven

当前分析容器本身没有 Maven，且容器网络无法解析 Maven/apt 仓库，所以无法在这里下载 Maven 与真实 Flink/Kafka 依赖并执行最终的 `mvn clean test package`。

因此交付前做的是：Java 8 全量源码编译级检查 + 关键外部 API 签名核对 + 22 个逻辑测试场景。

请在你的开发机上 **解压到全新目录** 后执行最终真实依赖构建：

```bash
mvn clean test package
```

成功产物：

```text
target/NetTrafficSentinel-1.0-SNAPSHOT.jar
```

如果构建日志仍出现 `DorisPollingAlertSource.java`，请先确认你是不是又在旧目录上覆盖解压；参见 `docs/CLEAN_BUILD_CN.md`。

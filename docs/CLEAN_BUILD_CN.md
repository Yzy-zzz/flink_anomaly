# 干净解压与构建说明

## 1. 这次报错的原因

如果 Maven 日志里仍然出现：

```text
src/main/java/cn/ac/iie/anomaly/source/DorisPollingAlertSource.java
```

说明当前正在编译的目录里还残留旧 Doris 版源码。

Kafka 输入版的正常源码目录中 **不存在** `DorisPollingAlertSource.java`。
上一版压缩包如果被直接覆盖解压到旧 `NetTrafficSentinel` 目录，ZIP 解压只会覆盖同名文件，不会自动删除旧文件，因此旧 Source 会继续被 Maven 扫描。

## 2. 推荐做法

不要覆盖旧工程。请把本压缩包解压到一个全新的目录，例如：

```text
D:\Code\B-traffic_behavior\NetTrafficSentinel-Kafka-v2
```

然后在新目录执行：

```bash
mvn clean test package
```

Windows 也可以直接执行：

```bat
build.bat
```

## 3. 本版本的双重兼容保护

为了防止再次发生“覆盖旧目录”问题，本版本增加了两层保护：

1. `WindowRange` 保留旧 `toDorisFilter(...)` 方法作为仅用于编译兼容的 deprecated 方法；Kafka 新拓扑不会调用它。
2. `pom.xml` 的 `maven-compiler-plugin` 明确排除 `**/DorisPollingAlertSource.java`。即使旧文件残留，它也不应该再被编译进 Kafka 版本。

因此建议仍然使用全新目录，但即使误覆盖旧目录，也比上一版更安全。

## 4. 构建前快速确认

Windows PowerShell：

```powershell
Get-ChildItem -Recurse src\main\java | Select-String "class DorisPollingAlertSource"
```

全新工程正常情况下不应找到该 Java 源文件。

Linux：

```bash
find src/main/java -name 'DorisPollingAlertSource.java'
```

正常情况下没有输出。

## 5. 构建产物

成功后：

```text
target/NetTrafficSentinel-1.0-SNAPSHOT.jar
```

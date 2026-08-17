#!/usr/bin/env bash
set -euo pipefail

FLINK_HOME="${FLINK_HOME:-/home/xgs/flink-1.17.2}"
JAR="${JAR:-$(pwd)/target/NetTrafficSentinel-1.0-SNAPSHOT.jar}"
CONFIG="${CONFIG:-$(pwd)/application.properties}"
APP_NAME="${APP_NAME:-NET-TRAFFIC-ANOMALY}"

if [[ ! -x "$FLINK_HOME/bin/flink" ]]; then
  echo "Flink command not found: $FLINK_HOME/bin/flink" >&2
  exit 1
fi
if [[ ! -f "$JAR" ]]; then
  echo "Jar not found: $JAR" >&2
  echo "Run: mvn clean package" >&2
  exit 1
fi
if [[ ! -f "$CONFIG" ]]; then
  echo "Config not found: $CONFIG" >&2
  exit 1
fi

CONFIG_DIR="$(cd "$(dirname "$CONFIG")" && pwd)"
CONFIG_ABS="$CONFIG_DIR/$(basename "$CONFIG")"
CONFIG_NAME="$(basename "$CONFIG")"

exec "$FLINK_HOME/bin/flink" run-application \
  -t yarn-application \
  -ys 1 \
  -p 1 \
  -Dlog4j.debug=true \
  -Denv.java.opts=-Duser.timezone=Asia/Shanghai \
  -Djobmanager.memory.process.size=8048m \
  -Dtaskmanager.memory.process.size=10240m \
  -Dtaskmanager.memory.managed.size=2048m \
  -Dyarn.application.name="$APP_NAME" \
  -Dyarn.ship-files="$CONFIG_ABS" \
  -c cn.ac.iie.topology.NetTrafficSentinel \
  "$JAR" \
  --config "$CONFIG_NAME"

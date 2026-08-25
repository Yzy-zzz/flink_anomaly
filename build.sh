#!/usr/bin/env bash
set -euo pipefail

if [[ -f "src/main/java/cn/ac/iie/anomaly/source/DorisPollingAlertSource.java" ]]; then
  echo "[WARN] Found legacy DorisPollingAlertSource.java in this directory." >&2
  echo "[WARN] pom.xml excludes it from the Kafka build, but using a fresh directory is recommended." >&2
fi

mvn clean test package
printf '\nBuild complete: target/NetTrafficSentinel-1.0-SNAPSHOT.jar\n'

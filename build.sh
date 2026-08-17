#!/usr/bin/env bash
set -euo pipefail
mvn clean package
printf '\nBuild complete: target/NetTrafficSentinel-1.0-SNAPSHOT.jar\n'

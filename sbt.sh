#!/usr/bin/env bash
set -euo pipefail
here="$(cd "$(dirname "$0")" && pwd)"
docker run --rm -u "$(id -u):$(id -g)" -e HOME=/cache/home -e SBT_OPTS="-Xmx2g" -e SPARK_VERSION="${SPARK_VERSION:-4.3.0}" -e JACKSON_ALIGN="${JACKSON_ALIGN:-up}" -e LOG4J_ALIGN="${LOG4J_ALIGN:-2.26.1}" \
  -v "$here/.cache:/cache" -v "$here/scala-job:/src" -w /src \
  sbtscala/scala-sbt:eclipse-temurin-17.0.21_9_1.13.1_2.13.18 \
  sbt -Dsbt.global.base=/cache/home/.sbt -Dsbt.ivy.home=/cache/home/.ivy2 -Dsbt.boot.directory=/cache/home/.sbt/boot "$@"

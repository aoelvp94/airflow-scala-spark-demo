#!/usr/bin/env bash
set -euo pipefail
here="$(cd "$(dirname "$0")" && pwd)"
cache="$here/.cache"
mkdir -p "$cache/home"
export SPARK_VERSION="${SPARK_VERSION:-4.3.0}"
export JACKSON_ALIGN="${JACKSON_ALIGN:-up}"
export LOG4J_ALIGN="${LOG4J_ALIGN:-2.26.1}"
export SBT_OPTS="${SBT_OPTS:--Xmx2g}"
sbt_args=(-Dsbt.global.base=/cache/home/.sbt -Dsbt.ivy.home=/cache/home/.ivy2 -Dsbt.boot.directory=/cache/home/.sbt/boot "$@")

if command -v docker >/dev/null 2>&1 && docker info >/dev/null 2>&1; then
  docker run --rm -u "$(id -u):$(id -g)" -e HOME=/cache/home -e SBT_OPTS="$SBT_OPTS" \
    -e SPARK_VERSION -e JACKSON_ALIGN -e LOG4J_ALIGN \
    -v "$cache:/cache" -v "$here/scala-job:/src" -w /src \
    sbtscala/scala-sbt:eclipse-temurin-17.0.21_9_1.13.1_2.13.18 \
    sbt "${sbt_args[@]}"
  exit 0
fi

if ! command -v java >/dev/null 2>&1; then
  for d in ${JAVA_HOME:-} /workspace/tools/jdk-21.0.13+9 /workspace/tools/jdk-17*; do
    if [ -n "${d:-}" ] && [ -x "$d/bin/java" ]; then
      export JAVA_HOME="$d"
      export PATH="$JAVA_HOME/bin:$PATH"
      break
    fi
  done
fi
if ! command -v sbt >/dev/null 2>&1; then
  for s in /workspace/tools/sbt/bin/sbt "$HOME/.local/share/sbt/bin/sbt"; do
    if [ -x "$s" ]; then
      export PATH="$(dirname "$s"):$PATH"
      break
    fi
  done
fi
if ! command -v java >/dev/null 2>&1 || ! command -v sbt >/dev/null 2>&1; then
  echo "sbt.sh: Docker is unavailable, and no local java+sbt was found. Install both or start Docker." >&2
  exit 1
fi

# Same cache layout as the Docker path, just without the /cache mount prefix.
sbt_args=(-Dsbt.global.base="$cache/home/.sbt" -Dsbt.ivy.home="$cache/home/.ivy2" -Dsbt.boot.directory="$cache/home/.sbt/boot" "$@")
cd "$here/scala-job"
exec sbt "${sbt_args[@]}"

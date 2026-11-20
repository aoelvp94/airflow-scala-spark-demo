#!/usr/bin/env bash
# Build the thin Scala handler bundles against published airflow-sdk 1.0.0:
# bundle-baseline (no Jackson Scala module) and bundle-baseline-scala (with
# jackson-module-scala 2.22.2, the way a Spark bundle ships it).
set -euo pipefail
source "$(dirname "$0")/common.sh"

build_one() {
  local with_scala="$1" out="$2"
  mkdir -p "$SBT_CACHE/home"
  docker run --rm --network=host -u "$(id -u):$(id -g)" \
    -e HOME=/cache/home -e SBT_OPTS=-Xmx2g -e WITH_JACKSON_SCALA="$with_scala" \
    -v "$SBT_CACHE:/cache" \
    -v "$DEMO_ROOT/scala-src:/src" -w /src \
    "$SBT_IMAGE" \
    sbt -Dsbt.global.base=/cache/home/.sbt -Dsbt.ivy.home=/cache/home/.ivy2 \
        -Dsbt.boot.directory=/cache/home/.sbt/boot -batch bundle

  local src="$DEMO_ROOT/scala-src/target/bundle"
  if [[ ! -f "$src/airflow-sdk-1.0.0.jar" ]]; then
    echo "sbt bundle did not produce airflow-sdk-1.0.0.jar" >&2
    ls -la "$src" >&2 || true
    exit 1
  fi
  rm -rf "$out"
  mkdir -p "$out"
  cp -a "$src/." "$out/"
  printf '*\n' > "$out/.airflowignore"
  echo "Wrote $out"
}

build_one 0 "$DEMO_ROOT/bundle-baseline"
build_one 1 "$DEMO_ROOT/bundle-baseline-scala"
ls "$DEMO_ROOT/bundle-baseline-scala" | grep -q '^jackson-module-scala_2.13-2.22.2.jar$' \
  || { echo "bundle-baseline-scala is missing jackson-module-scala" >&2; exit 1; }

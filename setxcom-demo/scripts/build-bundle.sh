#!/usr/bin/env bash
# Build the thin Scala handler bundle against published airflow-sdk 1.0.0.
set -euo pipefail
source "$(dirname "$0")/common.sh"

mkdir -p "$SBT_CACHE/home"
docker run --rm --network=host -u "$(id -u):$(id -g)" \
  -e HOME=/cache/home -e SBT_OPTS=-Xmx2g \
  -v "$SBT_CACHE:/cache" \
  -v "$DEMO_ROOT/scala-src:/src" -w /src \
  "$SBT_IMAGE" \
  sbt -Dsbt.global.base=/cache/home/.sbt -Dsbt.ivy.home=/cache/home/.ivy2 \
      -Dsbt.boot.directory=/cache/home/.sbt/boot -batch bundle

BUNDLE_SRC="$DEMO_ROOT/scala-src/target/bundle"
if [[ ! -f "$BUNDLE_SRC/airflow-sdk-1.0.0.jar" ]]; then
  echo "sbt bundle did not produce airflow-sdk-1.0.0.jar" >&2
  ls -la "$BUNDLE_SRC" >&2 || true
  exit 1
fi

rm -rf "$DEMO_ROOT/bundle-baseline"
mkdir -p "$DEMO_ROOT/bundle-baseline"
cp -a "$BUNDLE_SRC/." "$DEMO_ROOT/bundle-baseline/"
printf '*\n' > "$DEMO_ROOT/bundle-baseline/.airflowignore"
echo "Wrote $DEMO_ROOT/bundle-baseline"

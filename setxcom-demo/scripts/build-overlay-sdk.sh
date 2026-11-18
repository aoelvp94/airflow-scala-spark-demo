#!/usr/bin/env bash
# Overlay the setXCom loud-failure fix onto official airflow-sdk 1.0.0.
#
# Airflow 3.4.0 only knows supervisor schemas {2026-06-16, 2026-10-30} and
# rejects the 1.1.0-SNAPSHOT schema (2027-02-28) from current main. The
# published 1.0.0 jar already carries 2026-10-30, so this keeps that
# manifest and compiles just the fixed Kotlin sources against it.
#
# Requires: Docker (uses the same sbt/Temurin 17 image as the bundle), plus
# AIRFLOW_SRC pointing at an apache/airflow checkout with the fix applied
# (JsonValue.kt present). JsonValue.kt and Frame.kt are copied from that
# checkout (they match 1.0.0 + the fix). execution/Client.kt is taken from
# the 1.0.0 sources jar and only the setXCom / setTaskStateStore assignments
# are changed to jsonValue(value), because main's Client.kt is not drop-in
# compatible with 1.0.0 (getVariable signature).
set -euo pipefail
source "$(dirname "$0")/common.sh"

AIRFLOW_SRC="$(detect_airflow_src)" || {
  echo "Set AIRFLOW_SRC to an apache/airflow checkout with the setXCom fix applied." >&2
  exit 1
}
FIX_KT="$AIRFLOW_SRC/java-sdk/sdk/src/main/kotlin/org/apache/airflow/sdk/execution"
for f in JsonValue.kt Frame.kt; do
  [[ -f "$FIX_KT/$f" ]] || { echo "missing $FIX_KT/$f" >&2; exit 1; }
done

BUNDLE="${1:-$DEMO_ROOT/bundle-baseline}"
SDK_JAR="$BUNDLE/airflow-sdk-1.0.0.jar"
[[ -f "$SDK_JAR" ]] || { echo "run make bundle first (missing $SDK_JAR)" >&2; exit 1; }

WORK="$DEMO_ROOT/build/overlay"
OUT_JAR="${OUT_JAR:-$DEMO_ROOT/build/airflow-sdk-1.0.0-setxcom.jar}"
CACHE="$DEMO_ROOT/.cache/maven"
mkdir -p "$WORK/src" "$WORK/classes" "$CACHE"

SOURCES_JAR="$CACHE/airflow-sdk-1.0.0-sources.jar"
if [[ ! -f "$SOURCES_JAR" ]]; then
  echo "Downloading airflow-sdk 1.0.0 sources"
  curl -fsSL -o "$SOURCES_JAR" \
    https://repo1.maven.org/maven2/org/apache/airflow/airflow-sdk/1.0.0/airflow-sdk-1.0.0-sources.jar
fi

# 1.0.0 execution/Client.kt, then jsonValue() only on setXCom / setTaskStateStore.
python3 - << PY
import zipfile
from pathlib import Path
work = Path("$WORK/src")
with zipfile.ZipFile("$SOURCES_JAR") as z:
    data = z.read("org/apache/airflow/sdk/execution/Client.kt")
(work / "Client.kt").write_bytes(data)
PY
python3 "$DEMO_ROOT/scripts/patch_client.py" "$WORK/src/Client.kt"
cp "$FIX_KT/JsonValue.kt" "$WORK/src/JsonValue.kt"
cp "$FIX_KT/Frame.kt" "$WORK/src/Frame.kt"

# Compile inside the sbt image so reviewers only need Docker + the official jars
# already copied into the thin bundle (jackson, kotlin-stdlib, coroutines, …).
CP_DIR="$WORK/cp"
rm -rf "$CP_DIR"
mkdir -p "$CP_DIR"
# The official 1.0.0 jar plus every other runtime jar from the thin bundle.
while IFS= read -r -d '' jar; do
  cp "$jar" "$CP_DIR/"
done < <(find "$BUNDLE" -maxdepth 1 -name '*.jar' -print0)

KOTLINC_VERSION="${KOTLINC_VERSION:-2.4.20}"
KOTLINC_ZIP="$CACHE/kotlin-compiler-${KOTLINC_VERSION}.zip"
KOTLINC_HOME="$DEMO_ROOT/.cache/kotlinc"
ensure_kotlinc() {
  if [[ -x "$KOTLINC_HOME/kotlinc/bin/kotlinc" ]]; then
    return
  fi
  if curl -fsSL -o "$KOTLINC_ZIP" \
      "https://github.com/JetBrains/kotlin/releases/download/v${KOTLINC_VERSION}/kotlin-compiler-${KOTLINC_VERSION}.zip"; then
    rm -rf "$KOTLINC_HOME"
    mkdir -p "$KOTLINC_HOME"
    unzip -q -o "$KOTLINC_ZIP" -d "$KOTLINC_HOME"
    return
  fi
  echo "GitHub kotlinc zip unavailable; trying a local Gradle cache or Maven embeddable"
  local cached
  cached="$(find "${GRADLE_USER_HOME:-$HOME/.gradle}" /workspace/tools/gradle-user-home \
    -name "kotlin-compiler-embeddable-${KOTLINC_VERSION}.jar" 2>/dev/null | head -n1 || true)"
  if [[ -z "$cached" ]]; then
    echo "Could not find kotlinc $KOTLINC_VERSION. Download the compiler zip and set KOTLINC_HOME." >&2
    exit 1
  fi
  local dir
  dir="$(dirname "$cached")"
  # Expect sibling jars in the Gradle cache layout; if missing, fail clearly.
  mkdir -p "$KOTLINC_HOME/kotlinc/bin" "$KOTLINC_HOME/kotlinc/lib"
  # Copy well-known compiler jars if present next to the embeddable in caches
  find "${GRADLE_USER_HOME:-$HOME/.gradle}" /workspace/tools/gradle-user-home -name "kotlin-*-${KOTLINC_VERSION}.jar" \
    -path "*modules-2/files-2.1/org.jetbrains.kotlin/*" 2>/dev/null | while read -r j; do
      cp -n "$j" "$KOTLINC_HOME/kotlinc/lib/" 2>/dev/null || cp "$j" "$KOTLINC_HOME/kotlinc/lib/"
    done
  cat > "$KOTLINC_HOME/kotlinc/bin/kotlinc" << 'LAUNCH'
#!/usr/bin/env bash
set -euo pipefail
ROOT="$(cd "$(dirname "$0")/.." && pwd)"
# Prefer the full lib distribution; otherwise the embeddable + daemon + stdlib.
CP="$(find "$ROOT/lib" -name "*.jar" | paste -sd:)"
exec java -cp "$CP" org.jetbrains.kotlin.cli.jvm.K2JVMCompiler "$@"
LAUNCH
  chmod +x "$KOTLINC_HOME/kotlinc/bin/kotlinc"
}
ensure_kotlinc

rm -rf "$WORK/classes"
mkdir -p "$WORK/classes"

# Run kotlinc with the image JDK (17). Mount the demo so paths match.
docker run --rm --network=host -u "$(id -u):$(id -g)" \
  -v "$DEMO_ROOT:/demo" -w /demo \
  "$SBT_IMAGE" \
  bash -lc "
    set -euo pipefail
    CP=\$(find /demo/build/overlay/cp -name '*.jar' | paste -sd:)
    /demo/.cache/kotlinc/kotlinc/bin/kotlinc \
      -jvm-target 11 \
      -module-name org.apache.airflow_sdk \
      -Xfriend-paths=/demo/build/overlay/cp/airflow-sdk-1.0.0.jar \
      -cp \"\$CP\" \
      -d /demo/build/overlay/classes \
      /demo/build/overlay/src/JsonValue.kt \
      /demo/build/overlay/src/Frame.kt \
      /demo/build/overlay/src/Client.kt
  "

# Overlay only org/ classes; keep the official 1.0.0 manifest (schema 2026-10-30)
# and the official kotlin_module file.
cp "$SDK_JAR" "$OUT_JAR"
( cd "$WORK/classes" && jar uf "$OUT_JAR" org )

# Confirm schema is unchanged and the new class is present.
python3 - << PY
import zipfile
from pathlib import Path
jar = Path("$OUT_JAR")
with zipfile.ZipFile(jar) as z:
    manifest = z.read("META-INF/MANIFEST.MF").decode()
    names = z.namelist()
assert "Airflow-Supervisor-Schema-Version: 2026-10-30" in manifest, manifest
assert "org/apache/airflow/sdk/execution/JsonValueKt.class" in names
print("Wrote $OUT_JAR")
print("Manifest still has Airflow-Supervisor-Schema-Version: 2026-10-30")
PY

# Assemble the patched bundle from the baseline, swapping only the SDK jar.
rm -rf "$DEMO_ROOT/bundle-patched"
mkdir -p "$DEMO_ROOT/bundle-patched"
cp -a "$DEMO_ROOT/bundle-baseline/." "$DEMO_ROOT/bundle-patched/"
cp "$OUT_JAR" "$DEMO_ROOT/bundle-patched/airflow-sdk-1.0.0.jar"
printf '*\n' > "$DEMO_ROOT/bundle-patched/.airflowignore"
echo "Wrote $DEMO_ROOT/bundle-patched (airflow-sdk-1.0.0.jar replaced with the overlay)"

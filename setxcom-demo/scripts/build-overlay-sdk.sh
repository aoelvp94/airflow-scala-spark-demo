#!/usr/bin/env bash
# Overlay the setXCom encoding fix onto official airflow-sdk 1.0.0.
#
# Airflow 3.4.0 only knows supervisor schemas {2026-06-16, 2026-10-30} and
# rejects the 1.1.0-SNAPSHOT schema (2027-02-28) from current main. The
# published 1.0.0 jar already carries 2026-10-30, so this keeps that
# manifest and compiles only the fixed execution/Frame.kt against it.
#
# Requires: Docker (uses the same sbt/Temurin 17 image as the bundle), plus
# AIRFLOW_SRC pointing at an apache/airflow checkout with the fix applied.
# The fix lives in execution/Frame.kt and internal/JacksonModules.kt; their
# callers in 1.0.0 are unchanged.
set -euo pipefail
source "$(dirname "$0")/common.sh"

AIRFLOW_SRC="$(detect_airflow_src)" || {
  echo "Set AIRFLOW_SRC to an apache/airflow checkout with the setXCom fix applied." >&2
  exit 1
}
FIX_KT="$AIRFLOW_SRC/java-sdk/sdk/src/main/kotlin/org/apache/airflow/sdk/execution"
INTERNAL_KT="$AIRFLOW_SRC/java-sdk/sdk/src/main/kotlin/org/apache/airflow/sdk/internal"
[[ -f "$FIX_KT/Frame.kt" ]] || { echo "missing $FIX_KT/Frame.kt" >&2; exit 1; }
[[ -f "$INTERNAL_KT/JacksonModules.kt" ]] || { echo "missing $INTERNAL_KT/JacksonModules.kt" >&2; exit 1; }
grep -q 'RejectEmptyBeans' "$FIX_KT/Frame.kt" || {
  echo "$FIX_KT/Frame.kt does not contain the setXCom encoding fix" >&2
  exit 1
}

BUNDLE="${1:-$DEMO_ROOT/bundle-baseline}"
SDK_JAR="$BUNDLE/airflow-sdk-1.0.0.jar"
[[ -f "$SDK_JAR" ]] || { echo "run make bundle first (missing $SDK_JAR)" >&2; exit 1; }

WORK="$DEMO_ROOT/build/overlay"
OUT_JAR="${OUT_JAR:-$DEMO_ROOT/build/airflow-sdk-1.0.0-setxcom.jar}"
CACHE="$DEMO_ROOT/.cache/maven"
mkdir -p "$WORK/src" "$WORK/classes" "$CACHE"

cp "$FIX_KT/Frame.kt" "$WORK/src/Frame.kt"
cp "$INTERNAL_KT/JacksonModules.kt" "$WORK/src/JacksonModules.kt"

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
      /demo/build/overlay/src/Frame.kt \
      /demo/build/overlay/src/JacksonModules.kt
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
assert "org/apache/airflow/sdk/execution/RejectEmptyBeans.class" in names, "overlay is missing RejectEmptyBeans"
assert "org/apache/airflow/sdk/internal/JacksonModulesKt.class" in names, "overlay is missing JacksonModulesKt"
print("Wrote $OUT_JAR")
print("Manifest still has Airflow-Supervisor-Schema-Version: 2026-10-30")
PY

# Assemble the patched bundles from the baselines, swapping only the SDK jar.
for variant in "" "-scala"; do
  src="$DEMO_ROOT/bundle-baseline$variant"
  dst="$DEMO_ROOT/bundle-patched$variant"
  [[ -d "$src" ]] || { echo "missing $src; run make bundle" >&2; exit 1; }
  rm -rf "$dst"
  mkdir -p "$dst"
  cp -a "$src/." "$dst/"
  cp "$OUT_JAR" "$dst/airflow-sdk-1.0.0.jar"
  printf '*\n' > "$dst/.airflowignore"
  echo "Wrote $dst (airflow-sdk-1.0.0.jar replaced with the overlay)"
done

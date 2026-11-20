# shellcheck shell=bash
# Shared paths and defaults for the reviewer demo. Source from other scripts.

DEMO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
IMAGE="${IMAGE:-airflow-scala-docs-e2e:3.4.0}"
SBT_IMAGE="${SBT_IMAGE:-sbtscala/scala-sbt:eclipse-temurin-17.0.21_9_1.13.1_2.13.18}"
SBT_CACHE="${SBT_CACHE:-$DEMO_ROOT/.cache}"
DAG_ID="${DAG_ID:-setxcom_fix}"
CONTAINER_PREFIX="${CONTAINER_PREFIX:-airflow-reviewer-demo}"

detect_airflow_src() {
  if [[ -n "${AIRFLOW_SRC:-}" ]]; then
    printf '%s\n' "$AIRFLOW_SRC"
    return
  fi
  local c
  for c in \
    /workspace/airflow-scala/airflow-verify \
    /workspace/airflow-scala/airflow-setxcom \
    "$DEMO_ROOT/../airflow-verify" \
    "$DEMO_ROOT/../airflow-setxcom"
  do
    if grep -qs 'RejectEmptyBeans' "$c/java-sdk/sdk/src/main/kotlin/org/apache/airflow/sdk/execution/Frame.kt"; then
      printf '%s\n' "$c"
      return
    fi
  done
  return 1
}

#!/usr/bin/env bash
# Start Airflow 3.4.0 standalone, trigger setxcom_fix, collect evidence.
# Usage: run-airflow.sh baseline|patched
set -euo pipefail
source "$(dirname "$0")/common.sh"

MODE="${1:?usage: run-airflow.sh baseline|patched}"
case "$MODE" in
  baseline) BUNDLE="$DEMO_ROOT/bundle-baseline" ;;
  patched)  BUNDLE="$DEMO_ROOT/bundle-patched" ;;
  *) echo "mode must be baseline or patched" >&2; exit 1 ;;
esac
[[ -f "$BUNDLE/airflow-sdk-1.0.0.jar" ]] || { echo "missing $BUNDLE; run make bundle / make overlay" >&2; exit 1; }

NAME="${CONTAINER_PREFIX}-${MODE}"
LOGDIR="$DEMO_ROOT/evidence/logs-${MODE}"
EVID="$DEMO_ROOT/evidence"
mkdir -p "$LOGDIR" "$EVID"

# Standalone binds 8080 inside the container; we do not publish it to the host.
docker rm -f "$NAME" >/dev/null 2>&1 || true
rm -rf "$LOGDIR"
mkdir -p "$LOGDIR"
# The image runs as uid 50000 (airflow). A host-mounted log dir owned by
# the demo user (often 1000) must be world-writable or the dag-processor
# cannot create dag_processor/ and task log subdirectories.
chmod 777 "$LOGDIR"

docker run -d --name "$NAME" \
  -e AIRFLOW__CORE__EXECUTOR=LocalExecutor \
  -e AIRFLOW__CORE__LOAD_EXAMPLES=False \
  -e AIRFLOW__CORE__SIMPLE_AUTH_MANAGER_ALL_ADMINS=True \
  -e AIRFLOW__CORE__DAGS_FOLDER=/opt/airflow/dags \
  -e AIRFLOW__LOGGING__BASE_LOG_FOLDER=/opt/airflow/logs \
  -e 'AIRFLOW__SDK__COORDINATORS={"java":{"classpath":"airflow.sdk.coordinators.java.JavaCoordinator","kwargs":{"task_handler_bundle_name":"java-handlers","main_class":"com.mycompany.airflow.hello.Main"}}}' \
  -e 'AIRFLOW__SDK__QUEUE_TO_COORDINATOR={"java":"java"}' \
  -e 'AIRFLOW__DAG_PROCESSOR__DAG_BUNDLE_CONFIG_LIST=[{"name":"dags-folder","classpath":"airflow.dag_processing.bundles.local.LocalDagBundle","kwargs":{}},{"name":"java-handlers","classpath":"airflow.dag_processing.bundles.local.LocalDagBundle","kwargs":{"path":"/opt/airflow/java-handlers","refresh_interval":0}}]' \
  -v "$DEMO_ROOT/dags:/opt/airflow/dags:ro" \
  -v "$BUNDLE:/opt/airflow/java-handlers:ro" \
  -v "$LOGDIR:/opt/airflow/logs" \
  "$IMAGE" \
  standalone >/dev/null

echo "Started $NAME, waiting for DAG $DAG_ID ..."
ready=0
for _ in $(seq 1 90); do
  if docker exec "$NAME" airflow dags list 2>/dev/null | grep -q "$DAG_ID"; then
    ready=1
    break
  fi
  sleep 2
done
if [[ "$ready" -ne 1 ]]; then
  echo "Timed out waiting for $DAG_ID. Last logs:" >&2
  docker logs --tail 80 "$NAME" >&2 || true
  exit 1
fi

RUN_ID="reviewer_demo_${MODE}_$(TZ=Asia/Tbilisi date +%Y%m%dT%H%M%S)"
echo "$RUN_ID" > "$EVID/${MODE}-run-id.txt"
docker exec "$NAME" airflow dags unpause "$DAG_ID" >/dev/null
docker exec "$NAME" airflow dags trigger "$DAG_ID" --run-id "$RUN_ID" >/dev/null
echo "Triggered $RUN_ID, waiting for completion ..."

done_state=""
for _ in $(seq 1 60); do
  if docker exec "$NAME" airflow dags list-runs "$DAG_ID" -o json --limit 5 \
      > "$EVID/${MODE}-runs.json" 2>/dev/null; then
    done_state="$(python3 "$DEMO_ROOT/scripts/collect.py" --read-run-state "$RUN_ID" "$EVID/${MODE}-runs.json")"
  fi
  if [[ "$done_state" == "success" || "$done_state" == "failed" ]]; then
    break
  fi
  sleep 2
done

if [[ "$done_state" != "success" && "$done_state" != "failed" ]]; then
  echo "Timed out waiting for run $RUN_ID (last state: '${done_state}')" >&2
  docker logs --tail 80 "$NAME" >&2 || true
  exit 1
fi

docker exec "$NAME" airflow tasks states-for-dag-run "$DAG_ID" "$RUN_ID" -o json \
  > "$EVID/${MODE}-states.json"
docker cp "$NAME:/opt/airflow/airflow.db" "$EVID/${MODE}-airflow.db" >/dev/null
python3 "$DEMO_ROOT/scripts/collect.py" --mode "$MODE" --run-id "$RUN_ID" --demo-root "$DEMO_ROOT"

docker stop "$NAME" >/dev/null
echo "Run $RUN_ID finished with DAG state=$done_state. Evidence in $EVID"

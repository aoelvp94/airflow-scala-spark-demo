import json
import os
import subprocess
import sys
import time
import urllib.parse
import urllib.request

SCHEDULER = os.environ.get("AF_SCHEDULER", "airflow-scala-spark_3f9515-scheduler-1")
BASE = os.environ.get("AF_URL", "http://localhost:58280/api/v2")


def call(method: str, path: str, body: dict | None = None) -> dict:
    data = json.dumps(body).encode() if body is not None else None
    req = urllib.request.Request(BASE + path, data=data, method=method, headers={"Content-Type": "application/json"})
    with urllib.request.urlopen(req, timeout=60) as r:
        raw = r.read()
        return json.loads(raw) if raw and r.headers.get_content_type() == "application/json" else {"text": raw.decode()}


def unpause(dag_id: str) -> None:
    call("PATCH", f"/dags/{dag_id}", {"is_paused": False})


def trigger(dag_id: str, run_id: str) -> str:
    call("POST", f"/dags/{dag_id}/dagRuns", {"dag_run_id": run_id, "logical_date": None, "conf": {}})
    return run_id


def wait(dag_id: str, run_id: str, timeout: float = 900) -> str:
    end = time.time() + timeout
    while time.time() < end:
        state = call("GET", f"/dags/{dag_id}/dagRuns/{run_id}")["state"]
        if state in ("success", "failed"):
            return state
        time.sleep(1)
    return "timeout"


def task_instances(dag_id: str, run_id: str) -> list[dict]:
    return call("GET", f"/dags/{dag_id}/dagRuns/{run_id}/taskInstances")["task_instances"]


def log(dag_id: str, run_id: str, task_id: str, try_number: int = 1) -> str:
    path = f"/usr/local/airflow/logs/dag_id={dag_id}/run_id={run_id}/task_id={task_id}/attempt={try_number}.log"
    out = subprocess.run(["docker", "exec", SCHEDULER, "cat", path], capture_output=True, text=True)
    lines = []
    for line in out.stdout.splitlines():
        try:
            rec = json.loads(line)
        except ValueError:
            lines.append(line)
            continue
        extra = {k: v for k, v in rec.items() if k not in ("timestamp", "event", "level", "logger")}
        lines.append(f"{rec.get('level', '')[:5]:<5} {rec.get('logger', '')}: {rec.get('event', '')} {extra if extra else ''}")
    return "\n".join(lines)


def run(dag_id: str, tag: str = "") -> tuple[str, str, list[dict]]:
    unpause(dag_id)
    run_id = f"{tag or 'manual'}__{dag_id}__{time.strftime('%H%M%S')}_{time.time_ns() % 100000}"
    trigger(dag_id, run_id)
    state = wait(dag_id, run_id)
    return run_id, state, task_instances(dag_id, run_id)


if __name__ == "__main__":
    for dag_id in sys.argv[1:]:
        run_id, state, tis = run(dag_id, "e2e")
        print(f"{dag_id} {run_id} {state}")
        for ti in tis:
            print(f"  {ti['task_id']:<20} {ti['state']:<16} {ti.get('duration')}")

import json
import os
import statistics
import subprocess
import sys
import time
from datetime import datetime

import af

MODES = {
    "java_sdk": "bench_scala_java_sdk",
    "spark_submit_bash": "bench_spark_submit_bash",
    "spark_submit_operator": "bench_spark_submit_operator",
    "pyspark_task": "bench_pyspark_task",
    "java_sdk_daria": "bench_scala_daria_java_sdk",
    "java_sdk_plain": "bench_scala_plain_java_sdk",
    "spark_submit_daria": "bench_spark_submit_daria",
}
RESULT_DIRS = {"java_sdk_daria": "daria", "java_sdk_plain": "plain", "spark_submit_daria": "daria"}
RUNS = int(os.environ.get("BENCH_RUNS", "7"))
WARMUP = int(os.environ.get("BENCH_WARMUP", "1"))


def ts(s: str) -> float:
    return datetime.fromisoformat(s.replace("Z", "+00:00")).timestamp()


def result_file(mode: str, run_id: str) -> dict:
    safe = "".join(c if c.isalnum() or c in "_.-" else "_" for c in run_id)
    out = subprocess.run(
        ["docker", "exec", af.SCHEDULER, "cat", f"/tmp/bench_results/{RESULT_DIRS.get(mode, mode)}/{safe}.json"],
        capture_output=True,
        text=True,
    )
    return json.loads(out.stdout) if out.returncode == 0 else {}


def one(mode: str, dag_id: str, task_id: str = "workload", tag: str = "bench") -> dict:
    run_id, state, tis = af.run(dag_id, tag)
    ti = next(t for t in tis if t["task_id"] == task_id)
    rec = {
        "mode": mode,
        "run_id": run_id,
        "state": state,
        "ti_duration_s": ti["duration"],
        "queued_to_start_s": round(ts(ti["start_date"]) - ts(ti["queued_when"]), 3),
        "queued_to_end_s": round(ts(ti["end_date"]) - ts(ti["queued_when"]), 3),
    }
    if mode in MODES:
        rec.update({k: v for k, v in result_file(mode, run_id).items() if k != "mode"})
    else:
        rec["hwm_kb"] = af.call("GET", f"/dags/{dag_id}/dagRuns/{run_id}/taskInstances/{task_id}/xcomEntries/return_value")["value"]
    return rec


def summarize(records: list[dict]) -> dict:
    out = {}
    for mode in sorted({r["mode"] for r in records}):
        rs = [r for r in records if r["mode"] == mode and r["state"] == "success"]
        keys = ["ti_duration_s", "queued_to_start_s", "queued_to_end_s", "spark_start_ms", "compute_ms", "jvm_hwm_kb", "python_hwm_kb", "hwm_kb"]
        out[mode] = {"n": len(rs), "failed": sum(1 for r in records if r["mode"] == mode and r["state"] != "success")}
        for k in keys:
            vals = [r[k] for r in rs if isinstance(r.get(k), (int, float))]
            if vals:
                out[mode][k] = {"median": round(statistics.median(vals), 3), "min": min(vals), "max": max(vals)}
        if "compute_ms" in out[mode]:
            out[mode]["overhead_s_median"] = round(
                statistics.median(r["queued_to_end_s"] - r["compute_ms"] / 1000 for r in rs), 3
            )
            out[mode]["overhead_excl_spark_start_s_median"] = round(
                statistics.median(r["queued_to_end_s"] - (r["compute_ms"] + r["spark_start_ms"]) / 1000 for r in rs), 3
            )
    return out


def main() -> None:
    warm, measured = [], []
    for i in range(WARMUP + RUNS):
        for mode, dag_id in MODES.items():
            rec = one(mode, dag_id, tag="warmup" if i < WARMUP else "bench")
            (warm if i < WARMUP else measured).append(rec)
            print(json.dumps(rec), flush=True)
        for mode, task_id in (("noop_python", "python_noop"), ("noop_scala_java_sdk", "scala_noop")):
            rec = one(mode, "bench_noop", task_id, tag="warmup" if i < WARMUP else "bench")
            (warm if i < WARMUP else measured).append(rec)
            print(json.dumps(rec), flush=True)
    load = os.getloadavg()
    doc = {
        "generated_at": time.strftime("%Y-%m-%dT%H:%M:%S%z"),
        "airflow": "3.4.0 (apache/airflow:3.4.0-python3.12)",
        "airflow_java_sdk": "1.0.0",
        "spark": "4.3.0 local[*], driver 1g",
        "rows": int(os.environ.get("BENCH_ROWS", "20000000")),
        "runs_per_mode": RUNS,
        "warmup_per_mode": WARMUP,
        "host": {"nproc": os.cpu_count(), "loadavg_end": load},
        "summary": summarize(measured),
        "runs": measured,
        "warmup": warm,
    }
    path = sys.argv[1] if len(sys.argv) > 1 else "bench.json"
    with open(path, "w") as f:
        json.dump(doc, f, indent=2)
    print(json.dumps(doc["summary"], indent=2))


if __name__ == "__main__":
    main()

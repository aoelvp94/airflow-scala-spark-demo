import json
import sys

doc = json.load(open(sys.argv[1] if len(sys.argv) > 1 else "bench.json"))
s = doc["summary"]
LABELS = {
    "java_sdk": "(a) Scala task via Java SDK (`@task.stub` → JavaCoordinator JVM)",
    "spark_submit_bash": "(b1) BashOperator `spark-submit` (Scala jar)",
    "spark_submit_operator": "(b2) SparkSubmitOperator (Scala jar)",
    "pyspark_task": "(c) PySpark `@task`",
    "noop_scala_java_sdk": "no-op Scala task via Java SDK",
    "noop_python": "no-op Python `@task`",
}


def m(mode: str, key: str, scale: float = 1.0, fmt: str = "{:.2f}") -> str:
    v = s.get(mode, {}).get(key)
    return fmt.format(v["median"] * scale) if v else "–"


rows = [
    "| Mode | n | queued→end (s) | TI duration (s) | queued→start (s) | Spark session start (s) | Spark compute (s) | Overhead excl. compute (s) | Overhead excl. Spark start+compute (s) | Driver JVM peak RSS (MiB) | Python task peak RSS (MiB) |",
    "|---|---|---|---|---|---|---|---|---|---|---|",
]
for mode in ["java_sdk", "spark_submit_bash", "spark_submit_operator", "pyspark_task"]:
    r = s[mode]
    py = m(mode, "python_hwm_kb", 1 / 1024, "{:.0f}")
    rows.append(
        f"| {LABELS[mode]} | {r['n']} | {m(mode, 'queued_to_end_s')} | {m(mode, 'ti_duration_s')} | {m(mode, 'queued_to_start_s')} | "
        f"{m(mode, 'spark_start_ms', 1 / 1000)} | {m(mode, 'compute_ms', 1 / 1000)} | {r['overhead_s_median']:.2f} | "
        f"{r['overhead_excl_spark_start_s_median']:.2f} | {m(mode, 'jvm_hwm_kb', 1 / 1024, '{:.0f}')} | {py} |"
    )
noop = [
    "| Mode | n | queued→end (s) | TI duration (s) | queued→start (s) | Peak RSS of the task process (MiB) |",
    "|---|---|---|---|---|---|",
]
for mode in ["noop_scala_java_sdk", "noop_python"]:
    r = s[mode]
    noop.append(
        f"| {LABELS[mode]} | {r['n']} | {m(mode, 'queued_to_end_s')} | {m(mode, 'ti_duration_s')} | {m(mode, 'queued_to_start_s')} | {m(mode, 'hwm_kb', 1 / 1024, '{:.0f}')} |"
    )
print("\n".join(rows))
print()
print("\n".join(noop))

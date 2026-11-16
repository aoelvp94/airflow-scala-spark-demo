from __future__ import annotations

import json
import os
import resource
import time
from pathlib import Path

from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.providers.standard.operators.bash import BashOperator
from airflow.sdk import dag, task

APP_JAR = "/usr/local/airflow/include/spark-submit/scala-spark-workload.jar"
APP_CLASS = "example.scalaspark.SubmitApp"
ROWS = int(os.environ.get("BENCH_ROWS", "20000000"))
COMMON = {"schedule": None, "catchup": False, "max_active_runs": 1, "tags": ["bench", "spark"]}


def _vm_hwm_kb(pid: int | str = "self") -> int:
    for line in Path(f"/proc/{pid}/status").read_text().splitlines():
        if line.startswith("VmHWM:"):
            return int(line.split()[1])
    return -1


def scala_stub(task_id: str, **kwargs):
    def body(): ...

    body.__name__ = task_id
    return task.stub(task_id=task_id, queue="scala", **kwargs)(body)()


@dag(dag_id="bench_scala_java_sdk", **COMMON)
def bench_scala_java_sdk():
    scala_stub("workload")


@dag(dag_id="bench_spark_submit_bash", **COMMON)
def bench_spark_submit_bash():
    BashOperator(
        task_id="workload",
        bash_command=(
            f"spark-submit --master 'local[*]' --driver-memory 1g --conf spark.ui.enabled=false "
            f"--class {APP_CLASS} {APP_JAR} spark_submit_bash '{{{{ run_id }}}}'"
        ),
    )


@dag(dag_id="bench_spark_submit_operator", **COMMON)
def bench_spark_submit_operator():
    SparkSubmitOperator(
        task_id="workload",
        conn_id="spark_local",
        application=APP_JAR,
        java_class=APP_CLASS,
        driver_memory="1g",
        conf={"spark.ui.enabled": "false"},
        application_args=["spark_submit_operator", "{{ run_id }}"],
    )


@dag(dag_id="bench_pyspark_task", **COMMON)
def bench_pyspark_task():
    @task(task_id="workload")
    def workload(run_id: str | None = None) -> dict:
        from pyspark.sql import SparkSession
        from pyspark.sql import functions as F

        mode = "pyspark_task"
        t0 = time.perf_counter_ns()
        spark = (
            SparkSession.builder.appName(f"bench-{mode}")
            .master("local[*]")
            .config("spark.ui.enabled", "false")
            .config("spark.sql.shuffle.partitions", "8")
            .config("spark.driver.host", "127.0.0.1")
            .config("spark.driver.bindAddress", "127.0.0.1")
            .config("spark.driver.memory", "1g")
            .getOrCreate()
        )
        t1 = time.perf_counter_ns()
        try:
            out = f"/tmp/bench_out/{mode}"
            (
                spark.range(0, ROWS, 1, 8)
                .selectExpr("id % 1000 as k", "id * 2 as v")
                .groupBy("k")
                .agg(F.sum("v").alias("s"), F.count(F.lit(1)).alias("c"))
                .write.mode("overwrite")
                .parquet(out)
            )
            row = spark.read.parquet(out).agg(F.sum("c"), F.count(F.lit(1)), F.sum("s")).first()
            t2 = time.perf_counter_ns()
            jvm_pid = spark.sparkContext._jvm.java.lang.ProcessHandle.current().pid()
            result = {
                "mode": mode,
                "rows": row[0],
                "groups": row[1],
                "checksum": row[2],
                "spark_start_ms": (t1 - t0) // 1_000_000,
                "compute_ms": (t2 - t1) // 1_000_000,
                "jvm_hwm_kb": _vm_hwm_kb(jvm_pid),
                "python_hwm_kb": resource.getrusage(resource.RUSAGE_SELF).ru_maxrss,
            }
        finally:
            spark.stop()
        results = Path("/tmp/bench_results", mode)
        results.mkdir(parents=True, exist_ok=True)
        safe = "".join(c if c.isalnum() or c in "_.-" else "_" for c in run_id)
        (results / f"{safe}.json").write_text(json.dumps(result))
        print("BENCH_RESULT", json.dumps(result))
        return result

    workload(run_id="{{ run_id }}")


@dag(dag_id="bench_noop", **COMMON)
def bench_noop():
    @task(task_id="python_noop")
    def python_noop() -> int:
        return _vm_hwm_kb()

    python_noop() >> scala_stub("scala_noop")


bench_scala_java_sdk()
bench_spark_submit_bash()
bench_spark_submit_operator()
bench_pyspark_task()
bench_noop()

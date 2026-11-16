# Benchmark: one Spark workload, three ways to run it from Airflow

Run on 2026-11-16 at 15:10–15:15 (Asia/Tbilisi, UTC+4), in the Astro CLI project in `astro/`.

Setup:

- Airflow 3.4.0 (`apache/airflow:3.4.0-python3.12`) with the LocalExecutor, started with `astro dev start`
- Airflow Java SDK 1.0.0 from Maven Central, Temurin 17 JRE
- Spark 4.3.0 with Scala 2.13.18, `local[*]`, 1 GiB driver heap in every mode
- Box with 8 vCPUs and 15 GiB RAM. A separate benchmark was running on the same box at the same time (load average 5–6), so the runs were interleaved round-robin.
- 1 warm-up round, then 7 measured rounds. Every figure is a median, and all 42 measured runs succeeded.

Workload (identical in Scala and PySpark): `spark.range(20_000_000, 8 partitions)` → `id % 1000`, `id * 2` → `groupBy(k).agg(sum, count)` → write Parquet → read it back and aggregate. The checksum is verified in every run (`399999980000000`).

Modes:

- **(a)** `bench_scala_java_sdk`: a Python `@task.stub(queue="scala")` handled by `example.scalaspark.SparkWorkload` in the Scala bundle. The JavaCoordinator starts a JVM, and Spark runs inside that JVM.
- **(b1)** `bench_spark_submit_bash`: a `BashOperator` runs `spark-submit --class example.scalaspark.SubmitApp` on a thin Scala jar. Spark comes from the pyspark 4.3.0 wheel.
- **(b2)** `bench_spark_submit_operator`: a `SparkSubmitOperator` runs the same jar with the same class.
- **(c)** `bench_pyspark_task`: a PySpark `@task`.

Columns:

- *queued→end*: TI `queued_when` → `end_date`. This is the honest wall time, because the JavaCoordinator only reports the task as running after the JVM is up.
- *TI duration*: Airflow's own `duration` (`start_date` → `end_date`).
- *Spark session start* and *Spark compute*: measured inside the job.
- *Overhead*: queued→end minus compute, shown with and without the Spark session start.
- *Peak RSS*: `VmHWM` of the driver JVM, plus the Python task process for PySpark. The Airflow supervisor process, which every mode has, is not counted.

| Mode | n | queued→end (s) | TI duration (s) | queued→start (s) | Spark session start (s) | Spark compute (s) | Overhead excl. compute (s) | Overhead excl. Spark start+compute (s) | Driver JVM peak RSS (MiB) | Python task peak RSS (MiB) |
|---|---|---|---|---|---|---|---|---|---|---|
| (a) Scala task via Java SDK (`@task.stub` → JavaCoordinator JVM) | 7 | 5.85 | 5.36 | 0.49 | 1.14 | 3.82 | 2.09 | 0.98 | 515 | – |
| (b1) BashOperator `spark-submit` (Scala jar) | 7 | 6.96 | 6.95 | 0.01 | 1.94 | 3.20 | 3.88 | 1.89 | 527 | – |
| (b2) SparkSubmitOperator (Scala jar) | 7 | 6.96 | 6.95 | 0.01 | 1.92 | 3.29 | 3.66 | 1.75 | 518 | – |
| (c) PySpark `@task` | 7 | 7.38 | 7.37 | 0.01 | 3.29 | 3.21 | 4.24 | 0.95 | 498 | 332 |

| Mode | n | queued→end (s) | TI duration (s) | queued→start (s) | Peak RSS of the task process (MiB) |
|---|---|---|---|---|---|
| no-op Scala task via Java SDK | 7 | 0.76 | 0.27 | 0.49 | 127 |
| no-op Python `@task` | 7 | 0.56 | 0.55 | 0.01 | 245 |

## Reading it

- **The Java SDK path is the fastest end to end:** 5.85 s, against 6.96 s for both spark-submit variants (−16%) and 7.38 s for PySpark (−21%).
- **The gap comes from startup, not compute.**
  - (a) creates the `SparkSession` in 1.1 s inside an already running JVM.
  - spark-submit takes 1.9 s, because its launcher first runs an extra JVM to build the command line.
  - PySpark takes 3.3 s to start a JVM gateway through py4j.
- **Overhead without Spark** is about 1.0 s for (a) and (c), against about 1.8 s for spark-submit.
- **The JVM start shows up in queued→start for the Java SDK:** 0.49 s in every Scala run, against 0.01 s for the Python and Bash tasks. Airflow's TI `duration` hides it, so use queued→end when comparing.
- **Compute is about 0.6 s slower in (a):** 3.8 s against 3.2 s. This held in every run (min 3.6 s against 3.1 s) and is not explained yet. The candidates are the JVM flags (the coordinator's `jvm_args` versus what Spark's launcher adds) and the log appender. Before quoting compute numbers, check this with a larger workload.
- **Memory:** the driver JVM peaks at roughly the same RSS in every mode (500–530 MiB). PySpark adds a 332 MiB Python process on top. A no-op Scala task peaks at 127 MiB of JVM RSS, against 245 MiB for a no-op Python task process.
- **No-op tasks:**
  - **Python task:** 0.56 s queued→end.
  - **Scala task via the Java SDK:** 0.76 s, almost all of it JVM start and connect-back (0.49 s before the task is marked running).
  - **First Python run (cold):** 3.3 s, because it parses `bench_dags.py` with the Spark provider import. That run is excluded as warm-up.

## Reproduce

```bash
./build-bundle.sh
cd astro && astro dev start --no-browser --wait 3m && cd ..
cd bench && BENCH_RUNS=7 BENCH_WARMUP=1 uv run --no-project python bench.py bench.json
uv run --no-project python make_bench_md.py bench.json
```

Raw per-run data is in `bench.json` (the same file as `bench/bench.json`).

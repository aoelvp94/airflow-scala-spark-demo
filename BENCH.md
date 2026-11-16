# Benchmark: one Spark workload, three ways to run it from Airflow, plus spark-daria

Run on 2026-11-16 at 19:56–20:06 (Asia/Tbilisi, UTC+4), in the Astro CLI project in `astro/`.

Setup:

- Airflow 3.4.0 (`apache/airflow:3.4.0-python3.12`) with the LocalExecutor, started with `astro dev start`
- Airflow Java SDK 1.0.0 from Maven Central, Temurin 17 JRE
- Spark 4.3.0 with Scala 2.13.18, `local[*]`, 1 GiB driver heap in every mode; spark-daria 1.2.4
- Box with 8 vCPUs and 15 GiB RAM, nothing else running (load average 1.9 at start, 3.3 at end)
- 1 warm-up round, then 7 measured rounds, interleaved round-robin. Every figure is a median, and all 63 measured workload runs plus 14 no-op runs succeeded.

Host-network environment (local only, not committed):

- `dockerd` ran without iptables (`--iptables=false --ip6tables=false --bridge=none`). Containers on user-defined bridge networks get an IP but no embedded DNS and no outbound traffic, so `docker build` and `RUN` steps cannot use the default bridge.
- The image was pre-built with `docker build --network host -t airflow-scala-spark-local:latest astro/` and started with `astro dev start --no-browser -i airflow-scala-spark-local:latest`.
- All services already run with `network_mode: host` through `astro/docker-compose.override.yml` (Postgres on 56432, API on 58280). The `astro dev start` health check still times out because it probes the default port; the API on 58280 was healthy.
- The Scala bundle was built with the local sbt fallback of `sbt.sh` (Temurin 21.0.13, `-release 17`), because the sbt Docker image needs bridge networking to resolve dependencies.

Workload for (a)–(c), identical in Scala and PySpark: `spark.range(20_000_000, 8 partitions)` → `id % 1000`, `id * 2` → `groupBy(k).agg(sum, count)` → write Parquet → read it back and aggregate. The checksum is verified in every run (`399999980000000`).

Workload for (d1)–(d3): the spark-daria pipeline from `DariaWorkload.scala` on the same 20M-row source (snake_case renames, presence/absence/schema validation, `singleSpace`, `isTruthy`, then the same aggregate). (d2) is the same pipeline in plain Spark. Same checksum.

Modes:

- **(a)** `bench_scala_java_sdk`: a Python `@task.stub(queue="scala")` handled by `example.scalaspark.SparkWorkload` in the Scala bundle. The JavaCoordinator starts a JVM, and Spark runs inside that JVM.
- **(b1)** `bench_spark_submit_bash`: a `BashOperator` runs `spark-submit --class example.scalaspark.SubmitApp` on a thin Scala jar. Spark comes from the pyspark 4.3.0 wheel.
- **(b2)** `bench_spark_submit_operator`: a `SparkSubmitOperator` runs the same jar with the same class.
- **(c)** `bench_pyspark_task`: a PySpark `@task`.
- **(d1)** `bench_scala_daria_java_sdk`: `@task.stub` handled by `SparkDariaWorkload`.
- **(d2)** `bench_scala_plain_java_sdk`: `@task.stub` handled by `SparkPlainWorkload`.
- **(d3)** `bench_spark_submit_daria`: `BashOperator` running `spark-submit --jars spark-daria_2.13-1.2.4.jar ... SubmitApp daria`.

Columns:

- *queued→end*: TI `queued_when` → `end_date`. This is the honest wall time, because the JavaCoordinator only reports the task as running after the JVM is up.
- *TI duration*: Airflow's own `duration` (`start_date` → `end_date`).
- *Spark session start* and *Spark compute*: measured inside the job.
- *Overhead*: queued→end minus compute, shown with and without the Spark session start.
- *Peak RSS*: `VmHWM` of the driver JVM, plus the Python task process for PySpark. The Airflow supervisor process, which every mode has, is not counted.

| Mode | n | queued→end (s) | TI duration (s) | queued→start (s) | Spark session start (s) | Spark compute (s) | Overhead excl. compute (s) | Overhead excl. Spark start+compute (s) | Driver JVM peak RSS (MiB) | Python task peak RSS (MiB) |
|---|---|---|---|---|---|---|---|---|---|---|
| (a) Scala task via Java SDK (`@task.stub` → JavaCoordinator JVM) | 7 | 7.01 | 6.41 | 0.70 | 1.30 | 4.57 | 2.45 | 1.23 | 508 | – |
| (b1) BashOperator `spark-submit` (Scala jar) | 7 | 8.39 | 8.36 | 0.02 | 2.36 | 3.87 | 4.54 | 2.18 | 528 | – |
| (b2) SparkSubmitOperator (Scala jar) | 7 | 8.45 | 8.44 | 0.02 | 2.33 | 3.89 | 4.48 | 2.15 | 548 | – |
| (c) PySpark `@task` | 7 | 9.25 | 9.23 | 0.02 | 4.01 | 3.78 | 5.32 | 1.32 | 514 | 334 |
| (d1) spark-daria task via Java SDK (`@task.stub`) | 7 | 7.85 | 7.20 | 0.65 | 1.31 | 5.26 | 2.58 | 1.31 | 552 | – |
| (d2) same pipeline in plain Spark via Java SDK (`@task.stub`) | 7 | 7.02 | 6.39 | 0.62 | 1.27 | 4.52 | 2.48 | 1.23 | 539 | – |
| (d3) BashOperator `spark-submit` spark-daria pipeline | 7 | 8.59 | 8.57 | 0.02 | 2.17 | 4.04 | 4.56 | 2.42 | 557 | – |

| Mode | n | queued→end (s) | TI duration (s) | queued→start (s) | Peak RSS of the task process (MiB) |
|---|---|---|---|---|---|
| no-op Scala task via Java SDK | 7 | 0.95 | 0.33 | 0.61 | 140 |
| no-op Python `@task` | 7 | 0.74 | 0.72 | 0.02 | 247 |

## Reading it

- **The Java SDK path is still the fastest end to end:** 7.01 s, against 8.39–8.45 s for spark-submit (−17%) and 9.25 s for PySpark (−24%). The ranking and the gaps match the earlier run.
- **The gap comes from startup, not compute.** `SparkSession` start is 1.3 s in the JavaCoordinator JVM, 2.3 s with spark-submit (extra launcher JVM) and 4.0 s for PySpark (py4j gateway).
- **The JVM start shows up in queued→start for the Java SDK:** 0.6–0.7 s for every Scala task, against 0.02 s for Python and Bash. Airflow's TI `duration` hides it.
- **Compute is slower inside the JavaCoordinator JVM:** 4.57 s for (a) against 3.87 s for (b1), and 4.52 s for (d2) against 4.04 s for (d3). The 0.5–0.7 s gap from the earlier run is still there and still unexplained (JVM flags from `jvm_args` versus Spark's launcher, or the log appender).
- **spark-daria through Airflow:**
  - Via the Java SDK, (d1) spark-daria is 0.84 s slower end to end than (d2) plain Spark (7.85 s vs 7.02 s), all of it in compute (5.26 s vs 4.52 s). All 7 daria runs (min 4.70 s) were slower than the plain median, though the ranges overlap (plain max 5.26 s).
  - Via spark-submit, (d3) spark-daria takes 4.04 s compute, close to the plain workload in (b1) (3.87 s).
  - The local single-session bench below shows no difference (353 vs 354 ms), so the cost is not the spark-daria API itself on a hot JVM. It shows up on a fresh JVM in the Java SDK path, which points at cold class loading or JIT for the extra library plus the same JVM-flag gap. Treat it as an open question, not as spark-daria overhead.
  - Memory: the daria driver JVM peaks at 552–557 MiB, against 508–528 MiB for the plain workload.
- **No-op tasks:** Python 0.74 s queued→end, Scala via the Java SDK 0.95 s (0.61 s before the task is marked running). Peak RSS 140 MiB for the JVM against 247 MiB for the Python task process.
- **Absolute numbers are 15–25% higher than in the earlier run** (5.85 / 6.96 / 7.38 s, kept in `bench.json` under `previous_run`), although nothing else was running this time. The Docker daemon setup is different (no iptables, host networking only, fresh image), so only compare modes within one run.

## Reproduce

```bash
./build-bundle.sh
cd astro && astro dev start --no-browser --wait 3m && cd ..
cd bench && BENCH_RUNS=7 BENCH_WARMUP=1 uv run --no-project python bench.py bench.json
uv run --no-project python make_bench_md.py bench.json
```

On a Docker daemon without iptables, add `docker build --network host -t airflow-scala-spark-local:latest astro/` and pass `-i airflow-scala-spark-local:latest` to `astro dev start`.

Raw per-run data is in `bench.json` (the same file as `bench/bench.json`).

## spark-daria vs plain Spark (local, not via Airflow)

Run on 2026-11-16 at 19:33 (Asia/Tbilisi, UTC+4), on the box, with a single `SparkSession` (`local[*]`, Temurin 21.0.13, Spark 4.3.0, spark-daria 1.2.4). Docker was unavailable at the time. The Airflow path for spark-daria is now measured in modes (d1)–(d3) above.

This compares a spark-daria pipeline with an equivalent plain-Spark pipeline on the same 20,000,000-row source (`userId`, `groupKey`, `orderAmount`, `isActive`, `displayName`). Both paths rename to snake_case, check required / prohibited columns and the schema, apply `singleSpace` on `display_name` and an `isTruthy` flag on `is_active`, then `groupBy(group_key).agg(sum, count)` and write/read Parquet. The checksum is verified in every run (`399999980000000`, the same value as the workload above). 1 warm-up round, then 7 measured rounds interleaved, every figure a median.

The session is reused on purpose: the comparison is library overhead, not JVM or `SparkSession` start (1.69 s once). Do not compare these compute times with the Airflow table above, where each run started a fresh JVM.

| Mode | n | Spark compute (ms) | Driver JVM peak RSS (MiB, process-wide VmHWM) |
|---|---|---|---|
| spark-daria 1.2.4 (`camelCaseToSnakeCaseColumns`, `DariaValidator`, `composeTransforms`, `singleSpace`, `isTruthy`) | 7 | 353 (min 321, max 404) | 673 |
| plain Spark (same renames, checks and expressions) | 7 | 354 (min 297, max 404) | 686 |

## Reading it

- **No meaningful compute difference.** The medians are 353 ms vs 354 ms, inside run-to-run noise. spark-daria 1.2.4 (built for Spark 3.3.4) ran correctly on Spark 4.3.0 for these APIs.
- **Results match.** Every measured run in both modes returned 20,000,000 rows, 1,000 groups and checksum `399999980000000`.
- **Memory is not comparable per mode.** Both paths share one JVM, so `VmHWM` only rises. The figures are the process peak after that mode's measured rounds, not an isolated working set.
- **The first warm-up of the daria path took 5.2 s**, then 0.57 s for the first plain run, then 0.3–0.4 s once Spark was hot. That cold start is excluded from the medians.

## Reproduce

```bash
./sbt.sh -batch test
./sbt.sh -batch 'runMain example.scalaspark.DariaBench ../bench/daria-local.json'
```

`sbt.sh` uses Docker when it is available, or a local JDK and sbt if it is not. Raw data is in `bench/daria-local.json` and in the `daria_local` key of `bench.json`.

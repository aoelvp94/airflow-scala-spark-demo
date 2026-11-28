# Reviewer demo: encoding `setXCom` values with Jackson modules

This directory (`setxcom-demo/`) is a self-contained demo of a proposed Java SDK
change to how `setXCom` (and the task-state store) encode a value:

- The value is encoded with Jackson and every Jackson module in the bundle. With
  `jackson-module-scala` on the classpath, as in any Spark bundle, a Scala `Map`,
  `Seq`, `Option` or case class is stored as real JSON.
- An object Jackson finds no properties for is rejected with
  `IllegalArgumentException` instead of being stored as `{}`.
- Java values (maps from `.asJava`, beans, boxed numbers) are stored as before.

Run every command below from this directory. The proposed patches are in
`patches/` and apply on Apache Airflow `main` at `9c93fe8bbf`; see
`patches/README.md`. The fix is under review in
[apache/airflow#77249](https://github.com/apache/airflow/pull/77249).

## Why the fix is overlaid on `airflow-sdk` 1.0.0

Airflow 3.4.0's Java coordinator only knows supervisor schemas `2026-06-16`
and `2026-10-30`. A current `main` build of the SDK is `1.1.0-SNAPSHOT` and
advertises schema `2027-02-28`, which 3.4.0 rejects. Official `airflow-sdk`
1.0.0 already carries `2026-10-30`, so this demo compiles the fixed
`execution/Frame.kt` and the new `internal/JacksonModules.kt` against that
released jar and leaves the manifest alone. The 1.0.0 callers of `Frame` are
unchanged. The PR also moves Dag run conf and argument binding onto the same
module registration; that part is not overlaid, because this demo only pushes
XComs.

## Prerequisites

- Docker, with permission to run containers
- An `apache/airflow` checkout with the fix applied (see `patches/README.md`):

  ```bash
  export AIRFLOW_SRC=/path/to/apache/airflow
  ```
- Network on the first run (Maven Central for `airflow-sdk` 1.0.0 and
  `jackson-module-scala` 2.22.2, and a one-time `kotlinc` 2.4.20 download used by
  the overlay script)
- Roughly 4 GB for the `apache/airflow:3.4.0-python3.12` image plus a Temurin 17 JRE

No JDK is needed on the host for the Airflow runs. The SDK unit tests in step 9
need the Gradle wrapper in that checkout and JDK 17 or 21 to run Gradle.

## 1. Build the Airflow image

```bash
docker build -t airflow-scala-docs-e2e:3.4.0 -f Dockerfile .
```

The Dockerfile is `FROM apache/airflow:3.4.0-python3.12` plus a Temurin 17 JRE,
which the Java coordinator needs.

## 2. Build the Scala handler bundles (official 1.0.0)

```bash
make bundle
```

This compiles the handlers against published `airflow-sdk` 1.0.0 (Scala 2.13.18,
inside `sbtscala/scala-sbt:eclipse-temurin-17.0.21_9_1.13.1_2.13.18`) twice:

- `bundle-baseline/`: the handlers and their runtime jars
- `bundle-baseline-scala/`: the same plus `jackson-module-scala_2.13` 2.22.2, which
  matches the 1.0.0 SDK's `jackson-databind` 2.22.2

## 3. Overlay the fix onto 1.0.0

```bash
make overlay
```

This copies `execution/Frame.kt` and `internal/JacksonModules.kt` from
`$AIRFLOW_SRC`, compiles them against the official 1.0.0 jar (`-module-name org.apache.airflow_sdk`, `-Xfriend-paths`), and
overlays the new classes onto a copy of that jar. The manifest still has
`Airflow-Supervisor-Schema-Version: 2026-10-30`.

Output: `build/airflow-sdk-1.0.0-setxcom.jar`, plus `bundle-patched/` and
`bundle-patched-scala/`, which are the two baseline bundles with only
`airflow-sdk-1.0.0.jar` replaced.

## 4. What the Dag does

Nine independent `@task.stub(queue="java")` tasks in `setxcom_fix`, so one
failure does not skip the others:

| task | value pushed |
|---|---|
| `extract` | boxed long `42` |
| `score` | `Map("region" -> input.region, "threshold" -> input.threshold).asJava` |
| `push_as_java` | `Map("region" -> "eu", "threshold" -> 0.5).asJava` |
| `push_java_pojo` | Java bean `ScoreSummaryBean` with `getRegion` / `getThreshold` |
| `push_scala_map` | Scala `Map("region" -> "eu", "threshold" -> 0.5)` |
| `push_scala_seq` | Scala `Seq(1, 2, 3)` |
| `push_scala_option` | `Some("eu")` |
| `push_case_class` | `ScoreSummary("eu", 0.5)` |
| `push_empty_object` | Java `EmptyMarker`, a class with no properties |

Each run is standalone Airflow 3.4.0 with LocalExecutor and `JavaCoordinator`
(`task_handler_bundle_name=java-handlers`,
`main_class=com.mycompany.airflow.hello.Main`). Parallelism is capped at 3: with
nine JVM tasks starting at once on a busy machine, the standalone API server can
miss a task's first call, which fails it with "Could not reach the Execution API
server" before any task code runs. Every run ends in **failed**
because at least one task fails on purpose; read the per-task results.

## 5. Baseline: unpatched 1.0.0

```bash
make baseline
```

| task | state | XCom `return_value` |
|---|---|---|
| `extract` | success | `42` |
| `score`, `push_as_java`, `push_java_pojo` | success | `{"region": "eu", "threshold": 0.5}` |
| `push_scala_map` | success | `{"scala$collection$immutable$Map$Map2$$key1": "region", "scala$collection$immutable$Map$Map2$$value1": "eu", …, "empty": false, "traversableAgain": true}` |
| `push_scala_seq` | success | `{"empty": false, "traversableAgain": true}` |
| `push_scala_option` | success | `{"empty": false, "defined": true}` |
| `push_case_class` | success | `{}` |
| `push_empty_object` | failed | none |

The Scala tasks succeed and store bean properties or `{}`: that silent store is
the bug. The empty Java object already fails, but with Jackson's raw message:

```
java.lang.IllegalArgumentException: No serializer found for class com.mycompany.airflow.hello.EmptyMarker and no properties discovered to create BeanSerializer (to avoid exception, disable SerializationFeature.FAIL_ON_EMPTY_BEANS) (through reference chain: org.apache.airflow.sdk.execution.comm.SetXCom["value"])
```

Jackson fails like this only for classes without annotations. Every Scala class
carries `@ScalaSignature` (and every Kotlin class `@Metadata`), and an empty
annotated class is written as `{}` instead, which is why the case class slips
through.

## 6. Patched, without `jackson-module-scala`

```bash
make patched
```

| task | state | XCom `return_value` |
|---|---|---|
| `extract` | success | `42` |
| `score`, `push_as_java`, `push_java_pojo` | success | `{"region": "eu", "threshold": 0.5}` |
| `push_scala_map`, `push_scala_seq`, `push_scala_option` | success | same bean properties as the baseline |
| `push_case_class` | failed | none |
| `push_empty_object` | failed | none |

Both failures now carry the same message, for example:

```
java.lang.IllegalArgumentException: Cannot send com.mycompany.airflow.hello.ScoreSummary to Airflow: Jackson finds no properties to encode, so it would be stored as {}. Convert it to a java.util.Map, a java.util.List or a JSON scalar, or add a Jackson module for the type to the bundle (jackson-module-scala for Scala collections, Option and case classes).
```

Without the module, Scala collections and `Option` still have bean properties
(`empty`, `traversableAgain`, `defined`), so Jackson stores those. That is the
known limit of a fix that does not special-case Scala.

## 7. Patched, with `jackson-module-scala`

```bash
make patched-scala
```

| task | state | XCom `return_value` |
|---|---|---|
| `extract` | success | `42` |
| `score`, `push_as_java`, `push_java_pojo` | success | `{"region": "eu", "threshold": 0.5}` |
| `push_scala_map` | success | `{"region": "eu", "threshold": 0.5}` |
| `push_scala_seq` | success | `[1, 2, 3]` |
| `push_scala_option` | success | `"eu"` |
| `push_case_class` | success | `{"region": "eu", "threshold": 0.5}` |
| `push_empty_object` | failed | none, with the same message as in step 6 |

The Java values are unchanged. This run also guards against a regression an
earlier version of the fix had: `jackson-module-scala` turns untyped values into
Scala collections when Jackson reads them back, which the wire encoder rejects,
so every `setXCom` in a Spark bundle failed. The fix now reads the encoded value
back without the bundle's modules.

## 8. Read the evidence

```bash
make evidence
```

Then open:

- `evidence/EVIDENCE.md`: run ids, timestamps, per-task state, XCom and error for all three runs
- `evidence/{baseline,patched,patched-scala}-xcom.json` and `evidence/*-errors.json`
- `evidence/log-excerpts/`: the `IllegalArgumentException` lines from the patched run
- `evidence/logs-*/` and `evidence/*-airflow.db*`: written locally when you re-run (not in git)

To run steps 1–8 in one go: `make all` (or `./run.sh all`).

## 9. Java SDK unit tests (the fix itself)

```bash
cd "$AIRFLOW_SRC/java-sdk"
./gradlew :sdk:check
```

or, from this directory: `make sdk-check`.

With the patch on `9c93fe8bbf`, `:sdk:check` ran 423 tests with 0 failures, plus
ktlint and spotless (Gradle 8.14.6 on Temurin 17).

## Layout

```
Dockerfile          apache/airflow:3.4.0-python3.12 + Temurin 17 JRE
dags/               Python stub Dag
scala-src/          sbt thin-bundle project (handlers, Java TaskInput, bean and empty class)
scripts/            bundle, overlay, run, and evidence helpers
Makefile            image, bundle, overlay, baseline, patched, patched-scala, evidence, sdk-check
patches/            setXCom encoding fix and Scala docs patches (apply on 9c93fe8bbf)
evidence/           captured results (EVIDENCE.md, JSON, log excerpts)
```

`make clean` removes built bundles, containers, and evidence dumps.

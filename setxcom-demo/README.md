# Reviewer demo: fail loudly on Scala types in `setXCom`

This directory (`setxcom-demo/`) is a self-contained demo of a proposed
Java SDK change that rejects a Scala `Map`, `Seq`, `Option` or case class
in `setXCom` with `IllegalArgumentException`, while leaving Java-friendly
values unchanged.

Run every command below from this directory. The copies of the proposed
patches are in `patches/` (they apply on Apache Airflow `main` at
`9eb25f92`; see `patches/README.md`). Upstream PRs have not been opened
yet; replace the placeholder below once they exist:

- setXCom loud-failure PR: *not opened yet*
- Scala docs PR: *not opened yet*

## Why the fix is overlaid on `airflow-sdk` 1.0.0

Airflow 3.4.0's Java coordinator only knows supervisor schemas `2026-06-16`
and `2026-10-30`. A current `main` build of the SDK is `1.1.0-SNAPSHOT` and
advertises schema `2027-02-28`, which 3.4.0 rejects. Official
`airflow-sdk` 1.0.0 already carries `2026-10-30`, so this demo compiles the
fixed Kotlin sources against that released jar and leaves the manifest alone.

## Prerequisites

- Docker, with permission to run containers
- An `apache/airflow` checkout with the setXCom loud-failure patch applied.
  From a clean tree at `9eb25f92` (or the upstream PR branch, once it exists):

  ```bash
  git apply /path/to/airflow-scala-spark-demo/setxcom-demo/patches/setxcom-loud-failure.patch
  # optional, docs only:
  git apply /path/to/airflow-scala-spark-demo/setxcom-demo/patches/scala-docs.patch
  export AIRFLOW_SRC=/path/to/apache/airflow
  ```
- Network on the first run (Maven Central for `airflow-sdk` 1.0.0, and a
  one-time `kotlinc` 2.4.20 download used by the overlay script)
- Roughly 4 GB for the `apache/airflow:3.4.0-python3.12` image plus a Temurin 17 JRE

A JDK is **not** required on the host for the Airflow runs. To run the SDK
unit tests in step 8 you need JDK 11+ (17 is fine) and the Gradle wrapper in
that checkout.

## 1. Build the Airflow image

From this directory:

```bash
docker build -t airflow-scala-docs-e2e:3.4.0 -f Dockerfile .
```

The Dockerfile is `FROM apache/airflow:3.4.0-python3.12` plus a Temurin 17 JRE,
which the Java coordinator needs.

## 2. Point at the fix

```bash
export AIRFLOW_SRC=/path/to/apache/airflow   # checkout with the setXCom fix applied (see Prerequisites)
```

If you leave `AIRFLOW_SRC` unset, the overlay script also looks for a few
local worktrees (`airflow-verify`, `airflow-setxcom`) next to this repo, when
they exist.

## 3. Build the Scala handler bundle (official 1.0.0)

```bash
make bundle
```

This compiles the handlers against published `airflow-sdk` 1.0.0 (Scala 2.13.18,
inside `sbtscala/scala-sbt:eclipse-temurin-17.0.21_9_1.13.1_2.13.18`) and writes
`bundle-baseline/`. The application JAR and the official SDK jar both stay in
that bundle for the baseline run.

## 4. Overlay the fix onto 1.0.0

```bash
make overlay
```

This downloads the 1.0.0 sources jar, copies `JsonValue.kt` and `Frame.kt` from
`$AIRFLOW_SRC`, patches only `setXCom` / `setTaskStateStore` in the 1.0.0
`execution/Client.kt` to call `jsonValue(...)`, compiles those three files
against the official 1.0.0 jar (`-module-name org.apache.airflow_sdk`,
`-Xfriend-paths`), and overlays the new classes onto a copy of the official
jar. The manifest still has `Airflow-Supervisor-Schema-Version: 2026-10-30`.

Output: `build/airflow-sdk-1.0.0-setxcom.jar` and `bundle-patched/` (the same
application jars as the baseline, with only `airflow-sdk-1.0.0.jar` replaced).

## 5. Run the baseline (unpatched 1.0.0)

```bash
make baseline
```

Standalone Airflow 3.4.0, LocalExecutor, `JavaCoordinator`,
`task_handler_bundle_name=java-handlers`,
`main_class=com.mycompany.airflow.hello.Main`. The DAG is `setxcom_fix`.
The run script makes the host-mounted log directory writable by the
image user (uid 50000).

**Expected:** the DAG ends in **success**. All eight tasks succeed.

| task | expected XCom `return_value` |
|---|---|
| `extract` | `42` |
| `score` | `{"region": "eu", "threshold": 0.5}` |
| `push_as_java` | `{"region": "eu", "threshold": 0.5}` |
| `push_java_pojo` | `{"region": "eu", "threshold": 0.5}` |
| `push_scala_map` | bean garbage: `scala$collection$immutable$Map$Map2$$key1/value1/key2/value2`, plus `empty` / `traversableAgain` |
| `push_scala_seq` | `{"empty": false, "traversableAgain": true}` |
| `push_scala_option` | `{"empty": false, "defined": true}` |
| `push_case_class` | `{}` |

The Scala tasks do **not** fail on an unpatched 1.0.0 SDK. That silent store is
the bug.

## 6. Run the patched SDK

```bash
make patched
```

Same DAG and the same application jars; only the SDK jar is the overlay from
step 4.

**Expected:** the DAG ends in **failed**. The four control tasks succeed with
the **same** XCom values as the baseline (`42` and
`{"region": "eu", "threshold": 0.5}`). The four Scala tasks fail and write
**no** XCom.

The task log error is:

```
java.lang.IllegalArgumentException: Unsupported XCom value type: <type>. Scala collections, Option and case classes cannot be stored as XComs directly. Convert a Scala collection with scala.jdk.CollectionConverters (.asJava), and convert a case class or Option to a java.util.Map or list.
```

| task | `<type>` in the message |
|---|---|
| `push_scala_map` | `scala.collection.immutable.Map$Map2` |
| `push_scala_seq` | `scala.collection.immutable.$colon$colon` |
| `push_scala_option` | `scala.Some` |
| `push_case_class` | `com.mycompany.airflow.hello.ScoreSummary` |

`ScoreSummary` is a user case class (not a `scala.*` name). It is still
rejected because it implements `scala.Product`.

## 7. Read the evidence

```bash
make evidence
```

Then open:

- `evidence/EVIDENCE.md` — run ids, Asia/Tbilisi timestamps, per-task state and XCom
- `evidence/baseline-xcom.json` and `evidence/patched-xcom.json`
- `evidence/patched-errors.json` — exact exception text
- `evidence/log-excerpts/` — the `IllegalArgumentException` lines from the four failed tasks
- `evidence/logs-baseline/` and `evidence/logs-patched/` — written locally when you re-run (not in git)

To run steps 1–7 in one go: `make all` (or `./run.sh all`).

## 8. Java SDK unit tests (the fix itself)

From the same `$AIRFLOW_SRC` checkout (the setXCom patch; the Scala docs patch
may also be applied — the two patches do not conflict):

```bash
cd "$AIRFLOW_SRC/java-sdk"
./gradlew :sdk:check
```

or, from this directory: `make sdk-check`.

**Expected:** 426 tests, 0 failures, including `JsonValueTest` and the updated
`CommTest`, plus ktlint and spotless.

## What the DAG does

Eight independent `@task.stub(queue="java")` tasks (no `>>`), so a rejection
does not skip the controls.

| task | value pushed |
|---|---|
| `extract` | boxed long `42` |
| `score` | `Map("region" -> input.region, "threshold" -> input.threshold).asJava` |
| `push_as_java` | the same map via `.asJava` |
| `push_java_pojo` | Java bean `ScoreSummaryBean` with `getRegion` / `getThreshold` |
| `push_scala_map` | Scala `Map("region" -> "eu", "threshold" -> 0.5)` |
| `push_scala_seq` | Scala `Seq(1, 2, 3)` |
| `push_scala_option` | `Some("eu")` |
| `push_case_class` | `ScoreSummary("eu", 0.5)` |

## Layout

```
Dockerfile          apache/airflow:3.4.0-python3.12 + Temurin 17 JRE
dags/               Python stub DAG
scala-src/          sbt thin-bundle project (handlers + Java TaskInput / POJO)
scripts/            bundle, overlay, run, and evidence helpers
Makefile            image, bundle, overlay, baseline, patched, evidence, sdk-check
patches/            setXCom loud-failure and Scala docs patches (apply on 9eb25f92)
evidence/           captured results (EVIDENCE.md, JSON, log excerpts)
```

`make clean` removes built bundles, containers, and evidence dumps.

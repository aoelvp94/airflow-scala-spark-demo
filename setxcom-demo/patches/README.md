# Patches

Both patches apply on Apache Airflow `main` at `9c93fe8bbf` (27 Nov 2026), alone or
together in either order:

- `setxcom-loud-failure.patch`: the fix under review in
  [apache/airflow#77249](https://github.com/apache/airflow/pull/77249). It changes
  `execution/Frame.kt`, adds `internal/JacksonModules.kt` (also used by
  `ConfBinding` and `ArgValues`), the `Client` / `TaskStateStore` KDoc and the
  Java SDK guide, and adds `FrameTest`, `JacksonModulesTest`, `CommTest` cases,
  a test-only Jackson module and a Java test type.
- `scala-docs.patch` (optional, docs only): a "Scala" section for the Java SDK
  guide and a note in `java-sdk/scala_spark_example/README.md`.

```bash
cd /path/to/apache/airflow
git fetch upstream
git switch --detach 9c93fe8bbf
git apply /path/to/airflow-scala-spark-demo/setxcom-demo/patches/setxcom-loud-failure.patch
# optional, docs only:
git apply /path/to/airflow-scala-spark-demo/setxcom-demo/patches/scala-docs.patch
export AIRFLOW_SRC="$PWD"
```

`git apply` refuses new files that already exist, so start from a clean tree.

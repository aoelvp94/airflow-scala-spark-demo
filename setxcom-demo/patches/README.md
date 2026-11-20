# Patches

Both patches apply on Apache Airflow `main` at `943c8ffceb` (20 Nov 2026), alone or
together in either order:

- `setxcom-loud-failure.patch`: the fix under review in
  [apache/airflow#77249](https://github.com/apache/airflow/pull/77249). It changes
  `execution/Frame.kt`, the `Client` / `TaskStateStore` KDoc and the Java SDK
  guide, and adds `FrameTest`, `CommTest` cases and a test-only Jackson module.
- `scala-docs.patch` (optional, docs only): a "Scala" section for the Java SDK
  guide and a note in `java-sdk/scala_spark_example/README.md`.

```bash
cd /path/to/apache/airflow
git fetch upstream
git switch --detach 943c8ffceb
git apply /path/to/airflow-scala-spark-demo/setxcom-demo/patches/setxcom-loud-failure.patch
# optional, docs only:
git apply /path/to/airflow-scala-spark-demo/setxcom-demo/patches/scala-docs.patch
export AIRFLOW_SRC="$PWD"
```

`git apply` refuses new files that already exist, so start from a clean tree.

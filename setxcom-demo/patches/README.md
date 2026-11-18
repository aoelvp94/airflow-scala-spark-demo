# Patches

These apply on Apache Airflow `main` at `9eb25f92` (18 Nov 2026). They also apply
together, in either order, after a clean tree (`git apply` of new files will
refuse if those files already exist).

```bash
cd /path/to/apache/airflow
git fetch origin
git switch --detach 9eb25f92   # or the PR branch, once it exists
git apply /path/to/airflow-scala-spark-demo/setxcom-demo/patches/setxcom-loud-failure.patch
# optional, docs only:
git apply /path/to/airflow-scala-spark-demo/setxcom-demo/patches/scala-docs.patch
export AIRFLOW_SRC="$PWD"
```

Upstream PRs have not been opened yet. Replace this note with the PR links
when they exist.

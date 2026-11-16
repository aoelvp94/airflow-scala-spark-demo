from __future__ import annotations

from airflow.sdk import dag, task


@task.stub(queue="scala")
def ingest(): ...


@task.stub(queue="scala")
def transform(): ...


@dag(
    schedule=None,
    catchup=False,
    tags=["scala", "spark", "daria", "ingest", "stub"],
)
def scala_spark_ingest_transform():
    ingest() >> transform()


scala_spark_ingest_transform()

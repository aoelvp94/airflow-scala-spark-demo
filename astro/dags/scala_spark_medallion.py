from __future__ import annotations

from airflow.sdk import dag, task


@task.stub(queue="scala")
def ingest(): ...


@task.stub(queue="scala")
def bronze_to_silver(): ...


@task.stub(queue="scala")
def silver_to_gold(): ...


@dag(
    schedule=None,
    catchup=False,
    tags=["scala", "spark", "daria", "medallion", "stub"],
)
def scala_spark_medallion():
    ingest() >> bronze_to_silver() >> silver_to_gold()


scala_spark_medallion()

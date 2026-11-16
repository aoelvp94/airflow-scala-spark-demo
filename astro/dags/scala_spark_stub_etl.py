from __future__ import annotations

from airflow.sdk import dag, task


@task.stub(queue="scala")
def extract(): ...


@task.stub(queue="scala")
def aggregate(): ...


@task.stub(queue="scala")
def report(): ...


@dag(schedule=None, catchup=False, tags=["scala", "spark", "stub"])
def scala_spark_stub_etl():
    extract() >> aggregate() >> report()


scala_spark_stub_etl()

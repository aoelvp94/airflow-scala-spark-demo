from __future__ import annotations

from airflow.sdk import dag, task


@task.stub(queue="scala")
def daria(): ...


@task.stub(queue="scala")
def plain(): ...


@dag(schedule=None, catchup=False, tags=["scala", "spark", "daria", "stub"])
def scala_spark_daria():
    daria()
    plain()


scala_spark_daria()

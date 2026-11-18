from __future__ import annotations

from airflow.sdk import dag, task


@task.stub(queue="java")
def extract(): ...


@task.stub(queue="java")
def score(region: str, threshold: float): ...


@task.stub(queue="java")
def push_as_java(): ...


@task.stub(queue="java")
def push_java_pojo(): ...


@task.stub(queue="java")
def push_scala_map(): ...


@task.stub(queue="java")
def push_scala_seq(): ...


@task.stub(queue="java")
def push_scala_option(): ...


@task.stub(queue="java")
def push_case_class(): ...


@dag(dag_id="setxcom_fix", schedule=None, catchup=False, tags=["scala", "setxcom-fix-e2e"])
def setxcom_fix():
    # Independent tasks so a rejection does not skip the controls.
    extract()
    score(region="eu", threshold=0.5)
    push_as_java()
    push_java_pojo()
    push_scala_map()
    push_scala_seq()
    push_scala_option()
    push_case_class()


setxcom_fix()

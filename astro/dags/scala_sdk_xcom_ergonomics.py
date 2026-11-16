from __future__ import annotations

from airflow.sdk import dag, task

def scala_stub(task_id: str, **kwargs):
    def body(): ...

    body.__name__ = task_id
    return task.stub(task_id=task_id, queue="scala", **kwargs)(body)()


PUSHES = [
    "push_scala_map",
    "push_scala_seq",
    "push_case_class",
    "push_scala_option",
    "push_java_map",
    "push_big_decimal",
]


@dag(schedule=None, catchup=False, tags=["scala", "xcom"])
def scala_sdk_xcom_ergonomics():
    read = scala_stub("read_xcoms", trigger_rule="all_done")
    for task_id in PUSHES:
        scala_stub(task_id) >> read


scala_sdk_xcom_ergonomics()

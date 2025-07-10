"""dependencies_demo.py – Demonstrate dependency wiring patterns."""

from __future__ import annotations

from datetime import datetime, timedelta

from airflow import DAG
from airflow.models.baseoperator import chain
from airflow.operators.empty import EmptyOperator
from airflow.utils.task_group import TaskGroup

# ---------------------------------------------------------------------------
# DAG definition
# ---------------------------------------------------------------------------

default_args = {
    "retries": 1,
    "retry_delay": timedelta(minutes=3),
}

dag = DAG(
    dag_id="dependencies_demo",
    schedule_interval=None,  # manual trigger
    start_date=datetime(2025, 7, 1),
    catchup=False,
    default_args=default_args,
    tags=["exam", "dependencies"],
)

start = EmptyOperator(task_id="start", dag=dag)

# exam-fact: TaskGroup groups related tasks in the UI
with TaskGroup(group_id="mini_etl", dag=dag) as mini_etl:
    extract_a = EmptyOperator(task_id="extract_a")
    extract_b = EmptyOperator(task_id="extract_b")
    merge = EmptyOperator(task_id="merge")

    # exam-fact: Fan-out / fan-in using list dependencies and bitshift syntax
    [extract_a, extract_b] >> merge

transform = EmptyOperator(task_id="transform", dag=dag)
load = EmptyOperator(task_id="load", dag=dag)

# exam-fact: List dependency from one task to many
start >> [extract_a, extract_b]

# exam-fact: chain helper creates a linear dependency
chain(merge, transform, load)

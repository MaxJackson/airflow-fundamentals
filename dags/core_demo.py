from __future__ import annotations

"""
core_demo.py – Showcase DAG for the *Airflow Fundamentals* exam topics
=====================================================================
This DAG orchestrates a tiny crypto‑price pipeline and demonstrates:
* HTTP → S3 (MinIO) → Postgres ETL (Topic 1: Use‑cases)
* Dependency syntax (`>>`, lists, TaskGroup) (Topic 3)
* Branching & conditional logic (Topic 13)
* XCom push/pull & XComArg (Topic 9)
* Scheduling, retries, SLAs (Topic 6/8)
* Connections/Variables best‑practice (Topic 12/15)
Each fact needed for the exam is marked with `# exam‑fact:` comments.

The code is intentionally lightweight so reviewers can spin it up with the
`docker‑compose.yaml` in <10 minutes. Replace connection IDs or bucket names
as appropriate for your environment.
"""

from datetime import datetime, timedelta
import io
import json
import os
from typing import Any, Dict

import boto3
import pandas as pd
from airflow import DAG
from airflow.models.baseoperator import chain
from airflow.providers.http.operators.http import SimpleHttpOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.providers.slack.operators.slack_webhook import SlackWebhookOperator
from airflow.operators.python import BranchPythonOperator, PythonOperator
from airflow.utils.task_group import TaskGroup
from airflow.utils.trigger_rule import TriggerRule

# ---------------------------------------------------------------------------
# DAG definition
# ---------------------------------------------------------------------------

default_args = {
    "owner": "max",  # exam‑fact: owner metadata is optional but useful
    "retries": 2,  # exam‑fact: default_args propagate to tasks unless overridden
    "retry_delay": timedelta(minutes=5),
    "sla": timedelta(hours=2),  # exam‑fact: tasks missing SLA → email/slack callback
}

dag = DAG(
    dag_id="core_crypto_etl",
    description="Fetch BTC price hourly → store raw JSON → convert to Parquet → load Postgres",
    schedule_interval="0 * * * *",  # exam‑fact: every hour at minute 0
    start_date=datetime(2025, 7, 1),
    catchup=False,  # exam‑fact: disable back‑fill for demo
    max_active_runs=1,
    default_args=default_args,
    tags=["exam", "etl", "crypto"],
)

# ---------------------------------------------------------------------------
# Environment helpers – edit for your stack
# ---------------------------------------------------------------------------

S3_ENDPOINT = os.getenv("MINIO_ENDPOINT", "http://minio:9000")
S3_BUCKET = os.getenv("MINIO_BUCKET", "crypto-raw")
PG_TABLE = os.getenv("PG_TABLE", "public.btc_prices")

# ---------------------------------------------------------------------------
# Task definitions
# ---------------------------------------------------------------------------

fetch_prices = SimpleHttpOperator(
    task_id="fetch_prices",
    http_conn_id="http_default",  # exam‑fact: defined via Airflow UI/CLI/env
    endpoint="/v1/bpi/currentprice/BTC.json",
    method="GET",
    log_response=True,
    do_xcom_push=True,  # exam‑fact: pushes response text to XCom
    dag=dag,
)


def _save_raw(ti, **_) -> str:
    """Write raw JSON payload from XCom to S3 (MinIO). Returns the S3 key."""
    payload = ti.xcom_pull(task_ids="fetch_prices")  # exam‑fact: pull via task id
    ts_iso: str = ti.execution_date.isoformat()
    key = f"raw/{ts_iso}.json"

    s3 = boto3.resource(
        "s3",
        endpoint_url=S3_ENDPOINT,
        aws_access_key_id=os.getenv("MINIO_ROOT_USER", "minioadmin"),
        aws_secret_access_key=os.getenv("MINIO_ROOT_PASSWORD", "minioadmin"),
    )
    bucket = s3.Bucket(S3_BUCKET)
    bucket.put_object(Key=key, Body=payload.encode())

    return key  # → pushed automatically when returned (xcom_push=True by default)


save_raw = PythonOperator(
    task_id="save_raw",
    python_callable=_save_raw,
    provide_context=True,
    dag=dag,
)


with TaskGroup(group_id="transform", dag=dag) as transform_group:  # exam‑fact: TaskGroup UI

    def _convert_to_parquet(ti, **_) -> str:
        """Read JSON payload from XCom S3 key → Parquet → returns Parquet key."""
        key = ti.xcom_pull(task_ids="save_raw")
        s3 = boto3.resource(
            "s3",
            endpoint_url=S3_ENDPOINT,
            aws_access_key_id=os.getenv("MINIO_ROOT_USER", "minioadmin"),
            aws_secret_access_key=os.getenv("MINIO_ROOT_PASSWORD", "minioadmin"),
        )
        obj = s3.Object(S3_BUCKET, key)
        raw_bytes = obj.get()["Body"].read()
        data = json.loads(raw_bytes)
        price = float(data["bpi"]["USD"]["rate_float"])  # type: ignore[index]
        df = pd.DataFrame(
            [{"ts": ti.execution_date, "price_usd": price}],
        )
        parquet_key = key.replace("raw/", "curated/").replace(".json", ".parquet")
        buffer = io.BytesIO()
        df.to_parquet(buffer, index=False)
        buffer.seek(0)
        s3.Object(S3_BUCKET, parquet_key).put(Body=buffer.getvalue())
        return parquet_key

    convert_parquet = PythonOperator(
        task_id="convert_parquet",
        python_callable=_convert_to_parquet,
        provide_context=True,
    )

    load_postgres = PostgresOperator(
        task_id="load_postgres",
        postgres_conn_id="postgres_default",
        sql=f"""
            CREATE TABLE IF NOT EXISTS {PG_TABLE} (
                ts TIMESTAMP PRIMARY KEY,
                price_usd NUMERIC
            );

            COPY {PG_TABLE}(ts, price_usd)
            FROM PROGRAM 'aws s3 cp {S3_ENDPOINT}/{S3_BUCKET}/{{ ti.xcom_pull(task_ids="convert_parquet") }} -'
            WITH (FORMAT PARQUET);
        """,
    )

    chain(convert_parquet, load_postgres)  # tidy linear dependency inside group


def _is_weekend(execution_date, **_) -> str:
    """Branch on weekday vs weekend."""
    return "notify_slack" if execution_date.weekday() >= 5 else "no_notify"


branch_is_weekend = BranchPythonOperator(
    task_id="branch_is_weekend",
    python_callable=_is_weekend,
    dag=dag,
)

notify_slack = SlackWebhookOperator(
    task_id="notify_slack",
    http_conn_id="slack_webhook_default",
    message="BTC price pipeline succeeded for {{ ds }}.",
    trigger_rule=TriggerRule.NONE_FAILED_MIN_ONE_SUCCESS,  # ensure downstream even if skipped
    dag=dag,
)

no_notify = PythonOperator(
    task_id="no_notify",
    python_callable=lambda: print("Weekday run – no Slack message."),
    dag=dag,
)

# ---------------------------------------------------------------------------
# DAG dependencies
# ---------------------------------------------------------------------------

fetch_prices >> save_raw >> transform_group >> branch_is_weekend >> [notify_slack, no_notify]

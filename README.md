# Airflow Fundamentals

This repository contains two simple DAGs that showcase core Airflow concepts used throughout the [Airflow Fundamentals](https://airflow.apache.org/) learning materials. The code is intentionally lightweight so you can spin it up quickly with the provided `docker-compose.yaml` or your own Airflow instance.

## Features demonstrated

* **Operators**
  * `SimpleHttpOperator` to retrieve BTC price data
  * `PythonOperator` for custom Python logic (saving to S3 and converting to Parquet)
  * `PostgresOperator` to load results into Postgres
  * `SlackWebhookOperator` for optional notifications
  * `BranchPythonOperator` to run conditional logic
* **Task grouping** with `TaskGroup` to keep related tasks organised in the UI
* **Dependencies** using the bit‑shift (`>>`) syntax, lists and the `chain()` helper
* **XCom** push/pull and `xcom_return` values for passing data between tasks
* **Scheduling** via cron expressions, start dates and `catchup=False`
* **Retries and SLAs** configured through `default_args`
* **Connections/variables** referenced via IDs and environment variables

The `dependencies_demo` DAG contains small tasks showing how to express fan‑out/fan‑in patterns using lists and `chain()`.

Use these examples as a reference when studying for the exam or exploring how these Airflow features fit together in a real DAG.

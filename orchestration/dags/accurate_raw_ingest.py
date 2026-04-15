from __future__ import annotations

from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

from orchestration.scripts.ingest_raw_data import (
    ingest_customer,
    ingest_customer_category,
    ingest_invoice,
    ingest_item,
    ingest_item_category,
    ingest_warehouse,
)
from orchestration.scripts.load_db import load_dimensions_to_db, load_invoice_to_db
from orchestration.scripts.refresh_token import refresh_token

default_args = {
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="accurate_raw_ingest",
    start_date=datetime(2026, 2, 6),
    schedule_interval="@weekly",
    catchup=False,
    max_active_runs=1,
    default_args=default_args,
    tags=["accurate", "raw", "ingestion"],
) as dag:
    # We choose parallel dimension ingest topology so independent dimension API pulls
    # run concurrently after one token refresh, reducing end-to-end runtime.
    interval_kwargs = {
        "data_interval_start": "{{ data_interval_start }}",
        "data_interval_end": "{{ data_interval_end }}",
    }

    refresh_token_task = PythonOperator(
        task_id="refresh_token",
        python_callable=refresh_token,
    )

    ingest_customer_category_task = PythonOperator(
        task_id="ingest_customer_category",
        python_callable=ingest_customer_category,
        op_kwargs=interval_kwargs,
    )

    ingest_customer_task = PythonOperator(
        task_id="ingest_customer",
        python_callable=ingest_customer,
        op_kwargs=interval_kwargs,
    )

    ingest_warehouse_task = PythonOperator(
        task_id="ingest_warehouse",
        python_callable=ingest_warehouse,
        op_kwargs=interval_kwargs,
    )

    ingest_item_category_task = PythonOperator(
        task_id="ingest_item_category",
        python_callable=ingest_item_category,
        op_kwargs=interval_kwargs,
    )

    ingest_item_task = PythonOperator(
        task_id="ingest_item",
        python_callable=ingest_item,
        op_kwargs=interval_kwargs,
    )

    load_dimensions_to_db_task = PythonOperator(
        task_id="load_dimensions_to_db",
        python_callable=load_dimensions_to_db,
        op_kwargs=interval_kwargs,
    )

    ingest_invoice_task = PythonOperator(
        task_id="ingest_invoice",
        python_callable=ingest_invoice,
        op_kwargs=interval_kwargs,
    )

    load_invoice_to_db_task = PythonOperator(
        task_id="load_invoice_to_db",
        python_callable=load_invoice_to_db,
        op_kwargs=interval_kwargs,
    )

    dbt_run_task = BashOperator(
        task_id="dbt_run",
        bash_command="dbt run --project-dir /opt/project/dbt --profiles-dir /opt/project/dbt",
    )

    refresh_token_task >> [
        ingest_customer_category_task,
        ingest_customer_task,
        ingest_warehouse_task,
        ingest_item_category_task,
        ingest_item_task,
    ] >> load_dimensions_to_db_task

    refresh_token_task >> ingest_invoice_task >> load_invoice_to_db_task

    [load_dimensions_to_db_task, load_invoice_to_db_task] >> dbt_run_task

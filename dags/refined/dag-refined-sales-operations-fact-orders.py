import os
import sys
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator

_SRC_DIR = os.environ.get("PIPELINE_SRC_DIR", "/opt/pipeline/src")
if _SRC_DIR not in sys.path:
    sys.path.insert(0, _SRC_DIR)

from gold.sales_operations.fact_orders import build as build_fn


default_args = {
    "owner": "data-engineering",
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
    "email_on_failure": False,
}

with DAG(
    dag_id="dag-refined-sales-operations-fact-orders",
    default_args=default_args,
    description="",
    schedule_interval=None,
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["sales_operations", "gold", "orders"],
) as dag:


    build = PythonOperator(
        task_id="build_fact_orders",
        python_callable=build_fn,
        op_kwargs={"run_date": "{{ ds }}"},
    )

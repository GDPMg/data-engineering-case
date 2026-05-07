import os
import sys
from datetime import datetime, timedelta

from airflow import DAG
from airflow.models.param import Param
from airflow.operators.python import PythonOperator, ShortCircuitOperator

_DAGS_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
_SRC_DIR = os.environ.get("PIPELINE_SRC_DIR", "/opt/pipeline/src")
for _p in [_SRC_DIR, _DAGS_DIR]:
    if _p not in sys.path:
        sys.path.insert(0, _p)

from dag_utils.file_checks import check_entity_input
from bronze.sales_operations.orders import ingest as bronze_orders_fn
from silver.sales_operations.orders import process as silver_orders_fn


default_args = {
    "owner": "data-engineering",
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
    "email_on_failure": False,
}

with DAG(
    dag_id="dag-trusted-sales-operations-orders",
    default_args=default_args,
    description="",
    schedule_interval=None,
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["sales_operations", "orders", "medallion"],
    params={
        "run_date": Param(
            default=datetime.now().strftime("%Y-%m-%d"),
            type="string",
            description="Data a processar no formato YYYY-MM-DD (ex: 2026-05-07).",
        )
    },
) as dag:

    check_inputs = ShortCircuitOperator(
        task_id="check_input_files",
        python_callable=check_entity_input,
        op_kwargs={"domain": "sales_operations", "entity": "orders", "run_date": "{{ params.run_date }}"},
    )

    bronze_orders = PythonOperator(
        task_id="bronze_orders",
        python_callable=bronze_orders_fn,
        op_kwargs={"run_date": "{{ params.run_date }}"},
    )

    silver_orders = PythonOperator(
        task_id="silver_orders",
        python_callable=silver_orders_fn,
        op_kwargs={"run_date": "{{ params.run_date }}"},
    )

    check_inputs >> bronze_orders >> silver_orders

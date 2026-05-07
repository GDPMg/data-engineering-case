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

from dag_utils.file_checks import check_entity_silver
from gold.sales_operations.agg_customer_metrics import build as build_fn


default_args = {
    "owner": "data-engineering",
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
    "email_on_failure": False,
}

with DAG(
    dag_id="dag-refined-sales-operations-agg-customer-metrics",
    default_args=default_args,
    description="",
    schedule_interval=None,
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["sales_operations", "gold", "customers"],
    params={
        "run_date": Param(
            default=datetime.now().strftime("%Y-%m-%d"),
            type="string"
        )
    },
) as dag:

    check_silver = ShortCircuitOperator(
        task_id="check_silver_files",
        python_callable=check_entity_silver,
        op_kwargs={"domain": "sales_operations", "entity": "orders", "run_date": "{{ params.run_date }}"},
    )

    build = PythonOperator(
        task_id="build_agg_customer_metrics",
        python_callable=build_fn,
        op_kwargs={"run_date": "{{ params.run_date }}"},
    )

    check_silver >> build

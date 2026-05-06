import glob
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

from bronze.sales_operations.orders import ingest as bronze_orders_fn
from silver.sales_operations.orders import process as silver_orders_fn

DOMAIN = "sales_operations"
ENTITY = "orders"

default_args = {
    "owner": "data-engineering",
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
    "email_on_failure": False,
}


def _get_run_dates(**context):
    params = context["params"]
    if params.get("full_history"):
        base = os.environ.get("PIPELINE_BASE_DIR", "/opt/pipeline")
        input_base = os.path.join(base, "data", "input", DOMAIN, ENTITY)
        return sorted([
            d for d in os.listdir(input_base)
            if os.path.isdir(os.path.join(input_base, d))
            and glob.glob(os.path.join(input_base, d, "*.csv"))
        ])
    return [params.get("run_date") or context["ds"]]


def _check_inputs(**context):
    return bool(_get_run_dates(**context))


def _run_bronze(**context):
    for run_date in _get_run_dates(**context):
        bronze_orders_fn(run_date=run_date)


def _run_silver(**context):
    for run_date in _get_run_dates(**context):
        silver_orders_fn(run_date=run_date)


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
            default="",
            type="string",
            description="Specific date to process (YYYY-MM-DD). Leave empty to use DAG execution date.",
        ),
        "full_history": Param(
            default=False,
            type="boolean",
            description="If True, process all available dates ignoring run_date.",
        ),
    },
) as dag:

    check_inputs = ShortCircuitOperator(
        task_id="check_input_files",
        python_callable=_check_inputs,
    )

    bronze_orders = PythonOperator(
        task_id="bronze_orders",
        python_callable=_run_bronze,
    )

    silver_orders = PythonOperator(
        task_id="silver_orders",
        python_callable=_run_silver,
    )

    check_inputs >> bronze_orders >> silver_orders

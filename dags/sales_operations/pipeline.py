import os
import sys
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator, ShortCircuitOperator
from airflow.utils.task_group import TaskGroup

# dags/ folder → dag_utils imports
# src/ folder  → bronze / silver / gold imports
_DAGS_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
_SRC_DIR = os.environ.get("PIPELINE_SRC_DIR", "/opt/pipeline/src")
for _p in [_SRC_DIR, _DAGS_DIR]:
    if _p not in sys.path:
        sys.path.insert(0, _p)

from dag_utils.file_checks import check_all_inputs
from bronze.sales_operations.customers import ingest as bronze_customers_fn
from bronze.sales_operations.orders import ingest as bronze_orders_fn
from silver.sales_operations.customers import process as silver_customers_fn
from silver.sales_operations.orders import process as silver_orders_fn
from gold.sales_operations.dim_customers import build as gold_dim_customers_fn
from gold.sales_operations.agg_customer_metrics import build as gold_agg_customer_metrics_fn
from gold.sales_operations.fact_orders import build as gold_fact_orders_fn
from gold.sales_operations.agg_orders_monthly import build as gold_agg_orders_monthly_fn

default_args = {
    "owner": "data-engineering",
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
    "email_on_failure": False,
}

with DAG(
    dag_id="sales_operations_pipeline",
    default_args=default_args,
    description="Full medallion pipeline for sales_operations: input → bronze → silver → gold",
    schedule_interval="@daily",
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["sales_operations", "medallion"],
) as dag:

    # ── Gate ────────────────────────────────────────────────────────────────
    # Verifies that input files for today exist before spending any resources.
    # If missing, all downstream tasks are skipped (not failed).
    check_inputs = ShortCircuitOperator(
        task_id="check_input_files",
        python_callable=check_all_inputs,
        op_kwargs={"domain": "sales_operations", "run_date": "{{ ds }}"},
    )

    # ── Bronze ───────────────────────────────────────────────────────────────
    with TaskGroup("bronze") as bronze_group:
        bronze_customers = PythonOperator(
            task_id="customers",
            python_callable=bronze_customers_fn,
            op_kwargs={"run_date": "{{ ds }}"},
        )
        bronze_orders = PythonOperator(
            task_id="orders",
            python_callable=bronze_orders_fn,
            op_kwargs={"run_date": "{{ ds }}"},
        )

    # ── Silver ───────────────────────────────────────────────────────────────
    with TaskGroup("silver") as silver_group:
        silver_customers = PythonOperator(
            task_id="customers",
            python_callable=silver_customers_fn,
            op_kwargs={"run_date": "{{ ds }}"},
        )
        silver_orders = PythonOperator(
            task_id="orders",
            python_callable=silver_orders_fn,
            op_kwargs={"run_date": "{{ ds }}"},
        )
        # Orders silver validates customer_id against silver customers
        silver_customers >> silver_orders

    # ── Gold ─────────────────────────────────────────────────────────────────
    with TaskGroup("gold") as gold_group:
        dim_customers = PythonOperator(
            task_id="dim_customers",
            python_callable=gold_dim_customers_fn,
            op_kwargs={"run_date": "{{ ds }}"},
        )
        fact_orders = PythonOperator(
            task_id="fact_orders",
            python_callable=gold_fact_orders_fn,
            op_kwargs={"run_date": "{{ ds }}"},
        )
        agg_customer_metrics = PythonOperator(
            task_id="agg_customer_metrics",
            python_callable=gold_agg_customer_metrics_fn,
            op_kwargs={"run_date": "{{ ds }}"},
        )
        agg_orders_monthly = PythonOperator(
            task_id="agg_orders_monthly",
            python_callable=gold_agg_orders_monthly_fn,
            op_kwargs={"run_date": "{{ ds }}"},
        )
        # dim and fact are base tables; agg tables are derived from them
        dim_customers >> agg_customer_metrics
        fact_orders >> agg_orders_monthly

    # ── Dependencies ─────────────────────────────────────────────────────────
    check_inputs >> [bronze_customers, bronze_orders]
    bronze_customers >> silver_customers
    bronze_orders >> silver_orders
    [silver_customers, silver_orders] >> dim_customers
    [silver_customers, silver_orders] >> fact_orders

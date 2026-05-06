import os
import sys
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator

sys.path.insert(0, os.environ.get("PIPELINE_SRC_DIR", "/opt/pipeline/src"))

from gold.sales_operations.dim_customers import build as build_dim_customers
from gold.sales_operations.agg_customer_metrics import build as build_agg_customer_metrics
from gold.sales_operations.fact_orders import build as build_fact_orders
from gold.sales_operations.agg_orders_monthly import build as build_agg_orders_monthly

default_args = {
    "owner": "data-engineering",
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
    "email_on_failure": False,
}

with DAG(
    dag_id="gold_pipeline",
    default_args=default_args,
    description="Build analytical tables (dim/fact/agg) from silver: silver → gold",
    schedule_interval="@daily",
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["gold", "sales_operations", "medallion", "analytics"],
) as dag:

    dim_customers = PythonOperator(
        task_id="gold_dim_customers",
        python_callable=build_dim_customers,
        op_kwargs={"run_date": "{{ ds }}"},
    )

    agg_customer_metrics = PythonOperator(
        task_id="gold_agg_customer_metrics",
        python_callable=build_agg_customer_metrics,
        op_kwargs={"run_date": "{{ ds }}"},
    )

    fact_orders = PythonOperator(
        task_id="gold_fact_orders",
        python_callable=build_fact_orders,
        op_kwargs={"run_date": "{{ ds }}"},
    )

    agg_orders_monthly = PythonOperator(
        task_id="gold_agg_orders_monthly",
        python_callable=build_agg_orders_monthly,
        op_kwargs={"run_date": "{{ ds }}"},
    )

    dim_customers >> agg_customer_metrics
    fact_orders >> agg_orders_monthly

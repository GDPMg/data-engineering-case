import os
import sys
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.sensors.external_task import ExternalTaskSensor

sys.path.insert(0, os.environ.get("PIPELINE_SRC_DIR", "/opt/pipeline/src"))

from gold.sales_operations.customers import build as build_customers_gold
from gold.sales_operations.orders import build as build_orders_gold

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

    gold_customers = PythonOperator(
        task_id="gold_build_customers",
        python_callable=build_customers_gold,
        op_kwargs={"run_date": "{{ ds }}"},
    )

    gold_orders = PythonOperator(
        task_id="gold_build_orders",
        python_callable=build_orders_gold,
        op_kwargs={"run_date": "{{ ds }}"},
    )

    # Customers gold depends on both silver layers (agg_customer_metrics needs orders)
    gold_customers >> gold_orders

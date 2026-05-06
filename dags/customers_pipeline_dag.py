import os
import sys
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator

sys.path.insert(0, os.environ.get("PIPELINE_SRC_DIR", "/opt/pipeline/src"))

from bronze.sales_operations.customers import ingest as bronze_ingest
from silver.sales_operations.customers import process as silver_process

default_args = {
    "owner": "data-engineering",
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
    "email_on_failure": False,
}

with DAG(
    dag_id="customers_pipeline",
    default_args=default_args,
    description="Ingest and clean customer data: input → bronze → silver",
    schedule_interval="@daily",
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["customers", "sales_operations", "medallion"],
) as dag:

    bronze = PythonOperator(
        task_id="bronze_ingest_customers",
        python_callable=bronze_ingest,
        op_kwargs={"run_date": "{{ ds }}"},
    )

    silver = PythonOperator(
        task_id="silver_process_customers",
        python_callable=silver_process,
        op_kwargs={"run_date": "{{ ds }}"},
    )

    bronze >> silver

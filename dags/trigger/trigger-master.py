from datetime import datetime

from airflow import DAG
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.utils.task_group import TaskGroup
from airflow.models import Variable

SCHEDULE_INTERVAL = Variable.get("SCHEDULE_INTERVAL_DAILY", default_var="0 6 * * *")

default_args = {
    "owner": "data-engineering",
    "retries": 0,
    "email_on_failure": False,
}

with DAG(
    dag_id="trigger-master",
    default_args=default_args,
    description="",
    schedule_interval=SCHEDULE_INTERVAL,
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["trigger", "master"],
) as dag:

    with TaskGroup("Trusted") as Trusted:
        trigger_customers = TriggerDagRunOperator(
            task_id="trigger_customers_pipeline",
            trigger_dag_id="dag-trusted-sales-operations-customers",
            wait_for_completion=True,
            reset_dag_run=True,
            poke_interval=30,
        )
        trigger_orders = TriggerDagRunOperator(
            task_id="trigger_orders_pipeline",
            trigger_dag_id="dag-trusted-sales-operations-orders",
            wait_for_completion=True,
            reset_dag_run=True,
            poke_interval=30,
        )

        trigger_customers >> trigger_orders


    with TaskGroup("Refined") as Refined:
        trigger_dim_customers = TriggerDagRunOperator(
            task_id="trigger_gold_dim_customers",
            trigger_dag_id="dag-refined-sales-operations-dim-customers",
            wait_for_completion=True,
            reset_dag_run=True,
            poke_interval=30,
        )
        trigger_agg_customer_metrics = TriggerDagRunOperator(
            task_id="trigger_gold_agg_customer_metrics",
            trigger_dag_id="dag-refined-sales-operations-agg-customer-metrics",
            wait_for_completion=True,
            reset_dag_run=True,
            poke_interval=30,
        )
        trigger_fact_orders = TriggerDagRunOperator(
            task_id="trigger_gold_fact_orders",
            trigger_dag_id="dag-refined-sales-operations-fact-orders",
            wait_for_completion=True,
            reset_dag_run=True,
            poke_interval=30,
        )
        trigger_agg_orders_monthly = TriggerDagRunOperator(
            task_id="trigger_gold_agg_orders_monthly",
            trigger_dag_id="dag-refined-sales-operations-agg-orders-monthly",
            wait_for_completion=True,
            reset_dag_run=True,
            poke_interval=30,
        )

        trigger_dim_customers >> trigger_agg_customer_metrics
        trigger_fact_orders >> trigger_agg_orders_monthly

    Trusted >> Refined

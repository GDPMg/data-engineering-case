import glob
import os
from datetime import date, datetime, timezone

import pandas as pd

from utils.logger import get_logger
from utils.paths import ensure_dir, get_gold_path, get_silver_path

logger = get_logger("gold.customers")

DOMAIN = "sales_operations"
ENTITY = "customers"


def build(run_date: str) -> None:
    silver_dir = get_silver_path(DOMAIN, ENTITY, run_date)
    files = glob.glob(os.path.join(silver_dir, "*.csv"))
    if not files:
        raise FileNotFoundError(f"No silver customers file in {silver_dir}")

    df = pd.concat([pd.read_csv(f, dtype=str) for f in files], ignore_index=True)
    df["created_at"] = pd.to_datetime(df["created_at"], errors="coerce")

    # dim_customers: purely descriptive, zero PII
    dim = df[["customer_id", "city", "state", "status", "created_at"]].copy()
    today = pd.Timestamp(date.today())
    dim["days_since_registration"] = (today - dim["created_at"]).dt.days
    dim["updated_at"] = datetime.now(timezone.utc).isoformat()

    out_dir = ensure_dir(get_gold_path(DOMAIN, ENTITY, run_date))
    dim.to_csv(os.path.join(out_dir, "dim_customers.csv"), index=False, encoding="utf-8")
    logger.info(f"dim_customers written: {len(dim)} rows")

    # agg_customer_metrics: order-level aggregations per customer
    orders_silver_dir = get_silver_path(DOMAIN, "orders", run_date)
    orders_files = glob.glob(os.path.join(orders_silver_dir, "*.csv"))
    if not orders_files:
        logger.warning("Silver orders not found — skipping agg_customer_metrics")
        return

    orders = pd.concat([pd.read_csv(f) for f in orders_files], ignore_index=True)
    orders["amount"] = pd.to_numeric(orders["amount"], errors="coerce")
    orders["order_date"] = pd.to_datetime(orders["order_date"], errors="coerce")

    metrics = (
        orders.groupby("customer_id")
        .agg(
            total_orders=("order_id", "count"),
            total_amount=("amount", "sum"),
            avg_order_amount=("amount", "mean"),
            first_order_date=("order_date", "min"),
            last_order_date=("order_date", "max"),
        )
        .reset_index()
    )
    metrics["total_amount"] = metrics["total_amount"].round(2)
    metrics["avg_order_amount"] = metrics["avg_order_amount"].round(2)

    metrics.to_csv(
        os.path.join(out_dir, "agg_customer_metrics.csv"), index=False, encoding="utf-8"
    )
    logger.info(f"agg_customer_metrics written: {len(metrics)} rows")


if __name__ == "__main__":
    build(str(date.today()))

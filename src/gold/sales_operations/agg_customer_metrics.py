import glob
import os
from datetime import date

import pandas as pd

from utils.logger import get_logger
from utils.paths import ensure_dir, get_gold_table_path, get_silver_path

logger = get_logger("gold.agg_customer_metrics")

DOMAIN = "sales_operations"
TABLE = "agg_customer_metrics"


def build(run_date: str) -> None:
    orders_dir = get_silver_path(DOMAIN, "orders", run_date)
    files = glob.glob(os.path.join(orders_dir, "*.csv"))
    if not files:
        raise FileNotFoundError(f"No silver orders file in {orders_dir}")

    new_orders = pd.concat([pd.read_csv(f) for f in files], ignore_index=True)
    new_orders["amount"] = pd.to_numeric(new_orders["amount"], errors="coerce")
    new_orders["order_date"] = pd.to_datetime(new_orders["order_date"], errors="coerce")

    out_dir = ensure_dir(get_gold_table_path(DOMAIN, TABLE))
    out_path = os.path.join(out_dir, f"{TABLE}.csv")

    # Merge with existing orders from previous runs before recalculating
    if os.path.exists(out_path):
        existing_metrics = pd.read_csv(out_path)
        known_customers = set(existing_metrics["customer_id"])
        new_customers = set(new_orders["customer_id"])
        if not new_customers.issubset(known_customers):
            logger.info("New customers detected — recalculating metrics from full order history")

    # Accumulate raw orders from all previous silver partitions
    all_orders_dir = os.path.dirname(orders_dir)
    all_order_files = glob.glob(os.path.join(all_orders_dir, "**", "*.csv"), recursive=True)
    all_orders = pd.concat([pd.read_csv(f) for f in all_order_files], ignore_index=True)
    all_orders["amount"] = pd.to_numeric(all_orders["amount"], errors="coerce")
    all_orders["order_date"] = pd.to_datetime(all_orders["order_date"], errors="coerce")
    all_orders = all_orders.drop_duplicates(subset=["order_id"], keep="last")

    metrics = (
        all_orders.groupby("customer_id")
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

    metrics.to_csv(out_path, index=False, encoding="utf-8")
    logger.info(f"agg_customer_metrics written: {len(metrics)} rows (accumulated)")


if __name__ == "__main__":
    build(str(date.today()))

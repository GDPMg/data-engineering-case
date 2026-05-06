import glob
import os
from datetime import date

import pandas as pd

from utils.logger import get_logger
from utils.paths import ensure_dir, get_gold_path, get_silver_path

logger = get_logger("gold.agg_customer_metrics")

DOMAIN = "sales_operations"


def build(run_date: str) -> None:
    orders_dir = get_silver_path(DOMAIN, "orders", run_date)
    files = glob.glob(os.path.join(orders_dir, "*.csv"))
    if not files:
        raise FileNotFoundError(f"No silver orders file in {orders_dir}")

    orders = pd.concat([pd.read_csv(f) for f in files], ignore_index=True)
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

    out_dir = ensure_dir(get_gold_path(DOMAIN, "customers", run_date))
    metrics.to_csv(os.path.join(out_dir, "agg_customer_metrics.csv"), index=False, encoding="utf-8")
    logger.info(f"agg_customer_metrics written: {len(metrics)} rows")


if __name__ == "__main__":
    build(str(date.today()))

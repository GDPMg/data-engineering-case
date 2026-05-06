import glob
import os
from datetime import date, datetime

import pandas as pd

from utils.logger import get_logger
from utils.paths import ensure_dir, get_gold_path, get_silver_path

logger = get_logger("gold.orders")

DOMAIN = "sales_operations"
ENTITY = "orders"


def build(run_date: str) -> None:
    silver_dir = get_silver_path(DOMAIN, ENTITY, run_date)
    files = glob.glob(os.path.join(silver_dir, "*.csv"))
    if not files:
        raise FileNotFoundError(f"No silver orders file in {silver_dir}")

    orders = pd.concat([pd.read_csv(f) for f in files], ignore_index=True)
    orders["order_date"] = pd.to_datetime(orders["order_date"], errors="coerce")
    orders["amount"] = pd.to_numeric(orders["amount"], errors="coerce")

    out_dir = ensure_dir(get_gold_path(DOMAIN, ENTITY, run_date))

    # fact_orders: analytical event table with time dimensions pre-computed
    fact = orders[
        ["order_id", "customer_id", "order_date", "amount", "payment_method", "status"]
    ].copy()
    fact["year"] = fact["order_date"].dt.year.astype("Int64")
    fact["month"] = fact["order_date"].dt.month.astype("Int64")
    fact["quarter"] = fact["order_date"].dt.quarter.astype("Int64")

    fact.to_csv(os.path.join(out_dir, "fact_orders.csv"), index=False, encoding="utf-8")
    logger.info(f"fact_orders written: {len(fact)} rows")

    # agg_orders_monthly: time-series aggregation for trend analysis
    orders["year"] = orders["order_date"].dt.year
    orders["month"] = orders["order_date"].dt.month

    agg_monthly = (
        orders.groupby(["year", "month"])
        .agg(
            total_orders=("order_id", "count"),
            total_amount=("amount", "sum"),
            avg_amount=("amount", "mean"),
            unique_customers=("customer_id", "nunique"),
            paid_count=("status", lambda x: (x == "paid").sum()),
            cancelled_count=("status", lambda x: (x == "cancelled").sum()),
            refunded_count=("status", lambda x: (x == "refunded").sum()),
        )
        .reset_index()
    )
    agg_monthly["total_amount"] = agg_monthly["total_amount"].round(2)
    agg_monthly["avg_amount"] = agg_monthly["avg_amount"].round(2)

    agg_monthly.to_csv(
        os.path.join(out_dir, "agg_orders_monthly.csv"), index=False, encoding="utf-8"
    )
    logger.info(f"agg_orders_monthly written: {len(agg_monthly)} rows")


if __name__ == "__main__":
    build(str(date.today()))

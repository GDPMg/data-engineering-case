import glob
import os
from datetime import date

import pandas as pd

from utils.logger import get_logger
from utils.paths import ensure_dir, get_gold_path, get_silver_path

logger = get_logger("gold.agg_orders_monthly")

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
    orders["year"] = orders["order_date"].dt.year
    orders["month"] = orders["order_date"].dt.month

    agg = (
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
    agg["total_amount"] = agg["total_amount"].round(2)
    agg["avg_amount"] = agg["avg_amount"].round(2)

    out_dir = ensure_dir(get_gold_path(DOMAIN, ENTITY, run_date))
    agg.to_csv(os.path.join(out_dir, "agg_orders_monthly.csv"), index=False, encoding="utf-8")
    logger.info(f"agg_orders_monthly written: {len(agg)} rows")


if __name__ == "__main__":
    build(str(date.today()))

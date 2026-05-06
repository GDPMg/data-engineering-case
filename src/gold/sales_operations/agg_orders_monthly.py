import glob
import os
from datetime import date

import pandas as pd

from utils.logger import get_logger
from utils.paths import ensure_dir, get_gold_table_path, get_silver_path

logger = get_logger("gold.agg_orders_monthly")

DOMAIN = "sales_operations"
TABLE = "agg_orders_monthly"


def build(run_date: str) -> None:
    # Recalculate from all accumulated silver partitions for accuracy
    silver_base = os.path.dirname(get_silver_path(DOMAIN, "orders", run_date))
    all_files = glob.glob(os.path.join(silver_base, "**", "*.csv"), recursive=True)
    if not all_files:
        raise FileNotFoundError(f"No silver orders files found under {silver_base}")

    orders = pd.concat([pd.read_csv(f) for f in all_files], ignore_index=True)
    orders["order_date"] = pd.to_datetime(orders["order_date"], errors="coerce")
    orders["amount"] = pd.to_numeric(orders["amount"], errors="coerce")
    orders = orders.drop_duplicates(subset=["order_id"], keep="last")

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

    out_dir = ensure_dir(get_gold_table_path(DOMAIN, TABLE))
    out_path = os.path.join(out_dir, f"{TABLE}.csv")
    agg.to_csv(out_path, index=False, encoding="utf-8")
    logger.info(f"agg_orders_monthly written: {len(agg)} rows (accumulated)")


if __name__ == "__main__":
    build(str(date.today()))

import glob
import os
from datetime import date

import pandas as pd

from utils.logger import get_logger
from utils.paths import ensure_dir, get_gold_path, get_silver_path

logger = get_logger("gold.fact_orders")

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

    fact = orders[
        ["order_id", "customer_id", "order_date", "amount", "payment_method", "status"]
    ].copy()
    fact["year"] = fact["order_date"].dt.year.astype("Int64")
    fact["month"] = fact["order_date"].dt.month.astype("Int64")
    fact["quarter"] = fact["order_date"].dt.quarter.astype("Int64")

    out_dir = ensure_dir(get_gold_path(DOMAIN, ENTITY, run_date))
    fact.to_csv(os.path.join(out_dir, "fact_orders.csv"), index=False, encoding="utf-8")
    logger.info(f"fact_orders written: {len(fact)} rows")


if __name__ == "__main__":
    build(str(date.today()))

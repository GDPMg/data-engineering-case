import glob
import os
from datetime import date

import pandas as pd

from utils.logger import get_logger
from utils.paths import ensure_dir, get_gold_table_path, get_silver_path

logger = get_logger("gold.fact_orders")

DOMAIN = "sales_operations"
TABLE = "fact_orders"


def build(run_date: str) -> None:
    silver_dir = get_silver_path(DOMAIN, "orders", run_date)
    files = glob.glob(os.path.join(silver_dir, "*.csv"))
    if not files:
        raise FileNotFoundError(f"No silver orders file in {silver_dir}")

    new = pd.concat([pd.read_csv(f) for f in files], ignore_index=True)
    new["order_date"] = pd.to_datetime(new["order_date"], errors="coerce")
    new["amount"] = pd.to_numeric(new["amount"], errors="coerce")

    fact = new[["order_id", "customer_id", "order_date", "amount", "payment_method", "status"]].copy()
    fact["year"] = fact["order_date"].dt.year.astype("Int64")
    fact["month"] = fact["order_date"].dt.month.astype("Int64")
    fact["quarter"] = fact["order_date"].dt.quarter.astype("Int64")

    out_dir = ensure_dir(get_gold_table_path(DOMAIN, TABLE))
    out_path = os.path.join(out_dir, f"{TABLE}.csv")

    if os.path.exists(out_path):
        existing = pd.read_csv(out_path)
        fact = pd.concat([existing, fact], ignore_index=True)

    # Keep latest version per order — new silver always wins
    fact = fact.drop_duplicates(subset=["order_id"], keep="last")

    fact.to_csv(out_path, index=False, encoding="utf-8")
    logger.info(f"fact_orders written: {len(fact)} rows (accumulated)")


if __name__ == "__main__":
    build(str(date.today()))

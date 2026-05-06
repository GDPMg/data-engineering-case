import glob
import os
from datetime import date, datetime, timezone

import pandas as pd

from utils.logger import get_logger
from utils.paths import ensure_dir, get_gold_table_path, get_silver_path

logger = get_logger("gold.dim_customers")

DOMAIN = "sales_operations"
TABLE = "dim_customers"


def build(run_date: str) -> None:
    silver_dir = get_silver_path(DOMAIN, "customers", run_date)
    files = glob.glob(os.path.join(silver_dir, "*.csv"))
    if not files:
        raise FileNotFoundError(f"No silver customers file in {silver_dir}")

    new = pd.concat([pd.read_csv(f, dtype=str) for f in files], ignore_index=True)
    new["created_at"] = pd.to_datetime(new["created_at"], errors="coerce")

    dim = new[["customer_id", "city", "state", "status", "created_at"]].copy()
    dim["days_since_registration"] = (pd.Timestamp(date.today()) - dim["created_at"]).dt.days
    dim["updated_at"] = datetime.now(timezone.utc).isoformat()

    out_dir = ensure_dir(get_gold_table_path(DOMAIN, TABLE))
    out_path = os.path.join(out_dir, f"{TABLE}.csv")

    if os.path.exists(out_path):
        existing = pd.read_csv(out_path, dtype=str)
        dim = pd.concat([existing, dim], ignore_index=True)

    # Keep latest record per customer — new silver always wins
    dim = dim.drop_duplicates(subset=["customer_id"], keep="last")

    dim.to_csv(out_path, index=False, encoding="utf-8")
    logger.info(f"dim_customers written: {len(dim)} rows (accumulated)")


if __name__ == "__main__":
    build(str(date.today()))

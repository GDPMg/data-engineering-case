import glob
import os
from datetime import date, datetime, timezone

import pandas as pd

from utils.logger import get_logger
from utils.paths import ensure_dir, get_gold_path, get_silver_path

logger = get_logger("gold.dim_customers")

DOMAIN = "sales_operations"
ENTITY = "customers"


def build(run_date: str) -> None:
    silver_dir = get_silver_path(DOMAIN, ENTITY, run_date)
    files = glob.glob(os.path.join(silver_dir, "*.csv"))
    if not files:
        raise FileNotFoundError(f"No silver customers file in {silver_dir}")

    df = pd.concat([pd.read_csv(f, dtype=str) for f in files], ignore_index=True)
    df["created_at"] = pd.to_datetime(df["created_at"], errors="coerce")

    dim = df[["customer_id", "city", "state", "status", "created_at"]].copy()
    dim["days_since_registration"] = (pd.Timestamp(date.today()) - dim["created_at"]).dt.days
    dim["updated_at"] = datetime.now(timezone.utc).isoformat()

    out_dir = ensure_dir(get_gold_path(DOMAIN, ENTITY, run_date))
    dim.to_csv(os.path.join(out_dir, "dim_customers.csv"), index=False, encoding="utf-8")
    logger.info(f"dim_customers written: {len(dim)} rows")


if __name__ == "__main__":
    build(str(date.today()))

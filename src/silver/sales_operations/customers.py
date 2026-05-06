import glob
import os
from datetime import datetime, timezone

import pandas as pd

from utils.anonymization import apply_lgpd
from utils.data_quality import report_quality, split_rejects
from utils.logger import get_logger
from utils.paths import ensure_dir, get_bronze_path, get_rejects_path, get_silver_path

logger = get_logger("silver.customers")

DOMAIN = "sales_operations"
ENTITY = "customers"
VALID_STATUSES = {"active", "inactive", "blocked"}


def _normalize_date(value) -> str | None:
    if pd.isna(value) or str(value).strip() == "":
        return None
    raw = str(value).strip()
    for fmt in ("%Y-%m-%d", "%d/%m/%Y", "%Y/%m/%d"):
        try:
            return datetime.strptime(raw, fmt).strftime("%Y-%m-%d")
        except ValueError:
            continue
    logger.warning(f"Unparseable date value: '{raw}'")
    return None


def process(run_date: str) -> None:
    bronze_dir = get_bronze_path(DOMAIN, ENTITY, run_date)
    files = glob.glob(os.path.join(bronze_dir, "*.csv"))
    if not files:
        raise FileNotFoundError(f"No bronze file found in {bronze_dir}")

    df = pd.concat([pd.read_csv(f, dtype=str) for f in files], ignore_index=True)
    logger.info(f"Silver customers: {len(df)} rows loaded from bronze")

    df["created_at"] = df["created_at"].apply(_normalize_date)
    df["status"] = df["status"].str.strip().str.lower()

    # Duplicates: keep the last occurrence per customer_id
    # The source appends updated records at the end, so the last row wins
    before = len(df)
    df = df.drop_duplicates(subset=["customer_id"], keep="last")
    logger.info(f"Deduplication removed {before - len(df)} duplicate customer(s)")

    all_rejects = []

    # Hard reject: status outside the known domain
    df, rej = split_rejects(df, ~df["status"].isin(VALID_STATUSES), "invalid_status")
    all_rejects.append(rej)

    # Soft flags: missing optional fields (record is kept, flag is set for downstream awareness)
    df["has_email"] = df["email"].notna() & (df["email"] != "")
    df["has_phone"] = df["phone"].notna() & (df["phone"] != "")
    df["has_created_at"] = df["created_at"].notna()

    df = apply_lgpd(df)

    df["processed_at"] = datetime.now(timezone.utc).isoformat()
    df["pipeline_run_date"] = run_date

    report_quality(df, "silver.customers")

    out_dir = ensure_dir(get_silver_path(DOMAIN, ENTITY, run_date))
    df.to_csv(os.path.join(out_dir, "customers.csv"), index=False, encoding="utf-8")
    logger.info(f"Silver customers written: {len(df)} records")

    non_empty = [r for r in all_rejects if len(r) > 0]
    if non_empty:
        combined = pd.concat(non_empty, ignore_index=True)
        rej_dir = ensure_dir(get_rejects_path(DOMAIN, ENTITY, run_date))
        combined.to_csv(
            os.path.join(rej_dir, "customers.csv"), index=False, encoding="utf-8"
        )
        logger.warning(f"Rejects: {len(combined)} customer record(s) written to rejects")


if __name__ == "__main__":
    from datetime import date
    process(str(date.today()))

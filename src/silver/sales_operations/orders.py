import glob
import os
from datetime import datetime, timezone

import pandas as pd

from utils.data_quality import report_quality, split_rejects
from utils.logger import get_logger
from utils.paths import ensure_dir, get_bronze_path, get_rejects_path, get_silver_path

logger = get_logger("silver.orders")

DOMAIN = "sales_operations"
ENTITY = "orders"
VALID_STATUSES = {"paid", "cancelled", "refunded"}
VALID_PAYMENT_METHODS = {"credit_card", "pix", "boleto", "debit_card"}


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
    logger.info(f"Silver orders: {len(df)} rows loaded from bronze")

    # Normalize amount: accept both "." and "," as decimal separator
    df["amount"] = pd.to_numeric(
        df["amount"].str.replace(",", ".", regex=False), errors="coerce"
    )

    df["order_date"] = df["order_date"].apply(_normalize_date)
    df["status"] = df["status"].str.strip().str.lower()
    df["payment_method"] = df["payment_method"].str.strip().str.lower()

    # Duplicates: keep last occurrence per order_id
    # The source may append corrected records (e.g., updated amounts)
    before = len(df)
    df = df.drop_duplicates(subset=["order_id"], keep="last")
    logger.info(f"Deduplication removed {before - len(df)} duplicate order(s)")

    all_rejects = []

    df, rej = split_rejects(df, df["order_date"].isna(), "invalid_or_missing_date")
    all_rejects.append(rej)

    df, rej = split_rejects(df, df["amount"].isna(), "missing_amount")
    all_rejects.append(rej)

    # Negative amounts are structurally invalid in this domain (refunds use status=refunded)
    df, rej = split_rejects(df, df["amount"] < 0, "negative_amount")
    all_rejects.append(rej)

    df, rej = split_rejects(df, ~df["status"].isin(VALID_STATUSES), "invalid_status")
    all_rejects.append(rej)

    df, rej = split_rejects(
        df, ~df["payment_method"].isin(VALID_PAYMENT_METHODS), "invalid_payment_method"
    )
    all_rejects.append(rej)

    # Validate customer_id referential integrity against silver customers (if available)
    cust_silver_dir = get_silver_path(DOMAIN, "customers", run_date)
    cust_files = glob.glob(os.path.join(cust_silver_dir, "*.csv"))
    if cust_files:
        valid_ids = set(
            pd.concat(
                [pd.read_csv(f, usecols=["customer_id"], dtype=str) for f in cust_files],
                ignore_index=True,
            )["customer_id"]
        )
        df, rej = split_rejects(df, ~df["customer_id"].isin(valid_ids), "unknown_customer_id")
        all_rejects.append(rej)
        logger.info(f"Referential integrity check: {len(rej)} order(s) with unknown customer_id")
    else:
        logger.warning("Silver customers not found — skipping customer_id validation")

    df["processed_at"] = datetime.now(timezone.utc).isoformat()
    df["pipeline_run_date"] = run_date

    report_quality(df, "silver.orders")

    out_dir = ensure_dir(get_silver_path(DOMAIN, ENTITY, run_date))
    df.to_csv(os.path.join(out_dir, "silver_orders.csv"), index=False, encoding="utf-8")
    logger.info(f"Silver orders written: {len(df)} records")

    non_empty = [r for r in all_rejects if len(r) > 0]
    if non_empty:
        combined = pd.concat(non_empty, ignore_index=True)
        rej_dir = ensure_dir(get_rejects_path(DOMAIN, ENTITY, run_date))
        combined.to_csv(
            os.path.join(rej_dir, "rejects_orders.csv"), index=False, encoding="utf-8"
        )
        logger.warning(f"Rejects: {len(combined)} order record(s) written to rejects")


if __name__ == "__main__":
    from datetime import date
    process(str(date.today()))

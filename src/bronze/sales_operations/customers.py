import glob
import os
from datetime import datetime, timezone

import pandas as pd

from utils.data_quality import report_quality
from utils.logger import get_logger
from utils.paths import ensure_dir, get_bronze_path, get_input_path

logger = get_logger("bronze.customers")

DOMAIN = "sales_operations"
ENTITY = "customers"


def ingest(run_date: str) -> None:
    input_dir = get_input_path(DOMAIN, ENTITY, run_date)
    files = glob.glob(os.path.join(input_dir, "*.csv"))
    if not files:
        raise FileNotFoundError(f"No CSV found in {input_dir}")

    dfs = []
    for f in files:
        logger.info(f"Reading source file: {os.path.basename(f)}")
        df = pd.read_csv(f, dtype=str, encoding="utf-8")
        df["source_file"] = os.path.basename(f)
        dfs.append(df)

    df = pd.concat(dfs, ignore_index=True)
    df.columns = [c.strip().lower().replace(" ", "_") for c in df.columns]
    df["ingestion_timestamp"] = datetime.now(timezone.utc).isoformat()
    df["pipeline_run_date"] = run_date

    report_quality(df, "bronze.customers")

    out_dir = ensure_dir(get_bronze_path(DOMAIN, ENTITY, run_date))
    out_path = os.path.join(out_dir, "customers.csv")
    df.to_csv(out_path, index=False, encoding="utf-8")
    logger.info(f"Bronze customers written: {out_path} ({len(df)} rows)")


if __name__ == "__main__":
    from datetime import date
    ingest(str(date.today()))

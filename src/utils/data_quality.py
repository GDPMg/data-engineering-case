import pandas as pd

from utils.logger import get_logger

_logger = get_logger("data_quality")


def report_quality(df: pd.DataFrame, entity: str) -> None:
    _logger.info(f"[{entity}] Total records: {len(df)}")
    for col in df.columns:
        nulls = df[col].isna().sum()
        if nulls > 0:
            _logger.warning(
                f"[{entity}] '{col}': {nulls} null(s) ({nulls / len(df):.1%})"
            )
    dups = df.duplicated().sum()
    if dups:
        _logger.warning(f"[{entity}] {dups} fully duplicated row(s) detected")


def split_rejects(
    df: pd.DataFrame, mask: pd.Series, reason: str
) -> tuple[pd.DataFrame, pd.DataFrame]:
    """Splits df into (valid, rejects). Rejects gain a 'reject_reason' column."""
    rejects = df[mask].copy()
    rejects["reject_reason"] = reason
    valid = df[~mask].copy()
    return valid, rejects

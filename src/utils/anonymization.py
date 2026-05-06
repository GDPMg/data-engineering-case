import hashlib
import os

import pandas as pd

_SALT = os.environ.get("ANONYMIZATION_SALT", "koin_pipeline_2024_salt")


def _sha256(value: str) -> str:
    return hashlib.sha256(f"{_SALT}{value}".encode("utf-8")).hexdigest()


def hash_field(value) -> str | None:
    if pd.isna(value) or str(value).strip() == "":
        return None
    return _sha256(str(value).strip())


def mask_email(value) -> str | None:
    """Keeps the domain visible (***@domain.com) to allow city/region analysis without exposing identity."""
    if pd.isna(value) or str(value).strip() == "":
        return None
    parts = str(value).strip().split("@")
    if len(parts) == 2:
        return f"***@{parts[1]}"
    return "***@***"


def mask_phone(value) -> str | None:
    """Keeps only the last 4 digits for partial identification without full exposure."""
    if pd.isna(value) or str(value).strip() == "":
        return None
    digits = "".join(filter(str.isdigit, str(value)))
    if len(digits) >= 4:
        return f"{'*' * (len(digits) - 4)}{digits[-4:]}"
    return "****"


def apply_lgpd(df: pd.DataFrame) -> pd.DataFrame:
    """Applies LGPD-compliant masking to all PII columns present in the DataFrame."""
    df = df.copy()
    if "name" in df.columns:
        df["name"] = df["name"].apply(hash_field)
    if "email" in df.columns:
        df["email"] = df["email"].apply(mask_email)
    if "phone" in df.columns:
        df["phone"] = df["phone"].apply(mask_phone)
    return df

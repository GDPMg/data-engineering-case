import os
from pathlib import Path

BASE_DIR = os.environ.get(
    "PIPELINE_BASE_DIR",
    str(Path(__file__).resolve().parents[2]),
)


def _build_path(layer: str, domain: str, entity: str, run_date: str) -> str:
    return os.path.join(BASE_DIR, "data", layer, domain, entity, run_date)


def get_input_path(domain: str, entity: str, run_date: str) -> str:
    return _build_path("input", domain, entity, run_date)


def get_bronze_path(domain: str, entity: str, run_date: str) -> str:
    return _build_path("bronze", domain, entity, run_date)


def get_silver_path(domain: str, entity: str, run_date: str) -> str:
    return _build_path("silver", domain, entity, run_date)


def get_gold_path(domain: str, entity: str, run_date: str) -> str:
    return _build_path("gold", domain, entity, run_date)


def get_rejects_path(domain: str, entity: str, run_date: str) -> str:
    return _build_path("rejects", domain, entity, run_date)


def ensure_dir(path: str) -> str:
    os.makedirs(path, exist_ok=True)
    return path

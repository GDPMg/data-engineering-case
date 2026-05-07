import glob
import logging
import os

logger = logging.getLogger(__name__)


def check_entity_input(domain: str, entity: str, run_date: str) -> bool:
    base = os.environ.get("PIPELINE_BASE_DIR", "/opt/pipeline")
    path = os.path.join(base, "data", "input", domain, entity, run_date)
    files = glob.glob(os.path.join(path, "*.csv"))
    if files:
        logger.info(f"Input found for '{entity}' ({run_date}): {[os.path.basename(f) for f in files]}")
        return True
    logger.warning(f"No input file for '{entity}' at {path} — pipeline will be skipped")
    return False


def check_all_inputs(domain: str, run_date: str) -> bool:
    return all(
        check_entity_input(domain, entity, run_date)
        for entity in ["customers", "orders"]
    )

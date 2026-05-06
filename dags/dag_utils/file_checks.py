import glob
import logging
import os

logger = logging.getLogger(__name__)


def check_all_inputs(domain: str, run_date: str) -> bool:
    """
    ShortCircuitOperator callable.
    Returns True only when ALL entity input folders for the domain contain at least one CSV.
    If any file is missing, logs the missing path and returns False — skipping the entire pipeline.
    """
    base = os.environ.get("PIPELINE_BASE_DIR", "/opt/pipeline")
    entities = ["customers", "orders"]
    all_present = True

    for entity in entities:
        path = os.path.join(base, "data", "input", domain, entity, run_date)
        files = glob.glob(os.path.join(path, "*.csv"))
        if files:
            logger.info(f"Input found for '{entity}' ({run_date}): {[os.path.basename(f) for f in files]}")
        else:
            logger.warning(f"No input file for '{entity}' at {path} — pipeline will be skipped")
            all_present = False

    return all_present

from pathlib import Path
import shutil
from src.config import path_for

def extract_sources(run_id: str) -> Path:
    """
    Copy immutable source snapshots into a run-specific raw directory.

    Steps:
    1. Create data/raw/run_id=<run_id>/.
    2. Copy customers.csv, products.json, and orders.csv from data/source/.
    3. Return the run-specific raw path.
    4. Do not modify source files in place.
    """
    # 1. Create run-specific raw directory
    raw_run_path = path_for("raw") / f"run_id={run_id}"
    raw_run_path.mkdir(parents=True, exist_ok=True)

    # 2. Define source files
    source_dir = path_for("source")
    source_files = ["customers.csv", "products.json", "orders.csv"]

    # 3. Copy files into raw_run_path
    for fname in source_files:
        src = source_dir / fname
        dest = raw_run_path / fname
        if src.exists():
            shutil.copy(src, dest)
        else:
            print(f"Missing source file: {src}")

    # 4. Return the run-specific raw path
    return raw_run_path

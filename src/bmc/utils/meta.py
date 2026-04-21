from bmc.utils.logger import log_execution
import pandas as pd
import logging
from pathlib import Path
from typing import Optional

def fetch_meta(
    filename: str, 
    logger: Optional[logging.Logger] = None
) -> pd.DataFrame:
    """
    Locates and loads a static metadata catalog (CSV) bundled with the package.

    This utility dynamically resolves the path to the requested metadata file 
    by navigating up from 'src/bmc/utils' to the project base, and looking 
    in the parallel 'meta' directory.
    """
    # 1. Determine the codebase root
    # __file__ is .../src/bmc/utils/meta.py
    # .parents[0] is utils/, [1] is bmc/, [2] is src/, [3] is the repository root
    base_dir = Path(__file__).resolve().parents[3]
    
    # 2. Build the path to the parallel meta directory
    target_path = base_dir / 'meta' / filename

    # Assuming log_execution is imported or defined
    # log_execution(logger, f"Loading bundled metadata from: {target_path}", logging.DEBUG)

    # 3. Load and return the file
    try:
        return pd.read_csv(str(target_path))
    except FileNotFoundError:
        raise FileNotFoundError(
            f"Metadata file '{filename}' not found at {target_path}. "
            "Ensure the catalog has been generated and bundled with the package."
        )
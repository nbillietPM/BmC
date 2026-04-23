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

    Parameters
    ----------
    filename : str
        The name of the CSV metadata file to locate and load (e.g., 'catalog.csv').
    logger : logging.Logger, optional
        The logger instance used to record execution steps and debug information. 
        Default is None.

    Returns
    -------
    pd.DataFrame
        A pandas DataFrame containing the loaded metadata catalog.

    Raises
    ------
    FileNotFoundError
        If the specified metadata file cannot be found at the dynamically 
        resolved target path.
    """
    # Determine the codebase root
    # __file__ is .../src/bmc/utils/meta.py
    # .parents[0] is utils/, [1] is bmc/, [2] is src/, [3] is the repository root
    base_dir = Path(__file__).resolve().parents[3]
    
    # Build the path to the parallel meta directory
    target_path = base_dir / 'meta' / filename

    log_execution(logger, f"Loading bundled metadata from: {target_path}", logging.DEBUG)

    # Load and return the file
    try:
        return pd.read_csv(str(target_path))
    except FileNotFoundError as exc:
        error_msg = (
            f"Metadata file '{filename}' not found at {target_path}. "
            "Ensure the catalog has been generated and bundled with the package."
        )
        """
        exc_info=True: This tells Python's logging module to attach the exact traceback to the log file. 
        If this fails on a remote server, your log file won't just say "File not found"—it will show you exactly which line triggered it.
        """
        log_execution(logger, error_msg, logging.ERROR, exc_info=True)
        # Re-raise the error to properly halt the pipeline
        raise FileNotFoundError(error_msg) from exc
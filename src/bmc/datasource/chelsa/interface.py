from bmc.utils.logger import log_execution
import boto3
from botocore import UNSIGNED
from botocore.client import Config
import pandas as pd
from typing import List, Optional

def build_chelsa_catalog(
    bucket_name: str = 'chelsa02',
    endpoint_url: str = 'https://os.unil.cloud.switch.ch',
    categories: Optional[List[str]] = None,
    logger: Optional[logging.Logger] = None
) -> pd.DataFrame:
    """
    Scans a public S3 bucket for CHELSA climate data and builds a structured metadata catalog.

    This function bypasses recursive folder crawling by utilizing S3 pagination to rapidly 
    discover files. It extracts temporal, categorical, and scenario metadata directly from 
    the file paths and names. It also generates a GDAL-compatible virtual file system (VSI) 
    path for lazy loading.

    Parameters
    ----------
    bucket_name : str, optional
        The name of the S3 bucket hosting the CHELSA data. Default is 'chelsa02'.
    endpoint_url : str, optional
        The URL of the S3-compatible cloud storage endpoint. Default is 
        'https://os.unil.cloud.switch.ch'.
    categories : list of str, optional
        A list of top-level categories (prefixes) to scan. If None, defaults to 
        ['daily', 'monthly', 'annual', 'climatologies', 'bioclim'].
    logger : logging.Logger, optional
        The logger object to use for execution tracking. If None, a default logger is created.

    Returns
    -------
    pd.DataFrame
        A pandas DataFrame containing the metadata catalog with the following columns:
        - level (str): The temporal or categorical level (e.g., 'daily', 'bioclim').
        - variable (str): The climate variable (e.g., 'tas', 'bio01').
        - date (datetime64[ns]): The specific timestamp of the file (if applicable).
        - time_range (str): The climatological baseline period (if applicable).
        - ensemble (str): The climate model used for future projections (if applicable).
        - scenario (str): The SSP emission scenario (if applicable).
        - filename (str): The original filename.
        - size_mb (float): The file size in megabytes.
        - vsi_path (str): The /vsicurl/ path for lazy loading via GDAL/rasterio.
    """
    # Initialize logger if not provided
    if logger is None:
        logger = logging.getLogger(__name__)
        logger.setLevel(logging.INFO)

    # Initialize defaults cleanly to avoid mutable default arguments
    if categories is None:
        categories = ['daily', 'monthly', 'annual', 'climatologies', 'bioclim']
        
    # Ensure no trailing slashes mess up the path construction
    endpoint_url = endpoint_url.rstrip('/')
    base_http = f"{endpoint_url}/{bucket_name}"
    
    log_execution(logger, f"Connecting to S3 endpoint: {endpoint_url}", logging.INFO)
    
    s3_client = boto3.client(
        's3',
        endpoint_url=endpoint_url,
        config=Config(signature_version=UNSIGNED)
    )
    
    inventory = []

    for cat in categories:
        prefix = f"chelsa/global/{cat}/"
        log_execution(logger, f"Scanning S3 prefix: {prefix}", logging.INFO)
        
        paginator = s3_client.get_paginator('list_objects_v2')
        pages = paginator.paginate(Bucket=bucket_name, Prefix=prefix)

        for page in pages:
            if 'Contents' not in page:
                continue
                
            for obj in page['Contents']:
                key = obj['Key']
                
                # Skip anything that isn't a GeoTIFF
                if not key.endswith('.tif'):
                    continue
                
                parts = key.split('/')
                filename = parts[-1]
                level = parts[2]
                
                # --- BASE METADATA ---
                metadata = {
                    'level': level,
                    'variable': parts[3],
                    'date': None,
                    'time_range': None,
                    'ensemble': None,
                    'scenario': None,
                    'filename': filename,
                    'size_mb': round(obj['Size'] / (1024 * 1024), 2),
                    'vsi_path': f"/vsicurl/{base_http}/{key}"
                }
                
                # --- TEMPORAL EXTRACTION ---
                file_parts = filename.replace('.tif', '').split('_')
                
                if level == 'daily':
                    day, month, year = file_parts[2], file_parts[3], file_parts[4]
                    metadata['date'] = pd.to_datetime(f"{year}-{month}-{day}")
                
                elif level == 'monthly':
                    month, year = file_parts[2], file_parts[3]
                    metadata['date'] = pd.to_datetime(f"{year}-{month}-01")
                    
                elif level == 'annual':
                    year = file_parts[2]
                    metadata['date'] = pd.to_datetime(f"{year}-01-01")
                    
                elif level in ['climatologies', 'bioclim']:
                    metadata['time_range'] = parts[4] 
                    
                    # --- ENSEMBLE & SCENARIO EXTRACTION ---
                    if len(parts) == 7:
                        metadata['ensemble'] = parts[5]
                    elif len(parts) >= 8:
                        metadata['ensemble'] = parts[5]
                        metadata['scenario'] = parts[6]

                inventory.append(metadata)

    # Convert to DataFrame and sort chronologically and categorically
    df = pd.DataFrame(inventory)
    df = df.sort_values(by=['level', 'variable', 'date', 'time_range']).reset_index(drop=True)
    
    log_execution(logger, f"Catalog complete! Successfully inventoried {len(df)} files.", logging.INFO)
    
    return df

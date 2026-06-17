# Standard library imports
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone
import os
from typing import List, Optional

# Third-party imports
import boto3
from botocore import UNSIGNED
from botocore.client import Config
import pandas as pd
import pystac
import stac_geoparquet

# Local application/project imports
from bmc.utils.logger import log_execution

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

def fetch_and_build_items(prefix: str, base_http: str) -> list:
    """
    Worker function that scans a specific sub-folder in the CHELSA S3 bucket 
    and builds STAC Items in memory.

    Parameters
    ----------
    prefix : str
        The S3 directory prefix to scan (e.g., 'chelsa/global/climatologies/').
    base_http : str
        The base HTTP URL used to construct the virtual file system (vsi) 
        path for the STAC Asset.

    Returns
    -------
    list
        A list of constructed `pystac.Item` objects containing the extracted 
        metadata and spatial/temporal bounds for each file.
    """
    # Initialize an anonymous S3 client (CHELSA bucket is public)
    s3_client = boto3.client(
        's3', 
        endpoint_url='https://os.unil.cloud.switch.ch', 
        config=Config(signature_version=UNSIGNED)
    )
    paginator = s3_client.get_paginator('list_objects_v2')
    
    items = []
    pages = paginator.paginate(Bucket='chelsa02', Prefix=prefix)
    
    # Global spatial extent applicable to all CHELSA planetary datasets
    global_bbox = [-180.0, -90.0, 180.0, 90.0]
    global_geometry = {
        "type": "Polygon", 
        "coordinates": [[
            [-180.0, -90.0], [180.0, -90.0], [180.0, 90.0], 
            [-180.0, 90.0], [-180.0, -90.0]
        ]]
    }

    for page in pages:
        if 'Contents' not in page: 
            continue
            
        for obj in page['Contents']:
            key = obj['Key']
            
            # Skip any auxiliary files, only process GeoTIFFs
            if not key.endswith('.tif'): 
                continue
            
            # =========================================================
            # DIRECTORY & FILENAME PARSING
            # =========================================================
            parts = key.split('/')
            filename = parts[-1]
            
            # Extract ONLY the directories to prevent grabbing the .tif filename
            # as part of the nested model/scenario metadata.
            folders = parts[:-1] 
            
            # Folder depth mappings: [bucket_root]/[domain]/[level]/[variable]/...
            level = folders[2]
            variable = folders[3]
            
            # Initialize default homogeneous STAC properties
            properties = {
                "chelsa:level": level,
                "chelsa:variable": variable,
                "chelsa:filename": filename,
                "chelsa:time_range": None,
                "chelsa:month": None,
                "cmip6:model": "historical",     # Default fallback
                "cmip6:scenario": "historical"   # Default fallback
            }
            
            # =========================================================
            # METADATA EXTRACTION BY FOLDER DEPTH
            # =========================================================
            if level in ['climatologies', 'bioclim']:
                # Extract time_range, model, and scenario based on known folder depth
                if len(folders) > 4:
                    properties['chelsa:time_range'] = folders[4]
                if len(folders) > 5:
                    properties['cmip6:model'] = folders[5]
                if len(folders) > 6:
                    properties['cmip6:scenario'] = folders[6]
                    
                # Dynamic Month Scanning Engine for Climatologies
                # Searches the filename for a valid 2-digit month (01-12)
                if level == 'climatologies':
                    file_parts = filename.replace('.tif', '').split('_')
                    detected_month = None
                    for part in file_parts:
                        if part.isdigit() and len(part) == 2 and 1 <= int(part) <= 12:
                            detected_month = int(part)
                            break
                    properties['chelsa:month'] = detected_month
                    
            elif level == 'monthly':
                # Extract month strictly from the filename layout
                file_parts = filename.replace('.tif', '').split('_')
                try: 
                    properties['chelsa:month'] = int(file_parts[2])
                except (ValueError, IndexError): 
                    pass

            # =========================================================
            # TEMPORAL (DATETIME) EXTRACTION
            # =========================================================
            file_parts = filename.replace('.tif', '').split('_')
            
            # Default fallback datetime for non-continuous projections
            item_datetime = datetime(2000, 1, 1, tzinfo=timezone.utc)
            
            try:
                if level == 'daily':
                    day, month, year = file_parts[2], file_parts[3], file_parts[4]
                    item_datetime = datetime(int(year), int(month), int(day), tzinfo=timezone.utc)
                elif level == 'monthly':
                    month, year = file_parts[2], file_parts[3]
                    item_datetime = datetime(int(year), int(month), 1, tzinfo=timezone.utc)
                elif level == 'annual':
                    year = file_parts[2]
                    item_datetime = datetime(int(year), 1, 1, tzinfo=timezone.utc)
            except (ValueError, IndexError):
                pass

            # =========================================================
            # PANDAS NANOSECOND BOUNDARY FIX
            # Protects against OutOfBoundsDatetime errors in pyarrow if 
            # dates precede year 1678.
            # =========================================================
            if item_datetime.year < 1678:
                item_datetime = datetime(1981, 1, 1, tzinfo=timezone.utc)
                
            # Build the STAC Item
            item = pystac.Item(
                id=filename.replace('.tif', ''),
                geometry=global_geometry,
                bbox=global_bbox,
                datetime=item_datetime,
                properties=properties
            )
            
            # Add the actual cloud-optimized GeoTIFF as a STAC Asset
            item.add_asset(
                variable, 
                pystac.Asset(
                    href=f"/vsicurl/{base_http}/{key}", 
                    media_type=pystac.MediaType.COG, 
                    roles=["data"]
                )
            )
            items.append(item)
            
    print(f"Thread finished '{prefix}': Built {len(items)} items.")
    return items


def build_geoparquet_catalog(output_dir: str = "./chelsa_geoparquet") -> None:
    """
    Main orchestrator function that triggers multithreaded S3 scraping and 
    compresses the resulting STAC memory objects into a single GeoParquet database.

    Parameters
    ----------
    output_dir : str, optional
        The local directory where the resulting `chelsa_master.parquet` file 
        will be saved. Defaults to "./chelsa_geoparquet".
        
    Returns
    -------
    None
    """
    base_http = "https://os.unil.cloud.switch.ch/chelsa02"
    os.makedirs(output_dir, exist_ok=True)
    parquet_path = os.path.join(output_dir, "chelsa_master.parquet")
    
    # Core data categories corresponding to top-level folder prefixes
    categories = ['daily', 'monthly', 'annual', 'climatologies', 'bioclim']
    
    all_items = []
    print("Spinning up ThreadPoolExecutor for S3 pagination...")
    
    # Multithreaded S3 directory crawling to bypass sequential I/O bottlenecks
    with ThreadPoolExecutor(max_workers=5) as executor:
        futures = [
            executor.submit(fetch_and_build_items, f"chelsa/global/{cat}/", base_http) 
            for cat in categories
        ]
        
        for future in as_completed(futures):
            all_items.extend(future.result())

    print(f"\nTotal items loaded into memory: {len(all_items)}")
    
    # Convert PySTAC objects back into raw dictionaries
    print("Converting PySTAC items to dictionaries...")
    item_dicts = [item.to_dict() for item in all_items]
    
    # Use stac-geoparquet to flatten the JSON structures into a Pandas/GeoPandas DataFrame
    print("Flattening dictionaries into a GeoDataFrame...")
    gdf = stac_geoparquet.to_geodataframe(item_dicts)
    
    # Save the DataFrame to disk as a highly compressed binary GeoParquet file
    print(f"Compressing into a single GeoParquet database at: {parquet_path}...")
    gdf.to_parquet(parquet_path)
    
    print("Complete!")

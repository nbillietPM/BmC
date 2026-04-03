import urllib.parse
import json
from hda import Client
import zipfile
import shutil
from pathlib import Path

def get_dataset_variables(dataset_id: str, wekeo_client: Client):
    """
    Queries the WEkEO broker for all valid subsetting parameters for a given dataset.
    """
    # 1. URL-encode the dataset ID to handle the colons
    encoded_id = urllib.parse.quote(dataset_id)
    
    # 2. Drop the "api/v1/" since the client adds it automatically
    endpoint = f"dataaccess/queryable/{encoded_id}"
    
    try:
        # 3. The client.get() method returns the parsed JSON dictionary directly!
        schema_json = wekeo_client.get(endpoint)
        return schema_json
        
    except Exception as e:
        print(f"Error fetching schema: {e}")
        return None
    
import pandas as pd
import json
import logging

def intersect_config_with_dataframe(config, inventory_csv_path, target_res="100m", logger=None):
    """
    Validates user config against the ground-truth DataFrame.
    Strictly enforces the target resolution. Will not fall back to higher resolutions.
    Logs progress and the final generated execution queue.
    """
    # Load the ground truth
    df = pd.read_csv(inventory_csv_path)
    
    start_year = config['global_temporal']['start_year']
    end_year = config['global_temporal']['end_year']
    user_bbox = config.get('spatial', {}).get('bbox', None)
    
    execution_queue = []
    
    # Filter the DataFrame to only the years we care about
    temporal_df = df[(df['year'] >= start_year) & (df['year'] <= end_year)]
    
    if logger:
        logger.info(f"Validating config against inventory for years {start_year}-{end_year} at {target_res}...")
    
    # Loop through the categories requested in the YAML
    for category in ['TCF', 'GRA', 'IMP', 'CRL', 'CORINE']:
        requested_products = config.get(category, {}).get('productTypes', [])
        
        for product in requested_products:
            # THE STRICT FILTER: Match Product AND the exact Target Resolution
            strict_df = temporal_df[(temporal_df['productType'] == product) & 
                                    (temporal_df['resolution'] == target_res)]
            
            if strict_df.empty:
                if logger:
                    logger.warning(f"'{product}' skipped: No {target_res} data available between {start_year}-{end_year}.")
                continue
                
            # Find the unique valid years that survived the strict filter
            valid_years = sorted(strict_df['year'].unique())
            
            for year in valid_years:
                # Grab the correct dataset_id for this match
                dataset_id = strict_df[strict_df['year'] == year]['dataset_id'].iloc[0]
                
                query_kwargs = {
                    "dataset_id": dataset_id,
                    "product_type": product,
                    "year": str(year),
                    "resolution": target_res
                }
                
                if user_bbox:
                    query_kwargs["bbox"] = user_bbox
                    
                execution_queue.append(query_kwargs)
                if logger:
                    logger.info(f"Queued: {product} ({year}) strictly at {target_res}")
                
    # --- NEW: Log the final execution queue ---
    if logger:
        if execution_queue:
            # Pretty-print the list of dictionaries with indentation
            pretty_queue = json.dumps(execution_queue, indent=4)
            indented_queue = "\n    " + pretty_queue.replace("\n", "\n    ")
            logger.info(f"Successfully generated {len(execution_queue)} queries:{indented_queue}")
        else:
            logger.warning("No queries were generated. The execution queue is empty.")
            
    return execution_queue

def gather_tifs_from_zips(source_directory, target_directory, logger=None):
    """
    Iterates through zip files in a source directory and extracts 
    only the .tif files into a single target directory.
    """
    # 1. Set up the paths
    source_path = Path(source_directory)
    target_path = Path(target_directory)
    
    # Create the target directory if it doesn't exist yet
    target_path.mkdir(parents=True, exist_ok=True)

    # 2. Find all zip files in the source folder
    zip_files = list(source_path.glob("*.zip"))
    print(f"Found {len(zip_files)} zip files. Starting extraction...")

    # 3. Iterate over each zip file
    for zip_file_path in zip_files:
        with zipfile.ZipFile(zip_file_path, 'r') as zip_ref:
            
            # Look at every file hidden inside the zip archive
            for file_info in zip_ref.infolist():
                
                # Check if the file is a .tif
                if file_info.filename.endswith('.tif'):
                    
                    # Extract the filename without any internal zip folder structure
                    tif_filename = Path(file_info.filename).name 
                    destination_file = target_path / tif_filename
                    
                    # 4. Copy the .tif file to the shared folder
                    with zip_ref.open(file_info) as source_file:
                        with open(destination_file, 'wb') as target_file:
                            shutil.copyfileobj(source_file, target_file)
                            
                    print(f"Copied: {tif_filename}")

    print("All .tif files have been gathered successfully!")
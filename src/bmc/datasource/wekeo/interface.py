import os
import pandas as pd
import itertools
import re
import time
import logging
from typing import Optional, Dict, Any
import urllib.parse
from hda import Client
from bmc.utils.logger import log_execution

def generate_wekeo_query(
    dataset_id: str, 
    product_type: str, 
    year: str = None, 
    data_format: str = "GeoTiff100mt",
    resolution: str = None,
    bbox: list = None, 
    items_per_page: int = 200, 
    start_index: int = 0
) -> dict:
    """
    Generates a dynamically structured WEkEO HDA API query dictionary.

    This function safely constructs the JSON payload required to request data 
    from the WEkEO Harmonized Data Access (HDA) API. It automatically handles 
    dataset-specific API quirks, such as routing CORINE Land Cover queries to 
    use a 'format' key while rejecting 'year' and 'resolution', whereas standard 
    High Resolution Layers (HRL) correctly map 'resolution' and 'year'.

    Parameters
    ----------
    dataset_id : str
        The master identifier for the dataset (e.g., ``"EO:EEA:DAT:CORINE"`` or 
        ``"EO:EEA:DAT:HRL:CRL"``).
    product_type : str
        The specific product to request from within the dataset (e.g., 
        ``"Crop Types"`` or ``"Tree Cover Density"``).
    year : str, optional
        The reference year for the requested data. Ignored if the `dataset_id` 
        is CORINE. Default is ``None``.
    data_format : str, optional
        The format of the downloaded file. This parameter is exclusively used 
        when querying CORINE datasets. Default is ``"GeoTiff100mt"``.
    resolution : str, optional
        The spatial resolution string required by HRL datasets (e.g., ``"10m"`` 
        or ``"100m"``). Ignored if the `dataset_id` is CORINE. Default is ``None``.
    bbox : list of float, optional
        The spatial bounding box for the query, formatted as 
        ``[min_lon, min_lat, max_lon, max_lat]``. If ``None``, the API will 
        default to the dataset's maximum spatial extent. Default is ``None``.
    items_per_page : int, optional
        Pagination parameter controlling how many download links the API returns 
        per request. Default is ``200``.
    start_index : int, optional
        Pagination parameter indicating where the API should start returning 
        results. Default is ``0``.

    Returns
    -------
    dict
        A correctly formatted query payload dictionary, ready to be passed 
        directly to the WEkEO HDA API.
    """
    
    # Base query elements (Order is preserved top-to-bottom)
    query = {"dataset_id": dataset_id}
    
    # Inject bbox right after dataset_id if it exists
    if bbox is not None:
        query["bbox"] = bbox
        
    query["productType"] = product_type

    # Add dataset-specific parameters
    if dataset_id == "EO:EEA:DAT:CORINE":
        # CORINE exclusively uses 'format' and rejects 'year'/'resolution'
        query["format"] = data_format
        
    else:
        # Standard HRL layers AND SLF datasets use 'resolution' and 'year'
        if resolution is not None: 
            query["resolution"] = resolution
        if year is not None: 
            query["year"] = year

    # Cap it off with pagination parameters
    query["itemsPerPage"] = items_per_page
    query["startIndex"] = start_index

    return query

def get_dataset_variables(dataset_id: str, wekeo_client: Any) -> Optional[Dict[str, Any]]:
    """
    Retrieves the metadata schema and valid subsetting parameters for a specific WEkEO dataset.

    This function queries the WEkEO Harmonized Data Access (HDA) broker to fetch the 
    queryable JSON schema for a given dataset. This schema defines the exact parameters, 
    variables, bounding box constraints, and date ranges that are valid when constructing 
    a download request for this specific dataset.

    Parameters
    ----------
    dataset_id : str
        The unique identifier of the target WEkEO dataset 
        (e.g., ``"EO:EEA:DAT:CLMS_HRVPP_VPP"``).
    wekeo_client : Client
        An authenticated instance of the WEkEO API client (typically from the ``hda`` library) 
        used to execute the HTTP GET request.

    Returns
    -------
    dict or None
        A dictionary containing the parsed JSON schema of queryable parameters. 
        Returns ``None`` if the API request fails or the dataset ID is invalid.

    Notes
    -----
    WEkEO dataset IDs frequently contain colons (``:``). This function automatically 
    URL-encodes the `dataset_id` using ``urllib.parse.quote`` before injecting it into 
    the API endpoint path to ensure the HTTP request is formatted correctly and doesn't 
    result in a malformed URL error.

    Examples
    --------
    Fetch the queryable schema for the Copernicus Land Monitoring Service (CLMS) 
    >Tree Cover and Forest (TCF) dataset:

    >>> from hda import Client
    >>> client = Client(user="username", password="password")
    >>> schema = get_dataset_variables(
    ...     dataset_id="EO:EEA:DAT:HRL:TCF",
    ...     wekeo_client=client
    ... )
    >>> if schema:
    ...     print(schema.keys())
    dict_keys(['type', 'title', 'properties', 'required', 'constraints'])
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

def build_data_inventory(
    api_schema: dict, 
    dataset_map: Dict[str, str], 
    wekeo_client: Any, 
    output_filepath: Optional[str] = None, 
    logger: Optional[logging.Logger] = None
) -> pd.DataFrame:
    """
    Crawls the WEkEO API testing every combination in the schema.
    Creates a distinct record for every available format to allow 
    strict intersection with user YAML configurations.
    
    Includes a secondary check to verify if the dataset supports 
    spatial subsetting via bounding box (bbox) queries, and writes
    the final inventory to disk if an output filepath is provided.

    Parameters
    ----------
    api_schema : dict
        A dictionary containing the categories, products, and available configurations.
    dataset_map : dict
        A mapping of abstract category names to actual WEkEO dataset IDs 
        (e.g., {"CRL": "EO:EEA:DAT:HRL:CRL"}).
    wekeo_client : Any
        The initialized WEkEO HDA API client used to perform the searches.
    output_filepath : str, optional
        The destination file path to save the resulting CSV. Directories will 
        be created if they do not exist. Default is None.
    logger : logging.Logger, optional
        The logger instance to use for recording execution messages. Default is None.

    Returns
    -------
    pd.DataFrame
        A DataFrame containing the successfully validated dataset configurations.

    Notes
    -----
    The `api_schema` passed to this function is typically extracted dynamically 
    using a function like `get_dataset_variables` (which queries the WEkEO API 
    for valid constraints). 
    
    For example:
    ``metadata_schema = dict(zip([k for k in DATASET_MAP.keys()], [get_dataset_variables(DATASET_MAP[k], c)["constraints"][0] for k in DATASET_MAP.keys()]))``
    
    The function expects this schema to be a nested dictionary structured exactly 
    like the API's constraint output, where top-level keys map to `DATASET_MAP` 
    keys, and sub-keys are literal `productType` strings containing lists of 
    valid parameters:

    .. code-block:: python

        {
            "TCF": {
                "Forest Type": {
                    "year": ["2015", "2018"],
                    "resolution": ["10m", "100m"]
                }
            },
            "CORINE": {
                "Land Cover": {
                    "format": ["GeoTiff100mt", "Vector"]
                }
            }
        }
    """
    inventory_records = []
    log_execution(logger, "Starting API Inventory Crawl. This might take a few minutes...", logging.INFO)
        
    for category, available_products in api_schema.items():
        dataset_id = dataset_map.get(category)
        if not dataset_id: 
            continue
            
        for product, info in available_products.items():
            
            # Determine Formats for the Loop 
            query_formats = info.get('format', [None])
            
            # Determine Years for the Loop & DataFrame
            if 'year' in info:
                query_years = [y for y in info['year'] if '-' not in y]
                if not query_years: continue 
                df_year_override = None
            else:
                found_years = re.findall(r'\b(19\d{2}|20\d{2})\b', product)
                if len(found_years) > 1:
                    continue # Skip multi-year change layers
                elif len(found_years) == 1:
                    query_years = [found_years[0]]
                    df_year_override = int(found_years[0])
                else:
                    query_years = [None]
                    df_year_override = None
                    
            # Determine Resolutions for the Loop & DataFrame
            if 'resolution' in info:
                query_res = info['resolution']
                df_res_override = None
            else:
                query_res = [None]
                df_res_override = '100m' if category == 'CORINE' else 'Unknown'

            # Build Combinations and Test
            combinations = list(itertools.product(query_res, query_years, query_formats))
            
            for res, year, fmt in combinations:
                query = generate_wekeo_query(
                    dataset_id=dataset_id,
                    product_type=product,
                    year=str(year) if year is not None else None,
                    data_format=fmt, 
                    resolution=res,
                    items_per_page=1
                )
                
                final_year = int(year) if year is not None else df_year_override
                final_res = res if res is not None else df_res_override
                
                try:
                    matches = wekeo_client.search(query)
                    num_results = len(getattr(matches, 'results', []))
                    fmt_log = f" | {fmt}" if fmt else ""
                    
                    if num_results > 0:
                        
                        # --- UPDATED BBOX LOGIC ---
                        supports_bbox = False
                        test_bbox = [4.0, 50.0, 5.0, 51.0] # Safe 1x1 degree bounding box in Central Europe
                        
                        bbox_query = generate_wekeo_query(
                            dataset_id=dataset_id,
                            product_type=product,
                            year=str(year) if year is not None else None,
                            data_format=fmt, 
                            resolution=res,
                            bbox=test_bbox,
                            items_per_page=1
                        )
                        
                        try:
                            # 1. Execute the query
                            bbox_matches = wekeo_client.search(bbox_query)
                            
                            # 2. Extract the number of items returned
                            bbox_num_results = len(getattr(bbox_matches, 'results', []))
                            
                            # 3. Only validate if it strictly returns more than 0 results
                            if bbox_num_results > 0:
                                supports_bbox = True
                            else:
                                log_execution(logger, f"BBOX yielded 0 results for {product}. Assuming spatial mismatch.", logging.DEBUG)
                                supports_bbox = False
                                
                        except Exception as bbox_error:
                            log_execution(logger, f"BBOX rejected for {product}: {bbox_error}", logging.DEBUG)
                            supports_bbox = False
                            
                        time.sleep(0.5) 
                        
                        inventory_records.append({
                            "category": category,
                            "dataset_id": dataset_id,
                            "productType": product,
                            "resolution": final_res,
                            "year": final_year,
                            "format": fmt, 
                            "items_found": num_results,
                            "bbox": supports_bbox  
                        })
                        log_execution(logger, f"Found: {product} | {final_year} | {final_res}{fmt_log} | bbox: {supports_bbox}", logging.INFO)
                    else:
                        log_execution(logger, f"Ghost Data: {product} | {final_year} | {final_res}{fmt_log}", logging.WARNING)
                        
                except Exception as e:
                    log_execution(logger, f"API Error on {product} {final_year}: {e}", logging.ERROR, exc_info=True)
                    
                # Respect API rate limits
                time.sleep(0.5) 
                
    # Finalize DataFrame
    df = pd.DataFrame(inventory_records)
    log_execution(logger, f"Crawl complete. Found {len(df)} valid datasets.", logging.INFO)
    
    # Save to disk if requested
    if output_filepath:
        try:
            output_dir = os.path.dirname(os.path.abspath(output_filepath))
            if output_dir:
                os.makedirs(output_dir, exist_ok=True)
                
            df.to_csv(output_filepath, index=False)
            log_execution(logger, f"Inventory successfully saved to: {output_filepath}", logging.INFO)
            
        except Exception as e:
            log_execution(logger, f"Failed to save inventory to '{output_filepath}': {e}", logging.ERROR, exc_info=True)
        
    return df
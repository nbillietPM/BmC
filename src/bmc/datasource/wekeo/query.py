import json
from hda import Client
import pandas as pd





def generate_wekeo_query(dataset_id: str, 
                         product_type: str, 
                         year: str = None, 
                         data_format: str = "GeoTiff100mt",
                         resolution: str = None,
                         bbox: list = None, 
                         items_per_page: int = 200, 
                         start_index: int = 0) -> dict:
    """
    Generates a WEkEO HDA API query dictionary flexibly for various dataset structures.
    
    Args:
        dataset_id (str): The specific EO dataset ID (e.g., 'EO:EEA:DAT:HRL:TCF').
        product_type (str): The specific layer or product type.
        year (str, optional): The single year or multi-year range.
        resolution (str, optional): The spatial resolution (e.g., '10m' or '100m').
        data_format (str, optional): File format, typically used only for CORINE (e.g., 'GeoTiff100mt').
        bbox (list, optional): [West, South, East, North] coordinates. Defaults to None (full extent).
        items_per_page (int, optional): Pagination limit. Defaults to 200.
        start_index (int, optional): Pagination start index. Defaults to 0.
        
    Returns:
        dict: The formatted query payload ready for the WEkEO API.
    """


    if dataset_id not in ["EO:EEA:DAT:CORINE","EO:EEA:DAT:HRL:SLF"]:
        # 1. Initialize with the keys that are mandatory for EVERY query
        query = {
            "dataset_id": dataset_id,
            "productType": product_type,
            "itemsPerPage": items_per_page,
            "startIndex": start_index
        }
        
        # 2. Inject the bounding box right after dataset_id if provided
        if bbox is not None:
            query_with_bbox = {"dataset_id": dataset_id, "bbox": bbox}
            query_with_bbox.update({k: v for k, v in query.items() if k != "dataset_id"})
            query = query_with_bbox

            
        if resolution is not None:
            query["resolution"] = resolution
            
        if year is not None:
            query["year"] = year
            
        return query
    elif dataset_id=="EO:EEA:DAT:CORINE":
        query = {
        "dataset_id": dataset_id,
        "productType": product_type,
        "format": data_format,
        "itemsPerPage": items_per_page,
        "startIndex": start_index}
        return query
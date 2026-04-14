
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
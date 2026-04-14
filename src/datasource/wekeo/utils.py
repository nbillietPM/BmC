import urllib
from typing import Optional, Dict, Any

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
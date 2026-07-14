import os
import itertools
from functools import lru_cache, partial
from concurrent.futures import ThreadPoolExecutor
import pandas as pd
import pygbif

@lru_cache(maxsize=None)
def lookup_backbone_single(
    name_query: str, 
    col_backbone: bool = False, 
    col_uuid: str = '7ddf754f-d193-4cc9-b351-99906754a03b'
) -> dict:
    """Execute a cached single-name taxonomic lookup against GBIF or CoL backbones.

    Queries either the standard GBIF Backbone Taxonomy or a specific external 
    checklist (like the Catalogue of Life) hosted on GBIF. Normalizes and 
    flattens the variations in API payload responses into a standardized dictionary schema.

    Parameters
    ----------
    name_query : str
        The scientific or taxonomic name string to resolve.
    col_backbone : bool, default False
        If True, routes the search through the Catalogue of Life dataset using 
        the generic text lookup engine. If False, uses the official strict GBIF 
        backbone matching engine.
    col_uuid : str, default '7ddf754f-d193-4cc9-b351-99906754a03b'
        The GBIF dataset UUID key assigned to the target checklist taxonomy 
        (defaults to the Catalogue of Life Backbone).

    Returns
    -------
    dict
        A flattened dictionary mapping unified taxonomic keys. Essential keys 
        include: 'matchType', 'usageKey', 'acceptedUsageKey', 'scientificName', 
        'rank', 'status', and 'lookupNames'.

    Examples
    --------
    >>> lookup_backbone_single("Vespa velutina", col_backbone=False)
    {'matchType': 'EXACT', 'usageKey': 1311477, ... 'lookupNames': 'Vespa velutina'}

    >>> lookup_backbone_single("Vespa velutina", col_backbone=True)
    {'matchType': 'EXACT', 'usageKey': '7G3C6', ... 'lookupNames': 'Vespa velutina'}
    """
    # Guard against empty, null, or whitespace-only queries
    if not name_query:
        return {"matchType": "NONE", "lookupNames": name_query}
        
    try:
        if col_backbone:
            # --- COL LOOKUP ---
            res = pygbif.species.name_lookup(q=name_query, datasetKey=col_uuid, limit=50)
            results = res.get('results', [])
            
            if not results:
                return {"matchType": "NONE", "note": "No CoL match", "lookupNames": name_query}
                
            exact_matches = []
            for r in results:
                canon = r.get('canonicalName', r.get('scientificName', '')).lower()
                sci = r.get('scientificName', '').lower()
                if name_query.lower() in [canon, sci]:
                    exact_matches.append(r)
                    
            if exact_matches:
                # Prioritize ACCEPTED, otherwise take the first exact match
                best = next((r for r in exact_matches if r.get('taxonomicStatus') == 'ACCEPTED'), exact_matches[0])
                match_type = "EXACT"
            else:
                # If no exact text match is found, fallback to the top fuzzy result
                best = results[0]
                match_type = "FUZZY"
                
            # Safely parse keys: Keep as int if numeric string, else preserve as string identifier
            raw_usage = best.get("taxonID", best.get("key"))
            raw_accepted = best.get("acceptedNameUsageID", best.get("acceptedKey", raw_usage))
            
            usage_key = int(raw_usage) if str(raw_usage).isdigit() else raw_usage
            accepted_key = int(raw_accepted) if str(raw_accepted).isdigit() else raw_accepted
                
            return {
                "matchType": match_type,
                "usageKey": usage_key,
                "acceptedUsageKey": accepted_key,
                "scientificName": best.get("scientificName"),
                "rank": best.get("rank"),
                "status": best.get("taxonomicStatus"),
                "lookupNames": name_query
            }
            
        else:
            # --- GBIF LOOKUP (FLATTENED) ---
            res = pygbif.species.name_backbone(name_query, strict=False)
            
            if not res:
                return {"matchType": "NONE", "note": "Empty GBIF response", "lookupNames": name_query}
            
            # Extract Match Type safely
            if "diagnostics" in res:
                match_type = res["diagnostics"].get("matchType", "NONE")
            else:
                match_type = res.get("matchType", "NONE")
                
            if match_type == "NONE":
                return {"matchType": "NONE", "note": "No GBIF match", "lookupNames": name_query}
                
            usage = res.get("usage", res)
            accepted = res.get("acceptedUsage", usage)
            
            # Enforce strict integer casting for native GBIF backbone keys
            raw_usage_key = usage.get("key", res.get("usageKey"))
            raw_accepted_key = accepted.get("key", res.get("acceptedUsageKey", res.get("acceptedKey")))
            
            return {
                "matchType": match_type,
                "usageKey": int(raw_usage_key) if raw_usage_key is not None else None,
                "acceptedUsageKey": int(raw_accepted_key) if raw_accepted_key is not None else None,
                "scientificName": usage.get("name", res.get("scientificName")),
                "rank": usage.get("rank", res.get("rank")),
                "status": usage.get("status", res.get("status")),
                "lookupNames": name_query
            }
            
    except Exception as e:
        return {"matchType": "NONE", "note": f"API Error: {e}", "lookupNames": name_query}

@lru_cache(maxsize=None)
def lookup_usage_single(taxon_key: int | str, 
                        col_backbone: bool = False, 
                        col_uuid: str = '7ddf754f-d193-4cc9-b351-99906754a03b') -> dict | None:
    """Execute a cached single-key GBIF or CoL name usage API lookup.

    Queries the GBIF Species API to resolve metadata for a unique key.
    Uses the specified dataset and sourceId if col_backbone is active, otherwise
    defaults to the standard GBIF backbone integer lookup.

    Parameters
    ----------
    taxon_key : int or str
        The raw unique identifier (GBIF integer or CoL string).
    col_backbone : bool, default False
        If True, queries the alternate CoL dataset UUID using the sourceId parameter.
    col_uuid : str, default '7ddf754f-d193-4cc9-b351-99906754a03b'
        The target dataset key to search when col_backbone is active.

    Returns
    -------
    dict or None
        The parsed JSON dictionary response containing taxonomic metadata if successful,
        otherwise None if the API request encounters an exception or no match.
    """
    try:
        if col_backbone:
            # Query CoL dataset via sourceId (returns a paginated list)
            res = pygbif.species.name_usage(datasetKey=col_uuid, sourceId=str(taxon_key))
            if res and "results" in res and len(res["results"]) > 0:
                return res["results"][0]
            return None
        else:
            # Standard GBIF backbone integer lookup (returns a flat dict)
            return pygbif.species.name_usage(key=int(taxon_key))
            
    except Exception:
        # Silently catch network timeouts, 404s, or casting errors
        return None

def lookup_backbone(
    name_list: list[str], 
    max_workers: int = 8, 
    col_backbone: bool = False, 
    col_uuid: str = '7ddf754f-d193-4cc9-b351-99906754a03b'
) -> list[dict]:
    """Execute high-speed parallel backbone lookups across an array of names.

    Leverages thread pool multitasking to evaluate lookups asynchronously 
    while preserving the performance benefits of underlying cache stores.

    Parameters
    ----------
    name_list : list of str
        An array of raw taxonomic strings to resolve.
    max_workers : int, default 8
        The explicit thread allocation ceiling dedicated to parallel executor operations.
    col_backbone : bool, default False
        A toggle parameter controlling whether queries hit CoL or GBIF backbones.
    col_uuid : str, default '7ddf754f-d193-4cc9-b351-99906754a03b'
        The target checklist dataset UUID parameter.

    Returns
    -------
    list of dict
        A sequence containing standardized lookup record dictionaries.
    """
    # Freeze the backbone/UUID parameters into a single-argument callable for executor mapping
    lookup_func = partial(lookup_backbone_single, col_backbone=col_backbone, col_uuid=col_uuid)
    
    with ThreadPoolExecutor(max_workers=max_workers) as ex:
        # executor.map guarantees results are yielded in exact input sequence order
        return list(ex.map(lookup_func, name_list))


def fetch_taxon_info(
    species_names: list[str], 
    out_file: str = "", 
    out_path: str = "", 
    mismatch_file: str = "", 
    keep_higherrank: bool = False,
    keep_fuzzy: bool = False,
    max_workers: int = 8,
    col_backbone: bool = False,
    col_uuid: str = '7ddf754f-d193-4cc9-b351-99906754a03b'
) -> tuple[pd.DataFrame, pd.DataFrame]:
    """Parse a list of taxonomic names and partition them by query accuracy.

    Batch processes name resolutions, splits successful classifications from 
    unreliable matches (fuzzy steps, typos, variant names, higher ranks), 
    normalizes structural reference keys safely, and optionally records metrics to disk.

    Parameters
    ----------
    species_names : list of str
        An array of raw names to validate and ingest.
    out_file : str, optional
        The filename target where the validated taxonomic DataFrame is saved as a CSV.
    out_path : str, optional
        The directory destination folder path where output CSV files are written.
    mismatch_file : str, optional
        The filename target where rejected names and error details are saved as a CSV.
    keep_higherrank : bool, default False
        If True, treats broad classifications above species level (e.g., Genus, Family) 
        as valid matches. If False, routes them to the mismatch data output bucket.
    keep_fuzzy : bool, default False
        If True, treats spelling corrections, typos, and fuzzy variants 
        (matchType 'FUZZY', 'VARIANT') as valid entries. If False, shifts them to mismatches.
    max_workers : int, default 8
        Thread optimization threshold configurations assigned to async lookups.
    col_backbone : bool, default False
        If True, formats outputs matching Catalogue of Life string index standards.
    col_uuid : str, default '7ddf754f-d193-4cc9-b351-99906754a03b'
        The reference target database checklist key constraint parameter.

    Returns
    -------
    taxonomic_df : pd.DataFrame
        Cleaned, tabular taxonomic data containing 100% accepted matches.
    mismatch_df : pd.DataFrame
        Tabular error logs containing names that failed processing thresholds.
    """
    # Defensive data polishing: strip whitespace, filter out null strings, and deduplicate values
    species_names = list(set(name.strip() for name in species_names if name and name.strip()))
    
    # Return empty structural DataFrames cleanly if no input values survive sanitation steps
    if not species_names:
        return pd.DataFrame(), pd.DataFrame()
    
    # Run the parallel asynchronous network lookup engine
    taxonomic_info = lookup_backbone(
        species_names, 
        max_workers=max_workers, 
        col_backbone=col_backbone, 
        col_uuid=col_uuid
    )
    
    # Ingest the flat normalized records directly into Pandas
    taxonomic_df = pd.DataFrame(taxonomic_info)
    
    # Structural fallback insurance: safeguard column indices against empty payload scenarios
    if "lookupNames" not in taxonomic_df.columns:
        taxonomic_df["lookupNames"] = species_names

    # Build dynamically configurable error criteria thresholds
    mismatchTypes = [
        "NONE",
        *(["FUZZY", "VARIANT"] if not keep_fuzzy else []),
        *(["HIGHERRANK"] if not keep_higherrank else [])
    ]
                     
    # Defend against missing column errors if the entire batch returns empty
    if "matchType" not in taxonomic_df.columns:
        taxonomic_df["matchType"] = "NONE"
        
    # Evaluate boolean mask index values matching specified error categories
    mismatch_indices = taxonomic_df["matchType"].isin(mismatchTypes)
    
    # Inform users via stdout logs when routing data modifications occur
    if set(mismatch_indices) == {False, True}:
        counts = "".join(f"\nCount {mt}: {sum(taxonomic_df['matchType'] == mt)}" for mt in set(taxonomic_df['matchType']))
        print(f"Mismatches encountered of type {set(taxonomic_df['matchType'])}{counts}")
        
    # Split the records into distinct valid vs error destination DataFrames
    mismatch_df = taxonomic_df[mismatch_indices].copy()
    taxonomic_df = taxonomic_df[~mismatch_indices].copy()

    # Apply data-type formatting rules to remaining valid rows
    if not taxonomic_df.empty:
        # Unify baseline lookup markers if dedicated alternative accepted usage keys are absent
        if "acceptedUsageKey" not in taxonomic_df.columns:
            taxonomic_df["acceptedUsageKey"] = taxonomic_df.get("usageKey", None)
        else:
            taxonomic_df["acceptedUsageKey"] = taxonomic_df["acceptedUsageKey"].fillna(taxonomic_df.get("usageKey"))
            
        # Enforce distinct structural typecasting rules relative to targeted backbones
        if col_backbone:
            # Catalogue of Life formats leverage alphanumeric string markers (e.g. '7G3C6')
            taxonomic_df["acceptedUsageKey"] = taxonomic_df["acceptedUsageKey"].astype(str)
        else:
            # Standard native GBIF formats strictly leverage integer index sequences
            taxonomic_df["acceptedUsageKey"] = taxonomic_df["acceptedUsageKey"].astype(int)

    # File serialization steps: Output data to disk if optional paths were specified
    if out_file != "":
        taxonomic_df.to_csv(os.path.join(out_path, out_file), index=False)
        
    if mismatch_file != "" and not mismatch_df.empty:
        # Verify targeted column metrics exist inside structural layout frames before writing data
        cols_to_save = [col for col in ["matchType", "scientificName", "lookupNames"] if col in mismatch_df.columns]
        mismatch_df[cols_to_save].to_csv(os.path.join(out_path, mismatch_file), index=False)
        
    return taxonomic_df, mismatch_df

def match_names_to_keys(names: list[str], 
                        max_workers: int = 8,
                        col_backbone: bool = False, 
                        col_uuid: str = '7ddf754f-d193-4cc9-b351-99906754a03b') -> dict[str, int | str | None]:
    """
    Resolve a list of taxonomic scientific names to their unique keys, supporting both 
    GBIF integer keys and CoL alphanumeric identifiers.
    """
    if not names:
        return []
        
    unique_names = list(set(n for n in names if n))
    lookup_func = partial(lookup_backbone_single, col_backbone=col_backbone, col_uuid=col_uuid)
    
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        results = [result.get("usageKey") for result in executor.map(lookup_func, unique_names)]
        
    return results

@lru_cache(maxsize=None)
def map_gbif_to_col(gbif_backbone_key: int) -> dict:
    """Map a GBIF Backbone Key to its Catalogue of Life (CoL) identifier and compare names.
    
    Queries the GBIF API twice: first to retrieve the native scientific name from the 
    GBIF backbone, and second via the '/related' endpoint to find the equivalent record 
    in the Catalogue of Life dataset. Uses an LRU cache to prevent duplicate network requests.

    Parameters
    ----------
    gbif_backbone_key : int
        The raw integer identifier for a taxon within the core GBIF backbone.

    Returns
    -------
    dict
        A dictionary containing the native GBIF scientific name, resolved CoL mapping 
        metadata, and associated identifiers. If a lookup fails, it returns partial 
        data alongside an 'error' description.

    Notes
    -----
    Understanding the different identifiers returned by this function:

    * **gbif_backbone_key (The Master Key)**: GBIF's universal identifier (e.g., `6` for 
      Plantae). GBIF uses this master key to stitch together occurrences from thousands 
      of different datasets (iNaturalist, eBird, museum collections, etc.) into a 
      single taxonomic tree.
    * **gbif_internal_col_key (The Local Index)**: GBIF treats the Catalogue of Life as 
      just another ingested dataset (UUID: 7ddf...). When imported, GBIF's SQL database 
      automatically assigns a local, internal integer to that specific record. You 
      cannot use this ID on the actual CoL website; it is strictly how GBIF keeps 
      track of the record internally. Even though the internal key (gbif_internal_col_key) 
      is useless outside of GBIF, it is highly recommended to keep it in your mapping dataframes. 
      If you ever need to debug your data, query GBIF for the exact snapshot of the CoL record 
      they are hosting, or trace back exactly where a taxonomic merge failed in the GBIF backend, 
      you will need that internal key to retrieve it.
    * **col_identifier (The True External ID)**: The official alphanumeric string 
      assigned natively by the Catalogue of Life (e.g., `5T6MX`). If you leave the 
      GBIF ecosystem and query the official CoL website or native API, this is the 
      only ID they will recognize.

    Examples
    --------
    >>> result = map_gbif_to_col(6)
    >>> print(result['gbif_scientific_name'])
    'Plantae'
    >>> print(result['col_identifier'])
    '5T6MX'
    """
    COL_DATASET_KEY = "7ddf754f-d193-4cc9-b351-99906754a03b"
    
    # 1. Fetch the native scientific name from the GBIF backbone
    gbif_url = f"https://api.gbif.org/v1/species/{gbif_backbone_key}"
    gbif_name = None
    
    try:
        gbif_response = requests.get(gbif_url, timeout=10)
        gbif_response.raise_for_status()
        gbif_name = gbif_response.json().get('scientificName')
    except requests.exceptions.RequestException as e:
        # We don't fatally fail here; we just note the missing name and keep going
        pass 
        
    # 2. Fetch the equivalent CoL mapping via the 'related' endpoint
    related_url = f"https://api.gbif.org/v1/species/{gbif_backbone_key}/related"
    params = {"datasetKey": COL_DATASET_KEY}
    
    try:
        col_response = requests.get(related_url, params=params, timeout=10)
        col_response.raise_for_status()
    except requests.exceptions.RequestException as e:
        return {
            "gbif_backbone_key": gbif_backbone_key, 
            "gbif_scientific_name": gbif_name,
            "error": f"CoL API request failed: {str(e)}"
        }
        
    results = col_response.json().get('results', [])
    
    if not results:
        return {
            "gbif_backbone_key": gbif_backbone_key, 
            "gbif_scientific_name": gbif_name,
            "error": "No Catalogue of Life mapping found"
        }
        
    # Extract structural identifiers and timestamps from the primary CoL match
    col_record = results[0]
    
    return {
        "gbif_backbone_key": gbif_backbone_key,
        "gbif_scientific_name": gbif_name,  # The original backbone name
        "col_scientific_name": col_record.get('scientificName'),  # The matched CoL name
        "gbif_internal_col_key": col_record.get('key'),
        "col_identifier": col_record.get('taxonID'), # The true external Catalogue of Life ID
        "last_crawled": col_record.get('lastCrawled'),
        "last_interpreted": col_record.get('lastInterpreted'),
        "modified": col_record.get('modified')
    }
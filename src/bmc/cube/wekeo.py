from bmc.cube.spatiotemporal import *
import yaml
import os
import json
from bmc.utils.logger import log_execution
import logging
import pandas as pd
from typing import Dict, Any, Optional, List


class wekeo_cube(spatiotemporal_cube):
    _PRODUCT_TYPE_MAPPINGS = {
    # --- CROPLAND (CRL) CATEGORICAL PRODUCTS ---
    "Crop Types": {
        1110: "Wheat", 1120: "Barley", 1130: "Maize", 1140: "Rice", 
        1150: "Other_Cereals", 1210: "Fresh_Vegetables", 1220: "Dry_Pulses", 
        1310: "Potatoes", 1320: "Sugar_Beet", 1410: "Sunflower", 1420: "Soybeans", 
        1430: "Rapeseed", 1440: "Flax_Cotton_Hemp", 2100: "Grapes", 2200: "Olives", 
        2310: "Fruits", 2320: "Nuts", 3100: "Unclassified_Arable", 
        3200: "Unclassified_Permanent"
    },
    
    "Secondary Crops Type": {
        1110: "Wheat", 1120: "Barley", 1130: "Maize", 1140: "Rice", 
        1150: "Other_Cereals", 1210: "Fresh_Vegetables", 1220: "Dry_Pulses", 
        1310: "Potatoes", 1320: "Sugar_Beet", 1410: "Sunflower", 1420: "Soybeans", 
        1430: "Rapeseed", 1440: "Flax_Cotton_Hemp", 2100: "Grapes", 2200: "Olives", 
        2310: "Fruits", 2320: "Nuts", 3100: "Unclassified_Arable", 
        3200: "Unclassified_Permanent"
    },
    
    "Fallow Land Presence": {
        1: "Fallow_Land"
    },

    # --- TREE COVER / FOREST (TCF) CATEGORICAL PRODUCTS ---
    "Forest Type": {
        1: "Broadleaved_Forest", 
        2: "Coniferous_Forest"
    },
    
    "Dominant Leaf Type": {
        1: "Broadleaved", 
        2: "Coniferous"
    },

    # --- GRASSLAND (GRA) CATEGORICAL PRODUCTS ---
    "Grassland": {
        1: "Grassland"
    },
    
    "Ploughing Indicator": {
        1: "Ploughed"
    },

    # --- IMPERVIOUSNESS (IMP) CATEGORICAL PRODUCTS ---
    "Impervious Built-up": {
        1: "Impervious_Built_up"
    },

    "Corine Land Cover":{
        1: "111_Continuous_Urban_Fabric",
        2: "112_Discontinuous_Urban_Fabric",
        3: "121_Industrial_Commercial",
        4: "122_Road_Rail_Networks",
        5: "123_Port_Areas",
        6: "124_Airports",
        7: "131_Mineral_Extraction",
        8: "132_Dump_Sites",
        9: "133_Construction_Sites",
        10: "141_Green_Urban_Areas",
        11: "142_Sport_Leisure_Facilities",
        12: "211_Non_Irrigated_Arable_Land",
        13: "212_Permanently_Irrigated_Land",
        14: "213_Rice_Fields",
        15: "221_Vineyards",
        16: "222_Fruit_Trees_Berry_Plantations",
        17: "223_Olive_Groves",
        18: "231_Pastures",
        19: "241_Annual_Crops_with_Permanent",
        20: "242_Complex_Cultivation_Patterns",
        21: "243_Agriculture_with_Natural_Vegetation",
        22: "244_Agro_Forestry_Areas",
        23: "311_Broad_Leaved_Forest",
        24: "312_Coniferous_Forest",
        25: "313_Mixed_Forest",
        26: "321_Natural_Grasslands",
        27: "322_Moors_Heathland",
        28: "323_Sclerophyllous_Vegetation",
        29: "324_Transitional_Woodland_Shrub",
        30: "331_Beaches_Dunes_Sands",
        31: "332_Bare_Rocks",
        32: "333_Sparsely_Vegetated_Areas",
        33: "334_Burnt_Areas",
        34: "335_Glaciers_Perpetual_Snow",
        35: "411_Inland_Marshes",
        36: "412_Peat_Bogs",
        37: "421_Salt_Marshes",
        38: "422_Salines",
        39: "423_Intertidal_Flats",
        40: "511_Water_Courses",
        41: "512_Water_Bodies",
        42: "521_Coastal_Lagoons",
        43: "522_Estuaries",
        44: "523_Sea_Ocean",
        48: "NoData", 
        128: "NoData", 
        254: "Unclassifiable",
        255: "Outside_Area",
        65535: "Outside_Area"
    }
}
    def __init__(self):
        super().__init__()
        self.wekeo_logger = self._setup_pipeline_logger("wekeo_logger", "wekeoCube.log")

    def _fetch_unpack_query(self, wekeo_query, hda_client, base_dir="wekeo", logger=None):
        """
        Fetches data using the hda_client, logs the progress, and extracts .tif files.
        """
        if not logger:
            logger = self.wekeo_logger

        dataset_id = wekeo_query.get("dataset_id", "UNKNOWN_DATASET")
        log_execution(logger, f"===={dataset_id}====", logging.INFO)
        
        pretty_query = json.dumps(wekeo_query, indent=4)
        indented_block = "\n    " + pretty_query.replace("\n", "\n    ")
        log_execution(logger, f"Executing search query...{indented_block}", logging.INFO)

        response = hda_client.search(wekeo_query)
        
        # --- BUG FIX: Check if response has results or is fundamentally empty ---
        # Many API clients evaluate to 'False' if empty, or contain a .results list
        try:
            is_empty = not response or len(getattr(response, 'results', [])) == 0
        except Exception:
            # Fallback if the object structure is completely unexpected
            is_empty = False 

        if is_empty:
            log_execution(logger, f"EMPTY RESULT FROM QUERY", logging.WARNING)
            return None
            
        download_dir = os.path.join(base_dir, dataset_id.replace(":", "_"))
        
        log_execution(logger, f"Downloading response to {download_dir}...", logging.INFO)
        response.download(download_dir=download_dir)
        
        gather_tifs_from_zips(
            source_directory=download_dir, 
            target_directory=os.path.join(download_dir, "tif_files"),
            logger=logger
        )
        return download_dir
    
    def _validate_config_products(
            self,
            config: Dict[str, Any], 
            inventory_df: pd.DataFrame, 
            logger: Optional[logging.Logger] = None
            ) -> bool:
        """
        Validates user-requested product types against the ground-truth API inventory.

        This function cross-references the `productTypes` defined in a user's 
        configuration dictionary against the available products cataloged in the 
        inventory DataFrame. It ensures that the pipeline only attempts to query 
        and download data that actually exists on the WEkEO servers.

        Parameters
        ----------
        config : dict
            The parsed user configuration dictionary. It is expected to contain a 
            top-level ``'datasets'`` key, which maps categories to their requested 
            ``'productTypes'``.
        inventory_df : pd.DataFrame
            The ground-truth inventory DataFrame (typically generated by the API
            crawler) containing a ``'productType'`` column.
        logger : logging.Logger, optional
            The logger instance to use for recording validation warnings and 
            success messages. Default is ``None``.

        Returns
        -------
        bool
            Returns ``True`` if every requested product strictly exists in the 
            inventory DataFrame. Returns ``False`` if one or more requested 
            products are missing or misspelled.
        """
        is_valid = True
        datasets_config = config.get('datasets', {})
        
        # Create a set of all available products for lightning-fast lookups
        available_products = set(inventory_df['productType'].dropna().unique())
        
        for category, cat_config in datasets_config.items():
            if not cat_config:
                continue
                
            requested_products = cat_config.get('productTypes', [])
            
            for product in requested_products:
                if product not in available_products:
                    # Assuming log_execution is defined elsewhere in your module
                    log_execution(
                        logger, 
                        f"Mismatch in [{category}]: Requested product '{product}' was not found in the inventory DataFrame.", 
                        logging.WARNING
                    )
                    is_valid = False
                    
        if is_valid:
            log_execution(logger, "Validation passed: All requested productTypes match the inventory.", logging.INFO)
            
        return is_valid

    def intersect_config_with_dataframe(
        self,
        config: Dict[str, Any],
        inventory_csv_path: str,
        logger: Optional[logging.Logger] = None
        ) -> List[Dict[str, Any]]:
        """
        Validates user configuration and generates an execution queue of WEkEO API queries.

        This orchestrator function acts as the bridge between a user's desired 
        configuration and the actual realities of the WEkEO database. It first validates 
        the requested products against the ground-truth inventory. It then filters the 
        inventory based on global spatial and temporal constraints, and dynamically 
        generates a list of valid, concrete API query payloads.

        The function handles two distinct dataset behaviors:
        1. **Format-based datasets** (e.g., CORINE): Filtered purely by requested format 
        (e.g., "GeoTiff100mt").
        2. **Resolution-based datasets** (e.g., HRL layers): Filtered by target spatial 
        resolution and specific product types.

        Parameters
        ----------
        config : dict
            The parsed user configuration dictionary containing 'global_temporal', 
            'spatial', and 'datasets' rules.
        inventory_csv_path : str
            The file path to the ground-truth inventory CSV.
        logger : logging.Logger, optional
            The logger instance to use for recording execution messages and skipped 
            data warnings. Default is ``None``.

        Returns
        -------
        list of dict
            A list of fully formed WEkEO HDA API query dictionaries, ready to be 
            passed to the API client for download.

        Raises
        ------
        ValueError
            If the initial configuration validation fails due to missing or 
            misspelled products in the ``config`` dictionary.

        See Also
        --------
        validate_config_products : The upstream function that validates the configuration.
        generate_wekeo_query : The downstream function used to construct individual payloads.
        """
        # 1. Load the ground truth
        df = pd.read_csv(inventory_csv_path)
        
        # 2. VALIDATE CONFIGURATION FIRST
        if not validate_config_products(config, df, logger):
            raise ValueError("Configuration validation failed due to missing or misspelled products. Check the logs.")
        
        # 3. Proceed with query generation if validation passes
        start_year = config.get('global_temporal', {}).get('start_year')
        end_year = config.get('global_temporal', {}).get('end_year')
        
        target_res = config.get('spatial', {}).get('query_res', '100m')
        user_bbox = config.get('spatial', {}).get('bbox')
        if user_bbox == "None": user_bbox = None
            
        execution_queue = []
        temporal_df = df[(df['year'] >= start_year) & (df['year'] <= end_year)]
        datasets_config = config.get('datasets', {})
        
        for category, cat_config in datasets_config.items():
            if not cat_config: continue
                
            # Handle "Format-based" datasets (e.g., CORINE)
            if 'format' in cat_config:
                if not cat_config.get('include', False): continue
                    
                target_format = cat_config.get('format')
                strict_df = temporal_df[(temporal_df['category'] == category) & 
                                        (temporal_df['format'] == target_format)]
                
                if strict_df.empty:
                    log_execution(logger, f"Skipped {category}: No {target_format} data for {start_year}-{end_year}.", logging.WARNING)
                    continue
                
                for _, row in strict_df.iterrows():
                    query = generate_wekeo_query(
                        dataset_id=row['dataset_id'],
                        product_type=row['productType'],
                        data_format=target_format,
                        bbox=user_bbox
                    )
                    execution_queue.append(query)
                    log_execution(logger, f"Queued: {row['productType']} ({row['year']}) strictly at {target_format}", logging.INFO)

            # Handle "Resolution-based" datasets (e.g., HRL Layers)
            elif 'productTypes' in cat_config:
                for product in cat_config['productTypes']:
                    strict_df = temporal_df[(temporal_df['productType'] == product) & 
                                            (temporal_df['resolution'] == target_res)]
                    
                    if strict_df.empty:
                        log_execution(logger, f"Skipped '{product}': No {target_res} data for {start_year}-{end_year}.", logging.WARNING)
                        continue
                        
                    valid_years = sorted(strict_df['year'].unique())
                    
                    for year in valid_years:
                        dataset_id = strict_df[strict_df['year'] == year]['dataset_id'].iloc[0]
                        
                        query = generate_wekeo_query(
                            dataset_id=dataset_id,
                            product_type=product,
                            year=str(year),
                            resolution=target_res,
                            bbox=user_bbox
                        )
                        execution_queue.append(query)
                        log_execution(logger, f"Queued: {product} ({year}) strictly at {target_res}", logging.INFO)
                    
        return execution_queue
    #def generate_data_cube(self, config_path, config_file, logger_name=f"{config_file.split(".")[0]}_wekeo_pipeline.log"):
    #    logger = self._setup_pipeline_logger(logger_name=logger_name, )



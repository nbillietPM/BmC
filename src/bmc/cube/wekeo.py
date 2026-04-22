from bmc.cube.spatiotemporal import *
import yaml
import os
import json
from bmc.utils.logger import log_execution
from bmc.datasource.wekeo.interface import generate_wekeo_query, get_dataset_variables, build_data_inventory
from bmc.utils.meta import fetch_meta
import logging
import pandas as pd
from typing import Dict, Any, Optional, List
import urllib
from pathlib import Path
import re
import xarray as xr
import rioxarray

class wekeo_cube(spatiotemporal_cube):
    """
    Child class dedicated to the ingestion and processing of WEkEO (Copernicus DIAS) ecological data.

    Inherits from `spatiotemporal_cube`. This class acts as the WEkEO-specific engine, 
    translating abstract configuration recipes into highly specific Harmonised Data 
    Access (HDA) API queries. It manages the downloading, unzipping, categorical 
    translation, spatial processing, and rigorous QA validation of high-resolution 
    layers like Corine Land Cover, High Resolution Layers (HRL), and crop masks.

    Attributes
    ----------
    DATASET_MAP : dict
        A registry mapping shorthand dataset codes (e.g., "TCF", "CORINE") to their 
        official WEkEO HDA dataset identifiers (e.g., "EO:EEA:DAT:HRL:TCF").
    CATEGORICAL_CLASSES : dict
        The master translation dictionary mapping numerical pixel values from discrete 
        raster products (like "Forest Type" or "Corine Land Cover 2018") into 
        human-readable class names.
    wekeo_logger : logging.Logger or None
        The active logger instance dedicated to the WEkEO execution pipeline.

    Methods
    -------
    get_class_label(product_type, pixel_value)
        Translates numerical pixel values from categorical layers into human-readable class names.
    intersect_config_with_dataframe(recipe, wekeo_client, api_schema, inventory_filename='wekeo_data_inventory.csv', logger=None)
        Generates an execution queue of valid WEkEO API queries by intersecting the user config with a dataset inventory.
    validate_datalake(base_dir, tolerance=0.0001, logger=None)
        Performs strict mathematical and ecological QA validations on all generated Data Lake datasets.
    build_wekeo_datalake(recipe, wekeo_client, inventory_filename='wekeo_data_inventory.csv', logger=None)
        The main pipeline wrapper: downloads data, aligns to grid, calculates derivatives, and exports COGs.

    Notes
    -----
    When adding new categorical datasets to the WEkEO pipeline, you must update 
    the `CATEGORICAL_CLASSES` dictionary to ensure downstream processes (like 
    fractional coverage calculations) can properly split and name the resulting 
    single-band TIFFs. 
    
    The QA Engine (`validate_datalake`) enforces cross-layer ecological physics. 
    For example, it guarantees that fractional subsets (like "Broadleaved Forest" 
    and "Coniferous Forest") never sum to greater than 100%, and verifies they 
    do not mathematically conflict with continuous baseline layers (like 
    "Tree Cover Density").
    """
    
    DATASET_MAP = {
        "TCF": "EO:EEA:DAT:HRL:TCF",
        "GRA": "EO:EEA:DAT:HRL:GRA",
        "IMP": "EO:EEA:DAT:HRL:IMP",
        "SLF": "EO:EEA:DAT:HRL:SLF",
        "CRL": "EO:EEA:DAT:HRL:CRL", 
        "CORINE": "EO:EEA:DAT:CORINE"
    } 
    # ---------------------------------------------------------
    # PRIVATE CLASS ATTRIBUTE: CORINE Level 3 Nomenclature
    # Defined first so it can be referenced in the main dictionary below.
    # ---------------------------------------------------------
    _CLC_LEVEL_3 = {
        # 1. Artificial surfaces
        111: "Continuous urban fabric", 112: "Discontinuous urban fabric",
        121: "Industrial or commercial units", 122: "Road and rail networks and associated land",
        123: "Port areas", 124: "Airports", 131: "Mineral extraction sites",
        132: "Dump sites", 133: "Construction sites", 141: "Green urban areas",
        142: "Sport and leisure facilities",
        
        # 2. Agricultural areas
        211: "Non-irrigated arable land", 212: "Permanently irrigated land",
        213: "Rice fields", 221: "Vineyards", 222: "Fruit trees and berry plantations",
        223: "Olive groves", 231: "Pastures", 241: "Annual crops associated with permanent crops",
        242: "Complex cultivation patterns", 243: "Land principally occupied by agriculture",
        244: "Agro-forestry areas",
        
        # 3. Forest and semi natural areas
        311: "Broad-leaved forest", 312: "Coniferous forest", 313: "Mixed forest",
        321: "Natural grasslands", 322: "Moors and heathland", 323: "Sclerophyllous vegetation",
        324: "Transitional woodland-shrub", 331: "Beaches, dunes, sands",
        332: "Bare rocks", 333: "Sparsely vegetated areas", 334: "Burnt areas",
        335: "Glaciers and perpetual snow",
        
        # 4. Wetlands
        411: "Inland marshes", 412: "Peat bogs", 421: "Salt marshes",
        422: "Salines", 423: "Intertidal flats",
        
        # 5. Water bodies
        511: "Water courses", 512: "Water bodies", 521: "Coastal lagoons",
        522: "Estuaries", 523: "Sea and ocean",
        
        # Exceptions
        48: "Unclassified / Unverifiable", 128: "No data", 255: "Outside area / No data"
    }

    # ---------------------------------------------------------
    # PUBLIC CLASS ATTRIBUTE: Master Categorical Dictionary
    # ---------------------------------------------------------
    CATEGORICAL_CLASSES = {
        # --- CRL: Crop Types ---
        "Crop Types": {
            0: "No crop / Non-agricultural",
            1110: "Wheat", 1120: "Barley", 1130: "Maize", 1140: "Rice", 
            1150: "Other_Cereals", 1210: "Fresh_Vegetables", 1220: "Dry_Pulses", 
            1310: "Potatoes", 1320: "Sugar_Beet", 1410: "Sunflower", 1420: "Soybeans", 
            1430: "Rapeseed", 1440: "Flax_Cotton_Hemp", 2100: "Grapes", 2200: "Olives", 
            2310: "Fruits", 2320: "Nuts", 3100: "Unclassified_Arable", 
            3200: "Unclassified_Permanent", 254: "Unclassifiable", 255: "No data"
        },
        "Secondary Crops Type": {
            0: "No crop / Non-agricultural",
            1110: "Wheat", 1120: "Barley", 1130: "Maize", 1140: "Rice", 
            1150: "Other_Cereals", 1210: "Fresh_Vegetables", 1220: "Dry_Pulses", 
            1310: "Potatoes", 1320: "Sugar_Beet", 1410: "Sunflower", 1420: "Soybeans", 
            1430: "Rapeseed", 1440: "Flax_Cotton_Hemp", 2100: "Grapes", 2200: "Olives", 
            2310: "Fruits", 2320: "Nuts", 3100: "Unclassified_Arable", 
            3200: "Unclassified_Permanent", 254: "Unclassifiable", 255: "No data"
        },

        # --- CORINE Land Cover (Referencing the private dictionary) ---
        "Corine Land Cover 1990": _CLC_LEVEL_3,
        "Corine Land Cover 2000": _CLC_LEVEL_3,
        "Corine Land Cover 2006": _CLC_LEVEL_3,
        "Corine Land Cover 2012": _CLC_LEVEL_3,
        "Corine Land Cover 2018": _CLC_LEVEL_3,

        # --- TCF: Tree Cover & Forest Products ---
        "Forest Type": {
            0: "Non-forest areas", 1: "Broadleaved forest", 2: "Coniferous forest",
            254: "Unclassifiable", 255: "No data"
        },
        "Dominant Leaf Type": {
            0: "All non-tree areas", 1: "Broadleaved", 2: "Coniferous",
            254: "Unclassifiable", 255: "No data"
        },

        # --- IMP: Imperviousness Built-up ---
        "Impervious Built-up": {
            0: "Non-built-up area", 1: "Built-up area", 
            254: "Unclassifiable", 255: "No data"
        },

        # --- Binary Mask Products (SLF, GRA, CRL, etc.) ---
        "Grassland": {0: "Non-grassland", 1: "Grassland", 254: "Unclassifiable", 255: "No data"},
        "Ploughing Indicator": {0: "No ploughing detected", 1: "Ploughing detected", 254: "Unclassifiable", 255: "No data"},
        "Forest Mask": {0: "Non-forest", 1: "Forest", 254: "Unclassifiable", 255: "No data"},
        "Crop Mask": {0: "Non-crop", 1: "Crop", 254: "Unclassifiable", 255: "No data"},
        "Small Woody Features": {0: "Non-SWF", 1: "Small Woody Feature presence", 254: "Unclassifiable", 255: "No data"},
        "Fallow Land Presence": {0: "No fallow land", 1: "Fallow land present", 254: "Unclassifiable", 255: "No data"}
    }
    
    def __init__(self):
        super().__init__()
        self.wekeo_logger = None

    def _fetch_unpack_query(self, wekeo_query: dict, wekeo_client: object, base_dir: str = "wekeo", logger: Optional[logging.Logger] = None) -> Optional[str]:
        """
        Executes a formatted query against the WEkEO Harmonized Data Access (HDA) API, 
        downloads the compressed results, and unpacks the internal .tif files.

        Parameters
        ----------
        wekeo_query : dict
            The JSON-like dictionary payload containing the specific dataset, product, 
            spatial, and temporal parameters to be requested from the API.
        wekeo_client : object
            An initialized and authenticated instance of the WEkEO HDA client used 
            to send the search and download requests.
        base_dir : str, optional
            The root directory where the downloaded data will be stored. A dataset-specific 
            subdirectory will be created inside this path. Default is "wekeo".
        logger : logging.Logger, optional
            The logger instance to use for recording execution progress, bugs, and API 
            responses. If None, it defaults to the class's internal `wekeo_logger`.

        Returns
        -------
        download_dir : str or None
            The absolute or relative path to the directory where the unzipped .tif files 
            have been saved. Returns None if the API responds with an empty result set.

        Notes
        -----
        This is a private method meant to be called sequentially during the pipeline's 
        execution phase. It includes a built-in safety net to catch silent empty responses 
        from the API (where the API returns 200 OK but provides no actual data links).

        See Also
        --------
        gather_tifs_from_zips : Utility function called internally to extract the files.
        """
        if not logger:
            logger = self.wekeo_logger
        # Extract the id from the query
        dataset_id = wekeo_query.get("dataset_id", "UNKNOWN_DATASET")
        log_execution(logger, f"===={dataset_id}====", logging.INFO)
    
        pretty_query = json.dumps(wekeo_query, indent=4)
        indented_block = "\n    " + pretty_query.replace("\n", "\n    ")
        log_execution(logger, f"Executing search query...{indented_block}", logging.INFO)
        # Get all matches from the query
        response = wekeo_client.search(wekeo_query)
        
        # Check if response has results or is fundamentally empty
        try:
            is_empty = not response or len(getattr(response, 'results', [])) == 0
        except Exception:
            is_empty = False 

        if is_empty:
            log_execution(logger, f"EMPTY RESULT FROM QUERY", logging.WARNING)
            return None
        
        log_execution(logger, f"Downloading response to {base_dir}...", logging.INFO)
        response.download(download_dir=base_dir) # Use base_dir directly
        
        # Extract the tif files from their zipped folders
        self.gather_tifs_from_zips(
            source_directory=base_dir, 
            target_directory=os.path.join(base_dir, "tif_files"),
            logger=logger
        )
        return base_dir

    def get_class_label(self, product_type: str, pixel_value: int) -> str:
        """
        Translates numerical pixel values from categorical raster layers into human-readable 
        class names using the master classification dictionary.

        Parameters
        ----------
        product_type : str
            The name of the WEkEO product being evaluated (e.g., "Crop Types", 
            "Corine Land Cover 2018", "Forest Type").
        pixel_value : int
            The integer value extracted from the raster pixel.

        Returns
        -------
        label : str
            The human-readable classification of the pixel (e.g., "Broadleaved forest"). 
            If the product is continuous or the specific value is unmapped, it returns 
            a fallback string notifying the user.

        Notes
        -----
        This method relies on the `CATEGORICAL_CLASSES` class attribute being properly 
        defined. It is primarily used downstream when interpreting model outputs or 
        summarizing spatial layers.
        """
        if product_type in self.CATEGORICAL_CLASSES:
            product_dict = self.CATEGORICAL_CLASSES[product_type]
            return product_dict.get(pixel_value, f"Unknown Value: {pixel_value}")
        else:
            return f"Product '{product_type}' is continuous or not defined."

    def _validate_config_products(
            self,
            recipe: Dict[str, Any], 
            inventory_df: pd.DataFrame, 
            logger: Optional[logging.Logger] = None
            ) -> bool:
        """
        Validates user-requested WEkEO product types against the ground-truth API inventory.

        This internal method acts as a pre-execution safeguard. It ensures that the 
        products requested in the YAML configuration actually exist in the compiled 
        metadata inventory before the pipeline attempts to query the API. It also 
        validates specific resolution overrides, issuing a warning if an exact 
        resolution match is unavailable.

        Parameters
        ----------
        recipe : dict
            A dictionary containing the parsed execution recipe. Expected to contain 
            nested configuration under `['sources']['wekeo']`.
        inventory_df : pandas.DataFrame
            The ground-truth API inventory loaded as a DataFrame. Must contain at least 
            the columns 'productType' and 'resolution'.
        logger : logging.Logger, optional
            The logger instance used to record validation steps, warnings for missing 
            resolutions, and errors for missing products. Default is None.

        Returns
        -------
        bool
            True if all requested products are found in the inventory, or if the WEkEO 
            pipeline is explicitly disabled. False if one or more requested products 
            are completely missing from the inventory. 
            
            Note: Invalid resolution targets only generate a warning to the logger and 
            do not cause the function to return False, as the resolver will fallback 
            to the best available option.
        """
        is_valid = True
        # Retrieve requested wekeo section
        wekeo_config = recipe.get('sources', {}).get('wekeo', {})
        
        if not wekeo_config.get('enabled', False):
            log_execution(logger, "WEkEO pipeline is disabled in recipe. Skipping validation.", logging.INFO)
            return True
        
        datasets_config = wekeo_config.get('datasets', {})
        # Generate list of products that can be requested
        available_products = set(inventory_df['productType'].dropna().unique())
        
        for category, cat_config in datasets_config.items():
            if not cat_config or not cat_config.get('include', False):
                continue
                
            requested_products = cat_config.get('productTypes', [])
            
            for product in requested_products:
                if product not in available_products:
                    log_execution(logger, f"Mismatch in [{category}]: Product '{product}' not in inventory.", logging.WARNING)
                    is_valid = False
                else:
                    # New: Check if the specific resolution override exists (if provided)
                    target_res = cat_config.get('query_resolution')
                    if target_res and target_res not in ['highest', 'lowest']:
                        prod_res = inventory_df[inventory_df['productType'] == product]['resolution'].unique()
                        if target_res not in prod_res:
                            log_execution(logger, f"Warning: Specific resolution '{target_res}' for '{product}' not in inventory. Resolver will pick best available.", logging.WARNING)          
        return is_valid

    def intersect_config_with_dataframe(
            self,
            recipe: Dict[str, Any],
            wekeo_client: Any,
            api_schema: dict,
            inventory_filename: str = "wekeo_data_inventory.csv",
            logger: Optional[logging.Logger] = None
        ) -> List[Dict[str, Any]]:
            """
            Generates an execution queue of valid WEkEO API queries by intersecting 
            the user configuration with a verified ground-truth dataset inventory.

            This method attempts to load a pre-compiled dataset inventory from the 
            bundled `meta/` directory. If the file is missing, it automatically falls 
            back to dynamically crawling the WEkEO API to reconstruct it. Once the 
            inventory is loaded, it checks if requested datasets support bounding box 
            queries, gracefully degrading to full-extent downloads if they do not.

            Parameters
            ----------
            recipe : Dict[str, Any]
                The parsed YAML configuration dictionary containing the user's spatial, 
                temporal, and dataset requirements.
            wekeo_client : Any
                The initialized WEkEO HDA API client, used to perform the fallback 
                inventory crawl if the static catalog is missing.
            api_schema : dict
                A dictionary containing the categories, products, and available 
                configurations (extracted via WEkEO constraints) used to build the 
                fallback inventory.
            inventory_filename : str, optional
                The name of the static metadata catalog file to look for in the `meta/` 
                directory. Default is 'wekeo_data_inventory.csv'.
            logger : logging.Logger, optional
                The logger instance used to record execution status and warnings. 
                Default is None.

            Returns
            -------
            List[Dict[str, Any]]
                A list of dictionary payloads formatted as valid WEkEO HDA queries, 
                ready to be passed to the API downloader.

            Raises
            ------
            ValueError
                If the requested configuration products cannot be validated against 
                the ground-truth inventory.
            """
            # Try fetching static meta, fallback to dynamic build
            try:
                df = fetch_meta(inventory_filename, logger=logger)
                log_execution(logger, f"Successfully loaded pre-compiled inventory: '{inventory_filename}'", logging.INFO)
                
            except FileNotFoundError:
                log_execution(logger, f"Inventory '{inventory_filename}' not found in meta directory. Constructing dynamically...", logging.WARNING)
                
                # Resolve the path to the meta directory so the new file is saved properly
                base_dir = Path(__file__).resolve().parents[3]
                target_path = base_dir / 'meta' / inventory_filename
                
                df = build_data_inventory(
                    api_schema=api_schema,
                    dataset_map=self.DATASET_MAP,
                    wekeo_client=wekeo_client,
                    output_filepath=str(target_path),
                    logger=logger
                )

            # Validate user config against the resulting DataFrame
            if not self._validate_config_products(recipe, df, logger):
                raise ValueError("Recipe validation failed. Check inventory logs.")
            
            wekeo_config = recipe.get('sources', {}).get('wekeo', {})
            global_q_res = wekeo_config.get('query_resolution', 'highest') 
            
            # Extract Global Constraints
            temp_cfg = recipe.get('temporal', {})
            start_year, end_year = temp_cfg.get('start_year'), temp_cfg.get('end_year')
            temporal_df = df[(df['year'] >= start_year) & (df['year'] <= end_year)]

            # Extract Target Bounding Box
            user_bbox = None
            raw_spatial = recipe.get('raw_config', {}).get('spatial', {})
            use_bbox = raw_spatial.get('use_bbox', False)
            
            if use_bbox:
                bbox_dict = recipe.get('spatial', {}).get('bbox')
                if bbox_dict:
                    user_bbox = [
                        bbox_dict['long_min'], 
                        bbox_dict['lat_min'], 
                        bbox_dict['long_max'], 
                        bbox_dict['lat_max']
                    ]
                else:
                    log_execution(logger, "Warning: 'use_bbox' is True, but no coordinates were found.", logging.WARNING)
                
            execution_queue = []
            datasets_config = wekeo_config.get('datasets', {})
            
            # 3. Build the specific execution queue
            for category, cat_config in datasets_config.items():
                if not cat_config or not cat_config.get('include', False): 
                    continue
                    
                # Logic for Format-based layers (CORINE)
                if 'format' in cat_config:
                    target_format = cat_config.get('format')
                    strict_df = temporal_df[(temporal_df['category'] == category) & 
                                            (temporal_df['format'] == target_format)]
                    
                    for _, row in strict_df.iterrows():
                        supports_bbox = bool(row['bbox'])
                        query_bbox = user_bbox if supports_bbox else None
                        
                        if user_bbox and not supports_bbox:
                            log_execution(logger, f"Note: API BBOX not supported for '{row['productType']}'. Will download full extent.", logging.INFO)

                        execution_queue.append(generate_wekeo_query(
                            dataset_id=row['dataset_id'], 
                            product_type=row['productType'],
                            data_format=target_format, 
                            bbox=query_bbox
                        ))
                        log_execution(logger, f"Queued: {row['productType']} ({row['year']}) at {target_format}", logging.INFO)

                # Logic for Resolution-based layers (HRL, etc.)
                elif 'productTypes' in cat_config:
                    strategy = cat_config.get('query_resolution', global_q_res)
                    
                    for product in cat_config['productTypes']:
                        product_df = temporal_df[temporal_df['productType'] == product]
                        
                        if product_df.empty:
                            log_execution(logger, f"Skipped '{product}': No data found for specified years.", logging.WARNING)
                            continue
                            
                        # Resolve raw resolution to query
                        available_res = product_df['resolution'].unique()
                        resolved_res = self._resolve_query_resolution(strategy, available_res, logger)
                        
                        final_df = product_df[product_df['resolution'] == resolved_res]
                        
                        for year in sorted(final_df['year'].unique()):
                            row = final_df[final_df['year'] == year].iloc[0]
                            
                            supports_bbox = bool(row['bbox'])
                            query_bbox = user_bbox if supports_bbox else None
                            
                            if user_bbox and not supports_bbox:
                                log_execution(logger, f"Note: API BBOX not supported for '{product}' ({year}). Will download full extent.", logging.INFO)

                            execution_queue.append(generate_wekeo_query(
                                dataset_id=row['dataset_id'], 
                                product_type=product,
                                year=str(year), 
                                resolution=resolved_res, 
                                bbox=query_bbox
                            ))
                            log_execution(logger, f"Queued: {product} ({year}) at {resolved_res}", logging.INFO)
                      
            return execution_queue
    
    def validate_datalake(
        self, 
        base_dir: str, 
        tolerance: float = 0.0001, 
        logger: Optional[logging.Logger] = None
    ) -> bool:
        """
        Dynamically crawls a structured data lake and performs strict mathematical 
        and ecological QA validations on all detected continuous and categorical datasets.

        This function acts as the final gatekeeper in the data pipeline, ensuring that 
        the spatial resampling and aggregations (performed by GDAL) did not introduce 
        mathematical artifacts, NoData poisoning, or physical impossibilities.

        Parameters
        ----------
        base_dir : str
            The root directory of the generated data lake to be validated.
        tolerance : float, optional
            The acceptable floating-point tolerance for mathematical comparisons. 
            GDAL aggregations often introduce microscopic drift (e.g., summing to 1.0000001). 
            Default is 0.0001.
        logger : logging.Logger, optional
            The logger instance used to record validation progress and flag violations. 
            Default is None.

        Returns
        -------
        bool
            True if all QA tests pass with zero violations. False if any pixels 
            violate the mathematical or ecological rules.

        Notes
        -----
        The validation engine performs three primary categories of tests:

        1. Categorical Fractional Boundaries & Sums:
           - Bounds Check: Ensures all fractional pixels are bounded strictly between 0.0 and 1.0.
           - Mutually Exclusive Summation: Sums all classes within a single product 
             (e.g., Broadleaved + Coniferous) and verifies they do not exceed 100%. 
             This prevents spatial double-counting.

        2. Continuous Mathematical Inequalities:
           - Validates the statistical logic of the GDAL warp engine by ensuring the 
             aggregated pixels obey variance mathematics: Min <= Average <= RMS <= Max.

        3. Cross-Layer Ecological Logic:
           - Validates physical reality across completely different datasets. 
           - Example: If the continuous "Tree Cover Density" layer states the Maximum 
             density for a 100m pixel is 0%, the fractional "Forest Type" layer must 
             allocate exactly 0% to both Broadleaved and Coniferous classes.
        """
        import re

        base_path = Path(base_dir)
        log_execution(logger, f"\n=== Initiating Global QA Sweep on: {base_path.absolute()} ===", logging.INFO)
        
        if not base_path.exists():
            log_execution(logger, f"[!] Target directory does not exist: {base_dir}", logging.ERROR)
            return False

        all_passed = True

        # Discover and Group Data by Product and Year
        lake_catalog = {}
        """
        glob (short for "global") refers to the process of using wildcard characters to find specific files 
        or directories based on patterns, rather than typing out exact filenames. rglob (short for recursive glob) 
        lies in how deep they look into your folders. While both are used to find files matching a pattern, they 
        differ in their "scope" of vision. rglob (Recursive): It is deep. It automatically searches the current directory 
        and every single sub-directory beneath it, no matter how many levels down they go.
        """
        for tif_path in base_path.rglob("*.tif"):
            # Separate the path into the different hierarchical levels
            parts = tif_path.parts
            # General structure is 'lake_name/dataset_id/productType/aggregation/*tif'
            if len(parts) < 4: continue
            
            category, product, aggregation = parts[-4], parts[-3], parts[-2]
            
            # Regular expression match to fit anything 19XX or 20XX where X can be any digit
            year_match = re.search(r'(19\d{2}|20\d{2})', tif_path.name)
            year = int(year_match.group(1)) if year_match else "AllYears"
            
            if category not in lake_catalog: lake_catalog[category] = {}
            if product not in lake_catalog[category]: lake_catalog[category][product] = {}
            if year not in lake_catalog[category][product]: lake_catalog[category][product][year] = {}
            
            if aggregation == 'coverage':
                if 'coverage' not in lake_catalog[category][product][year]:
                    lake_catalog[category][product][year]['coverage'] = []
                lake_catalog[category][product][year]['coverage'].append(tif_path)
            else:
                lake_catalog[category][product][year][aggregation] = tif_path

        def load_da(path):
            return rioxarray.open_rasterio(path, masked=True, chunks={'x': 2048, 'y': 2048}).squeeze()

        # Execute Validations
        # Perform iteration per product to validate all time slices consecutively
        for category, products in lake_catalog.items():
            for product, years in products.items():
                for year, layers in years.items():
                    log_execution(logger, f"\n--- Validating: {category} | {product} | {year} ---", logging.INFO)
                    
                    # ==========================================
                    # TEST A: Categorical Fractional Boundaries & Sums
                    # ==========================================
                    if 'coverage' in layers:
                        fraction_das = []
                        for frac_path in layers['coverage']:
                            da = load_da(frac_path)
                            # Load in the data array associated with a fraction
                            fraction_das.append(da)
                            class_name = frac_path.stem.split(f"{year}_")[-1] 
                            
                            # Identify pixels where the value dips below 0 or above 1
                            # generates binary masks that sum up to the number of pixels that violate the rule
                            under_0 = (da < -tolerance).sum().compute().item()
                            over_1 = (da > (1.0 + tolerance)).sum().compute().item()
                            
                            if under_0 == 0 and over_1 == 0:
                                log_execution(logger, f"    -> {class_name} Bounds [0,1]: ✅", logging.INFO)
                            else:
                                log_execution(logger, f"    -> {class_name} Bounds [0,1]: ❌ ({under_0} < 0, {over_1} > 1)", logging.WARNING)
                                all_passed = False
                        
                        if len(fraction_das) > 1:
                            # Fill in NaN values with 0 (invalid to sum numbers with a NaN) and sum arrays pixel wise
                            total_coverage = sum([da.fillna(0) for da in fraction_das])
                            # Valid mask serves as safety net to differentiate between valid 0's and NaN values and will return a 
                            # Binary mask that defines regions where there is actual information
                            valid_mask = sum([da.notnull() for da in fraction_das]) > 0
                            # Verifies the existence of pixels where the total sum exceeds 100%
                            over_100 = ((total_coverage > (1.0 + tolerance)) & valid_mask).sum().compute().item()
                            
                            if over_100 == 0:
                                log_execution(logger, f"    -> Aggregate Sum <= 100%: ✅", logging.INFO)
                            else:
                                log_execution(logger, f"    -> Aggregate Sum <= 100%: ❌ ({over_100} pixels violated)", logging.WARNING)
                                all_passed = False

                    # ==========================================
                    # TEST B: Continuous Mathematical Inequalities
                    # ==========================================
                    else:
                        da_min = load_da(layers.get('min')) if 'min' in layers else None
                        da_avg = load_da(layers.get('average')) if 'average' in layers else None
                        da_rms = load_da(layers.get('rms')) if 'rms' in layers else None
                        da_max = load_da(layers.get('max')) if 'max' in layers else None
                        
                        if da_min is not None and da_avg is not None:
                            min_gt_avg = (da_min > (da_avg + tolerance)).sum().compute().item()
                            if min_gt_avg == 0:
                                log_execution(logger, "    -> Math Check (Min <= Avg): ✅", logging.INFO)
                            else:
                                log_execution(logger, f"    -> Math Check (Min <= Avg): ❌ ({min_gt_avg} violations)", logging.WARNING)
                                all_passed = False
                        
                        if da_avg is not None and da_rms is not None:
                            avg_gt_rms = (da_avg > (da_rms + tolerance)).sum().compute().item()
                            if avg_gt_rms == 0:
                                log_execution(logger, "    -> Math Check (Avg <= RMS): ✅", logging.INFO)
                            else:
                                log_execution(logger, f"    -> Math Check (Avg <= RMS): ❌ ({avg_gt_rms} violations)", logging.WARNING)
                                all_passed = False

                        if da_rms is not None and da_max is not None:
                            rms_gt_max = (da_rms > (da_max + tolerance)).sum().compute().item()
                            if rms_gt_max == 0:
                                log_execution(logger, "    -> Math Check (RMS <= Max): ✅", logging.INFO)
                            else:
                                log_execution(logger, f"    -> Math Check (RMS <= Max): ❌ ({rms_gt_max} violations)", logging.WARNING)
                                all_passed = False

        # ==========================================
        # TEST C: Global Cross-Layer Ecological Logic
        # ==========================================
        log_execution(logger, "\n--- Running Cross-Layer Dependencies ---", logging.INFO)
        
        if 'TCF' in lake_catalog and 'Tree_Cover_Density' in lake_catalog['TCF'] and 'Forest_Type' in lake_catalog['TCF']:
            for year in lake_catalog['TCF']['Tree_Cover_Density'].keys():
                if year in lake_catalog['TCF']['Forest_Type']:
                    max_tcd_path = lake_catalog['TCF']['Tree_Cover_Density'][year].get('max')
                    forest_coverages = lake_catalog['TCF']['Forest_Type'][year].get('coverage', [])
                    
                    if max_tcd_path and forest_coverages:
                        tcd_max = load_da(max_tcd_path)
                        total_forest = sum([load_da(p).fillna(0) for p in forest_coverages])
                        
                        impossible_forests = ((total_forest > 0.01) & (tcd_max == 0)).sum().compute().item()
                        if impossible_forests == 0:
                            log_execution(logger, f"  [TCF] {year} | 0% Max Density -> 0% Forest Fraction: ✅", logging.INFO)
                        else:
                            log_execution(logger, f"  [TCF] {year} | 0% Max Density -> 0% Forest Fraction: ❌ ({impossible_forests} violations)", logging.WARNING)
                            all_passed = False

        if 'IMP' in lake_catalog and 'Imperviousness_Density' in lake_catalog['IMP'] and 'Impervious_Built-up' in lake_catalog['IMP']:
            for year in lake_catalog['IMP']['Imperviousness_Density'].keys():
                 if year in lake_catalog['IMP']['Impervious_Built-up']:
                    max_imp_path = lake_catalog['IMP']['Imperviousness_Density'][year].get('max')
                    builtup_coverages = lake_catalog['IMP']['Impervious_Built-up'][year].get('coverage', [])
                    
                    if max_imp_path and builtup_coverages:
                        imp_max = load_da(max_imp_path)
                        total_built = sum([load_da(p).fillna(0) for p in builtup_coverages])
                        
                        impossible_built = ((total_built > 0.01) & (imp_max == 0)).sum().compute().item()
                        if impossible_built == 0:
                            log_execution(logger, f"  [IMP] {year} | 0% Max Density -> 0% Built-up Fraction: ✅", logging.INFO)
                        else:
                            log_execution(logger, f"  [IMP] {year} | 0% Max Density -> 0% Built-up Fraction: ❌ ({impossible_built} violations)", logging.WARNING)
                            all_passed = False

        status_msg = "PASSED" if all_passed else "FAILED (See Warnings)"
        log_execution(logger, f"\n=== Global QA Sweep Complete: {status_msg} ===", logging.INFO if all_passed else logging.WARNING)
        
        return all_passed

    def build_wekeo_datalake(
        self, 
        recipe: Dict[str, Any], 
        wekeo_client: Any, 
        inventory_filename: str = "wekeo_data_inventory.csv",
        logger: Optional[logging.Logger] = None
    ) -> List[str]:
        """
        Backend Ingestion Engine: Downloads WEkEO data, perfectly aligns it to a 
        master spatial grid, calculates statistical derivatives, and exports everything 
        to Cloud Optimized GeoTIFFs (COGs) inside a structured Data Lake directory hierarchy.

        Parameters
        ----------
        recipe : Dict[str, Any]
            A configuration dictionary containing the pipeline parameters, including 
            target spatial grids, temporal bounds, resampling strategies, and output paths.
        wekeo_client : Any
            The initialized WEkEO Harmonised Data Access (HDA) API client used 
            to execute spatial/temporal searches and download raw zip files.
        inventory_filename : str, optional
            The file name of the ground-truth WEkEO dataset inventory CSV, used to 
            validate configurations and generate the query execution queue. 
            Default is "wekeo_data_inventory.csv".
        logger : logging.Logger, optional
            A logger instance for recording execution progress, warnings, and errors. 
            If None, a pipeline-specific logger is automatically configured.

        Returns
        -------
        List[str]
            A list of file paths pointing to the successfully generated Cloud Optimized 
            GeoTIFFs (COGs) within the Data Lake structure.
        """
        import gc
        import os
        import sys
        import shutil

        raw_dir = recipe["paths"]["raw_dir"]
        base_dir = recipe["paths"]["base_dir"] 
        
        cube_name = recipe.get('raw_config', {}).get('cube_name', 'datalake_build')
        log_filepath = os.path.join(base_dir, f"{cube_name}.log")
        
        active_logger = self._setup_pipeline_logger(logger_name=cube_name, log_filepath=log_filepath)
        
        log_execution(active_logger, f"Starting Data Lake Ingestion for: {cube_name}", logging.INFO)

        target_grid_key = recipe["spatial"]["target_grid_key"]
        resampling_config = recipe["spatial"]["resampling_strategies"]
        
        try:
            target_grid, target_res = target_grid_key.split("_", 1)
        except ValueError:
            target_grid, target_res = target_grid_key, "unknown"
            
        wekeo_config = recipe["sources"].get("wekeo", {})
        if not wekeo_config.get("enabled", False):
            return []

        #Generate the metadata schema in case the data inventory is absent from the repository
        metadata_schema = dict(zip(
            [key for key in self.DATASET_MAP.keys()],
            [get_dataset_variables(self.DATASET_MAP[key], wekeo_client)["constraints"][0] for key in self.DATASET_MAP.keys()]
        ))

        # Generate the different queries based on the recipe
        execution_queue = self.intersect_config_with_dataframe(
            recipe=recipe, 
            wekeo_client=wekeo_client, 
            api_schema=metadata_schema, 
            logger=active_logger
        )

        if not execution_queue:
            return []

        generated_cogs = []

        for query in execution_queue:
            product_type = query.get("productType", "Unknown_Product")
            clean_product = product_type.replace(' ', '_').replace('/', '_')
            year = query.get("year", "AllYears")
            
            dataset_id = query.get("dataset_id", "")
            dataset_cat = dataset_id.split(":")[-1] if dataset_id else "UnknownDataset"
            
            safe_name = f"{clean_product}_{year}"
            log_execution(active_logger, f"\n{'='*50}\nBuilding Data Lake Layer: {safe_name}\n{'='*50}", logging.INFO)

            # Force the clean 'wekeo/dataset_id/product_layer' schema
            safe_dataset_id = dataset_id.replace(":", "_")
            isolated_raw_dir = os.path.join(raw_dir, "wekeo", safe_dataset_id, clean_product)
            os.makedirs(isolated_raw_dir, exist_ok=True)
            
            download_dir = self._fetch_unpack_query(
                wekeo_query=query, 
                wekeo_client=wekeo_client, # <-- Fixed to match the expected keyword 
                base_dir=isolated_raw_dir, 
                logger=active_logger
            )

            # Force the code to continue even when fetch_unpack returns None
            if not download_dir:
                continue

            tif_folder = os.path.join(download_dir, "tif_files")
            vrt_path = os.path.join(tif_folder, f"{safe_name}.vrt")

            # Build the VRT based on the tif files being present and force continue if nothing is present
            if not self.build_virtual_mosaic(tif_folder, vrt_path, active_logger):
                continue

            # Categorical Processing (Single-Band Fractional Coverages)
            if product_type in self.CATEGORICAL_CLASSES:
                discrete_strat = resampling_config.get('discrete', 'coverage')
                if discrete_strat == 'coverage':
                    aggr_name = "coverage"
                    layer_dir = os.path.join(base_dir, dataset_cat, clean_product, aggr_name)
                    os.makedirs(layer_dir, exist_ok=True)
                    # Consitent filename schema
                    file_prefix = f"{clean_product}_{year}_{target_grid}_{target_res}"
                    
                    log_execution(active_logger, "Processing as Single-Band Categorical Fractions...", logging.INFO)
                    
                    raw_class_dict = self.CATEGORICAL_CLASSES[product_type]
                    class_mapping = {k: v for k, v in raw_class_dict.items() if k not in [48, 128, 254, 255]}
                    target_classes = list(class_mapping.keys())
                    
                    try:
                        fraction_cogs = self.process_virtual_mosaic(
                            vrt_path=vrt_path, 
                            strategy='coverage', 
                            grid_name=target_grid_key,
                            output_dir_or_file=layer_dir, 
                            logger=active_logger, 
                            class_values=target_classes,
                            class_mapping=class_mapping,
                            file_prefix=file_prefix 
                        )
                        
                        generated_cogs.extend(fraction_cogs)
                        # After data has been safely written, force interpreter to free up RAM resources
                        gc.collect()
                        
                    except Exception as e:
                        log_execution(active_logger, f"Failed fractional coverage for {safe_name}: {e}", logging.ERROR)

            # Continuous Processing (Multiple Statistics)
            else:
                cont_stats = resampling_config.get('continuous', ['average'])
                if isinstance(cont_stats, str):
                    cont_stats = [cont_stats]

                for stat in cont_stats:
                    aggr_name = stat.lower()
                    layer_dir = os.path.join(base_dir, dataset_cat, clean_product, aggr_name)
                    os.makedirs(layer_dir, exist_ok=True)
                    
                    file_name = f"{clean_product}_{year}_{target_grid}_{target_res}_{aggr_name}.tif"
                    final_cog_path = os.path.join(layer_dir, file_name)
                    
                    log_execution(active_logger, f"Calculating: {stat.upper()}", logging.INFO)
                    try:
                        temp_tif_path = os.path.join(layer_dir, f"temp_{file_name}")
                        
                        result_xr = self.process_virtual_mosaic(
                            vrt_path=vrt_path, strategy='reproject', grid_name=target_grid_key,
                            output_dir_or_file=temp_tif_path, logger=active_logger, resample_keyword=stat
                        )
                        
                        self.export_to_cog(result_xr, final_cog_path, logger=active_logger)
                        generated_cogs.append(final_cog_path)
                        
                        result_xr.close() 
                        del result_xr
                        
                        if os.path.exists(temp_tif_path):
                            os.remove(temp_tif_path)
                            
                        gc.collect()
                        
                    except Exception as e:
                        log_execution(active_logger, f"Failed statistic '{stat}' for {safe_name}: {e}", logging.ERROR)

        # Clean up disc space if keep_raw is set to False in recipe
        self.cleanup_raw_storage(recipe=recipe, logger=active_logger)
        # Validate the logic and consistency of the generated datalake
        self.validate_datalake(base_dir=base_dir, logger=active_logger)

        log_execution(active_logger, "\n=== WEkEO Data Lake Ingestion Complete ===", logging.INFO)
        return generated_cogs
import os
import logging
import pandas as pd
import xarray as xr
import rioxarray
import rasterio
from rasterio.warp import transform_bounds
from rasterio.enums import Resampling
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Dict, List, Tuple, Optional, Any

from bmc.cube.spatiotemporal import spatiotemporal_cube
from bmc.utils.logger import log_execution
from bmc.utils.spatial import build_envelope_from_file
from bmc.utils.io import parallel_fetch_rasters


class chelsa_cube(spatiotemporal_cube):
    """
    Child class dedicated to the single-pass generation of high-resolution 
    CHELSA V2.1 spatiotemporal data cubes.

    Inherits from `spatiotemporal_cube`. This processing engine utilizes external 
    generalized spatial and I/O utilities to dynamically calculate safe fetching 
    envelopes, stream raw Cloud Optimized GeoTIFF matrices, structure complex 
    MultiIndex CMIP6 metadata, and utilize the parent class affine reprojection 
    engine to guarantee perfect grid alignment across all environmental data sources.

    Attributes
    ----------
    resample_rules : dict
        A strict internal routing dictionary mapping physical environmental variables 
        (e.g., 'tas', 'pr', 'bio12') to their physically appropriate GDAL resampling 
        algorithms (e.g., continuous gradients via `bilinear` vs. volume conservation 
        via `average`).

    Methods
    -------
    intersect_config(recipe, catalog_df, logger=None)
        Filters a master CHELSA inventory catalog against user-defined temporal, spatial, 
        and variable constraints to generate a precise execution queue.
    process_cube(recipe, catalog_df, max_workers=10, logger=None)
        The primary orchestration loop: fetches remote COGs, harmonizes Z-axis data, 
        performs disk-cached GDAL affine reprojection, and compiles final datasets.
    _resolve_chelsa_target_grid(spatial_cfg, logger=None)
        Translates user spatial requests into physically safe native target grids, 
        intercepting and safely degrading sub-kilometer requests (e.g., 100m to 1km).
    _parse_worker_metadata(row, da)
        Translates flat tabular metadata (CMIP6 ensembles, SSP scenarios, dates) 
        into rich, multi-dimensional Xarray coordinates to expand 2D arrays to 3D.

    Notes
    -----
    When extracting raw spatial bounds, this class explicitly pads the spatial envelope 
    with a 5-pixel buffer. This physical "scaffolding" prevents edge-starvation 
    anomalies during the GDAL affine reprojection phase. Once the matrix is snapped 
    to the master grid, the excess border is mathematically clipped away to perfectly 
    match the user's requested bounding box.
    
    Furthermore, because standard GeoTIFF I/O operations inherently flatten and destroy 
    complex Z-axis Xarray dimensions, this engine utilizes a "Metadata Catch and Release" 
    architecture. Complex coordinate frameworks (such as CMIP6 ensembles or time series) 
    are stripped immediately prior to disk-cached warping, and surgically re-injected 
    upon reload to preserve data integrity for downstream MultiIndex compilation.
    """

    def __init__(self, **kwargs):
        """
        Initializes the CHELSA spatiotemporal data cube engine rules.

        Parameters
        ----------
        **kwargs : dict
            Optional keyword arguments passed directly to the parent class.
        """
        # Pass any external keyword arguments up to the spatiotemporal_cube parent class
        super().__init__(**kwargs)

        # Define a strict dictionary mapping physical variables to GDAL interpolation rules
        self.resample_rules = {
            'tas': 'bilinear',       # Air temperature requires smooth bilinear gradients
            'tasmax': 'bilinear',    # Maximum temperature requires smooth gradients
            'tasmin': 'bilinear',    # Minimum temperature requires smooth gradients
            'sfcWind': 'bilinear',   # Wind speed requires smooth gradients
            'vpd': 'bilinear',       # Vapor pressure deficit requires smooth gradients
            'pr': 'average',         # Precipitation flux requires volume-conserving averages
            'rsds': 'average',       # Solar radiation flux requires volume-conserving averages
            'clt': 'average',        # Cloud cover percentage requires cell-area averages
            'hurs': 'average',       # Relative humidity percentage requires cell-area averages
            'pet': 'average'         # Potential evapotranspiration flux requires averages
        }

    def _resolve_chelsa_target_grid(
        self, 
        spatial_cfg: Dict[str, Any], 
        logger: Optional[logging.Logger] = None
    ) -> str:
        """
        Intersects target spatial configurations to override fine resolutions, locking 
        execution strictly to the equivalent native ~1km grid framework from GRID_REGISTRY.

        Parameters
        ----------
        spatial_cfg : dict
            The spatial configuration block extracted from the YAML execution recipe.
        logger : logging.Logger, optional
            The logger instance used to record configuration modifications.

        Returns
        -------
        str
            The specific, validated target grid key matching `GRID_REGISTRY`.
        """
        # Log the beginning of the target grid evaluation sequence
        log_execution(logger, "Resolving target grid base for CHELSA data cube generation...", logging.INFO)
        
        # Extract the user's requested grid from the config, defaulting to 'EEA', and convert to uppercase
        grid_base = str(spatial_cfg.get('target_grid', 'EEA')).upper()
        
        # Check if the requested grid is the European Environment Agency metric grid
        if grid_base == "EEA":
            # Force the resolution down to the 1km equivalent to match CHELSA's native scale
            resolved_grid = "EEA_1km"
            
        # Check if the requested grid is the Global Equal Area metric grid
        elif grid_base in ["GEA", "GLOBAL_EQUALAREA", "GLOBAL_EQUAL_AREA"]:
            # Force the resolution down to the 1km equivalent
            resolved_grid = "Global_EqualArea_1km"
            
        # Check if the requested grid is the standard global geographic degrees grid
        elif grid_base in ["WGS84", "GLOBAL_WGS84"]:
            # Force the resolution down to 30 arc-seconds (~1km at the equator)
            resolved_grid = "Global_WGS84_30sec"
            
        # If the requested grid string does not match standard shorthands, attempt a dynamic registry search
        else:
            # Set a safe default fallback in case the dynamic search fails
            resolved_grid = "EEA_1km"
            
            # Loop through all available keys in the parent class's GRID_REGISTRY
            for key in self.GRID_REGISTRY.keys():
                # Check if the user's base string exists in the key AND it represents a 1km/30sec scale
                if grid_base in key.upper() and ("1KM" in key.upper() or "30SEC" in key.upper()):
                    # Assign the exact registry key string and break the search loop
                    resolved_grid = key
                    break
                    
            # Log a warning that the specific user grid was overridden to protect memory bounds
            log_execution(
                logger, 
                f"Unrecognized target grid base '{grid_base}'. Applying registry fallback: '{resolved_grid}'.", 
                logging.WARNING
            )
            # Return the dynamically found fallback grid
            return resolved_grid

        # Log confirmation of the successfully resolved 1km equivalent grid
        log_execution(logger, f"Enforcing native atmospheric scale: mapped to registry grid '{resolved_grid}'.", logging.INFO)
        
        # Return the resolved grid key string
        return resolved_grid

    def intersect_config(
        self, 
        recipe: Dict[str, Any], 
        catalog_df: pd.DataFrame,
        logger: Optional[logging.Logger] = None
    ) -> pd.DataFrame:
        """
        Parses the abstract configuration recipe against the master CHELSA catalog to 
        isolate the precise multi-variable file sets required for cube assembly.

        Parameters
        ----------
        recipe : dict
            The complete, loaded YAML configuration recipe.
        catalog_df : pd.DataFrame
            The master CHELSA inventory catalog containing requisite indexing columns.
        logger : logging.Logger, optional
            The logger instance used to document intersection operations.

        Returns
        -------
        pd.DataFrame
            A highly filtered subset of the inventory catalog containing only activated assets.
        """
        # Log the start of the catalog subsetting process
        log_execution(logger, "Intersecting configuration recipe against CHELSA inventory...", logging.INFO)
        
        # Extract the nested CHELSA configuration block from the main recipe dictionary
        chelsa_cfg = recipe.get('sources', {}).get('chelsa', {})
        
        # Check if the user explicitly disabled CHELSA processing in their configuration
        if not chelsa_cfg.get('enabled', False):
            # Log the bypass and immediately return an empty DataFrame
            log_execution(logger, "CHELSA processing explicitly disabled in recipe.", logging.INFO)
            return pd.DataFrame()

        # Extract the master temporal bounds configuration block
        temp_cfg = recipe.get('temporal', {})
        
        # Construct a precise start date object using string formatting to ensure proper zero-padding
        start_date = pd.to_datetime(f"{temp_cfg.get('start_year', 1980)}-{temp_cfg.get('start_month', 1):02d}-01")
        
        # Construct a precise end date object, defaulting to the 28th to avoid leap year/end-of-month bugs
        end_date = pd.to_datetime(f"{temp_cfg.get('end_year', 2020)}-{temp_cfg.get('end_month', 12):02d}-28")

        # Extract the specific frequency level definitions (e.g., daily, monthly, bioclim)
        levels_cfg = chelsa_cfg.get('levels', {})
        
        # Initialize an empty list to store the filtered catalog chunks before final concatenation
        filtered_chunks = []

        # Iterate over each temporal level and its associated sub-settings
        for level, settings in levels_cfg.items():
            
            # If the user toggled this specific level off in the YAML, skip it completely
            if not settings.get('include', False):
                continue

            # Extract a list of only the physical variables explicitly set to True by the user
            active_vars = [var for var, is_active in settings.get('variables', {}).items() if is_active]
            
            # If no variables were activated for this level, skip to the next level
            if not active_vars:
                continue

            # Create a boolean mask filtering the catalog to the current level and the requested variables
            base_mask = (catalog_df['level'] == level) & (catalog_df['variable'].isin(active_vars))

            # Check if the current level represents continuous historical time-series data
            if level in ['daily', 'monthly', 'annual']:
                
                # Create a secondary boolean mask filtering dates strictly within the requested bounds
                time_mask = (catalog_df['date'] >= start_date) & (catalog_df['date'] <= end_date)
                
                # Apply both masks to the catalog to isolate the final target chunk
                chunk = catalog_df[base_mask & time_mask]

            # Check if the current level represents multi-decadal norms or future projection matrices
            elif level in ['climatologies', 'bioclim']:
                
                # Extract lists of activated scenarios, models, and temporal windows
                active_ranges = [tr for tr, state in settings.get('time_ranges', {}).items() if state]
                active_ensembles = [ens for ens, state in settings.get('ensembles', {}).items() if state]
                active_scenarios = [scen for scen, state in settings.get('scenarios', {}).items() if state]

                # Create a complex mask that allows matching active selections OR NaN values (for historical data)
                static_mask = (
                    (catalog_df['time_range'].isin(active_ranges)) &
                    (catalog_df['ensemble'].isin(active_ensembles) | catalog_df['ensemble'].isna()) &
                    (catalog_df['scenario'].isin(active_scenarios) | catalog_df['scenario'].isna())
                )
                
                # Apply both masks to isolate the specific projection models and timeframes
                chunk = catalog_df[base_mask & static_mask]

            # Append the filtered chunk to our tracking list
            filtered_chunks.append(chunk)

        # Check if the filtered chunks list contains any data
        if filtered_chunks:
            # Merge all individual level chunks into a single unified execution plan DataFrame
            final_df = pd.concat(filtered_chunks, ignore_index=True)
            
            # Log the successful generation of the fetching queue
            log_execution(logger, f"Intersection successful: queued {len(final_df)} target cube assets.", logging.INFO)
            
            # Return the populated execution plan
            return final_df
            
        # If the list is empty, log a warning that the user's configuration produced zero valid targets
        log_execution(logger, "No inventory assets matched specified parameters.", logging.WARNING)
        
        # Return an empty DataFrame to gracefully terminate downstream pipeline steps
        return pd.DataFrame()

    def _parse_worker_metadata(self, row: pd.Series, da: xr.DataArray) -> Tuple[str, xr.DataArray]:
        """
        Extracts complex CHELSA MultiIndex metadata from catalog rows and injects 
        it safely into the raw Xarray DataArray coordinates.

        Parameters
        ----------
        row : pd.Series
            The matched catalog row containing explicit variable metadata.
        da : xr.DataArray
            The raw spatial array fetched from network or disk.

        Returns
        -------
        tuple
            The frequency level string and the structurally formatted DataArray.
        """
        # Extract the processing level from the catalog row
        level = row['level']
        
        # Extract the remote asset path from the catalog row
        vsi_path = row['vsi_path']
        
        # Overwrite the generic xarray DataArray name with the specific physical variable name
        da.name = row['variable']
        
        # Attempt to get a clean filename from the row, or fallback to splitting the S3 URL string
        filename = row.get('filename') if pd.notna(row.get('filename')) else vsi_path.split('/')[-1]
        
        # Strip the file extension and split the filename by underscores to parse its structural tags
        file_parts = filename.replace('.tif', '').split('_')

        # Check if the catalog row contains a valid continuous date
        if pd.notna(row.get('date')):
            # Assign the exact date object to a temporary tracking variable
            coord_val = row['date']
            # Designate the coordinate axis name as 'time'
            dim_name = "time"
            
        # Check if the data belongs to the decadal climatology level
        elif level == 'climatologies':
            # Safely extract the climate model ensemble, defaulting to 'historical' if null
            ens = str(row['ensemble']) if pd.notna(row.get('ensemble')) else 'historical'
            
            # Safely extract the SSP greenhouse gas scenario, defaulting to 'historical' if null
            scen = str(row['scenario']) if pd.notna(row.get('scenario')) else 'historical'
            
            # Safely extract the multi-decadal time range, defaulting to the baseline if null
            tr = str(row['time_range']) if pd.notna(row.get('time_range')) else '1981-2010'
            
            # Initialize the month variable to 1
            m = 1
            # Iterate through the split filename parts to find the specific month integer safely
            for part in file_parts:
                # Check if the part is exclusively numeric, exactly 2 characters long, and between 1 and 12
                if part.isdigit() and len(part) == 2 and 1 <= int(part) <= 12:
                    # Parse it to an integer and assign it to the month tracker
                    m = int(part)
                    # Break the loop since we found the month successfully
                    break
            
            # Create a unique, flat string representation to prevent sorting collisions during merging
            coord_val = f"{ens}_{scen}_{tr}_{m:02d}"
            # Designate the coordinate axis name as the more abstract 'projection'
            dim_name = "projection"
            
        # Check if the data belongs to the complex bioclimatic indicator level
        elif level == 'bioclim':
            # Extract ensemble model safely
            ens = str(row['ensemble']) if pd.notna(row.get('ensemble')) else 'historical'
            
            # Extract SSP scenario safely
            scen = str(row['scenario']) if pd.notna(row.get('scenario')) else 'historical'
            
            # Extract temporal window safely
            tr = str(row['time_range']) if pd.notna(row.get('time_range')) else 'unknown'
            
            # Create the unique flat string identifier for safe array concatenation
            coord_val = f"{ens}_{scen}_{tr}"
            # Designate the axis name as 'projection'
            dim_name = "projection"
            
        # Fallback check for purely annual datasets containing only a year integer
        elif pd.notna(row.get('year')):
            # Convert the raw year integer into a true pandas datetime object on January 1st
            coord_val = pd.to_datetime(f"{int(row['year'])}-01-01")
            # Designate the axis name as 'time'
            dim_name = "time"
            
        # Ultimate fallback for un-parseable datasets
        else:
            # Use the raw pandas index number as the coordinate value
            coord_val = row.name 
            # Designate a generic axis name
            dim_name = "index"

        # Expand the flat 2D array into 3D, assigning the designated coordinate value to the new Z-axis
        da = da.expand_dims(dim_name).assign_coords({dim_name: [coord_val]})

        # If the newly created axis is our custom projection axis, attach the independent MultiIndex components
        if dim_name == "projection":
            
            # Check if we are attaching the 4-part climatology metadata
            if level == 'climatologies':
                # Bind the specific ensemble, scenario, range, and month directly to the projection dimension
                da = da.assign_coords({
                    "ensemble": (dim_name, [ens]),
                    "scenario": (dim_name, [scen]),
                    "time_range": (dim_name, [tr]),
                    "month": (dim_name, [m])
                })
                
            # Check if we are attaching the 3-part bioclim metadata
            elif level == 'bioclim':
                # Bind the ensemble, scenario, and range directly to the projection dimension
                da = da.assign_coords({
                    "ensemble": (dim_name, [ens]),
                    "scenario": (dim_name, [scen]),
                    "time_range": (dim_name, [tr])
                })

        # Check if rioxarray automatically generated a default "band" coordinate
        if "band" in da.coords:
            # Squeeze the array to eliminate the 1-size band dimension, and drop the coordinate metadata safely
            da = da.squeeze("band").drop_vars("band", errors="ignore")

        # Return the processed level string alongside the mathematically structured DataArray
        return level, da

    def process_cube(
        self, 
        recipe: Dict[str, Any], 
        catalog_df: pd.DataFrame,
        max_workers: int = 10, 
        logger: Optional[logging.Logger] = None
    ) -> Dict[str, xr.Dataset]:
        """
        Orchestrates the overarching single-pass computational pipeline: processes 
        configurations, evaluates safe spatial parameters dynamically, extracts 
        cloud subsets via the universal I/O engine, structures multi-indexes, and 
        reprojects arrays utilizing the unified parent class affine reprojection engine.

        Parameters
        ----------
        recipe : dict
            The validated YAML execution blueprint dictating spatiotemporal targets.
        catalog_df : pd.DataFrame
            The master precompiled dataframe containing the inventory catalog rows.
        max_workers : int, optional
            Maximum concurrent workers dedicated to network stream layers. Default is 10.
        logger : logging.Logger, optional
            The logger instance recording execution operations.

        Returns
        -------
        Dict[str, xr.Dataset]
            A dictionary mapping processed frequency level strings directly to reprojected, 
            MultiIndexed, and perfectly grid-aligned spatiotemporal datasets.
        """
        # Extract base_dir up front from recipe to prevent NameError downstream
        base_dir = recipe.get('base_dir', './test_outputs/')

        # Check if an external logging utility has been passed into the function call
        if logger is None:
            # Create a dedicated logs subdirectory inside the user's absolute output folder
            log_dir = os.path.join(base_dir, 'logs')
            os.makedirs(log_dir, exist_ok=True)
            
            # Define the absolute path for the .log file
            log_filepath = os.path.join(log_dir, 'chelsa_cube_generation.log')
            
            # Call the parent class method to instantiate the file handler and exception hooks
            logger = self._setup_pipeline_logger(logger_name="chelsa_cube", log_filepath=log_filepath)
            
            # Optional: Bind it to the instance so other internal methods can access self.logger
            self.logger = logger

        # Log the initialization of the overarching Data Cube generation sequence
        log_execution(logger, "\n=== Initiating CHELSA Out-of-Core Data Cube Generation ===", logging.INFO)
        
        # Isolate the required execution list by intersecting the recipe against the catalog parameter
        filtered_df = self.intersect_config(recipe, catalog_df, logger=logger)
        
        # Verify that the generated execution plan actually contains targets
        if filtered_df.empty:
            # Warn the user and abort the processing pipeline cleanly
            log_execution(logger, "Terminating pipeline: no candidate asset catalog generated.", logging.WARNING)
            return {}

        # Extract the spatial block from the main recipe dictionary
        spatial_cfg = recipe.get('spatial', {})
        
        # Resolve the requested target grid to its precise 1km CHELSA-compliant equivalent
        target_grid_key = self._resolve_chelsa_target_grid(spatial_cfg, logger=logger)
        
        # Retrieve the strict mathematical definitions for the target grid from the parent registry
        grid_info = self.GRID_REGISTRY[target_grid_key]
        
        # Extract the exact EPSG definition string to use for downstream projection
        target_crs = grid_info["crs"]
        
        # Extract the exact metric or degree resolution spacing for the destination pixels
        target_res = grid_info["resolution"]

        # Safely extract the raw bounding box float coordinates from the configuration
        bbox_cfg = spatial_cfg.get('bbox', {})
        
        # Safely extract the raw bounding box float coordinates from the configuration (Defined in WGS84 Degrees)
        bbox_cfg = spatial_cfg.get('bbox', {})
        wgs84_bounds = (
            min(bbox_cfg.get('long_min', 0), bbox_cfg.get('long_max', 0)),
            min(bbox_cfg.get('lat_min', 0), bbox_cfg.get('lat_max', 0)),
            max(bbox_cfg.get('long_min', 0), bbox_cfg.get('long_max', 0)),
            max(bbox_cfg.get('lat_min', 0), bbox_cfg.get('lat_max', 0))
        )

        # Immediately project the user's WGS84 degrees into the native Target CRS (e.g., European Meters)
        target_bounds = transform_bounds("EPSG:4326", target_crs, *wgs84_bounds)

        # CHECKPOINT 1: Bounding Box Translation Verification
        log_execution(logger, f"[CHECKPOINT 1] WGS84 Degrees: {wgs84_bounds}", logging.INFO)
        log_execution(logger, f"[CHECKPOINT 1] Target {target_crs} Bounds: {target_bounds}", logging.INFO)

        # Grab the raw S3 string path from the very first asset in the execution catalog
        sample_file_path = filtered_df.iloc[0]['vsi_path']

        # Call the spatial utility to ping the S3 file, detect its native CRS, and generate a padded bounding box
        source_bbox = build_envelope_from_file(
            target_crs=target_crs,
            target_bounds=target_bounds,
            source_file_path=sample_file_path,
            pixel_buffer=5,
            logger=logger
        )

        # Extract all unique S3 paths from the execution plan into a flat Python list
        target_paths = filtered_df['vsi_path'].unique().tolist()
        
        # Pass the file list and safe bounding box into the high-performance parallel fetching utility
        raw_fetched_data = parallel_fetch_rasters(
            paths=target_paths,
            geom=source_bbox,
            max_workers=max_workers
        )

        # Initialize a complex dictionary structure to map levels -> variables -> list of downloaded arrays
        level_variable_bins: Dict[str, Dict[str, List[xr.DataArray]]] = {
            lvl: {} for lvl in filtered_df['level'].unique()
        }

        # Loop over the execution plan to link the raw downloaded bytes back to their CMIP6 context metadata
        for _, row in filtered_df.iterrows():
            
            # Extract the path key to perform the lookup
            path = row['vsi_path']
            
            # Retrieve the raw xarray object from the fetcher's returned dictionary
            raw_da = raw_fetched_data.get(path)
            
            # Ensure the array actually exists (was not dropped due to a network timeout)
            if raw_da is not None:
                
                # Execute the internal parsing method to inject the MultiIndex labels
                level, structured_da = self._parse_worker_metadata(row, raw_da)
                
                # Convert the array name into a string to use as a clean dictionary key
                var_name = str(structured_da.name)
                
                # Insert the formatted array into the nested dictionary structure safely
                level_variable_bins[level].setdefault(var_name, []).append(structured_da)

        # Initialize the final dictionary that will hold the completed Dataset hypercubes
        final_cubes: Dict[str, xr.Dataset] = {}

        # Iterate over each temporal frequency level and its nested variable groups
        for level, variables_dict in level_variable_bins.items():
            
            # Skip levels that are completely empty due to network dropouts
            if not variables_dict:
                continue

            # Log the commencement of the single-pass warp architecture for the current level
            log_execution(logger, f"Harmonizing and warping level '{level}' cubes in single-pass...", logging.INFO)
            
            # Initialize an empty list to gather the finalized, perfectly warped individual variables
            reprojected_vars = []

            # Iterate over the individual arrays grouped by variable (e.g., all the 'tas' grids)
            for var_name, da_list in variables_dict.items():

                # CHECKPOINT 2: Raw Fetch Verification
                sample_raw = da_list[0]
                log_execution(
                    logger, 
                    f"[CHECKPOINT 2] '{var_name}' Raw Fetch | CRS: {sample_raw.rio.crs} | Shape: {sample_raw.shape} | Bounds: {sample_raw.rio.bounds()}", 
                    logging.INFO
                )

                # Extract the base spatial coordinates from the very first array in the list
                base_x, base_y = da_list[0].coords['x'], da_list[0].coords['y']
                
                # Reassign the base coordinates to all arrays to erase microscopic GDAL clipping variance
                snapped_list = [d.assign_coords(x=base_x, y=base_y) for d in da_list]
                
                # Identify the third dimension dynamically (it will be 'time' or 'projection')
                z_dim = [dim for dim in snapped_list[0].dims if dim not in ['x', 'y']][0]
                
                # Sort the arrays to guarantee strict chronological or index-based stacking order
                snapped_list.sort(key=lambda da: da.coords[z_dim].values[0])
                
                # Concatenate the ordered arrays into a single, cohesive 3D block
                combined_da = xr.concat(snapped_list, dim=z_dim)
                
                # Explicitly reaffirm the variable name on the combined block
                combined_da.name = var_name

                log_execution(
                    logger, 
                    f"[CHECKPOINT 3] '{var_name}' Combined | Z-Dim: {z_dim} (Size: {combined_da.sizes[z_dim]}) | 2D Grid: {combined_da.sizes.get('y')}x{combined_da.sizes.get('x')}", 
                    logging.INFO
                )

                # Check the class dictionary to determine the correct GDAL spatial interpolation rule
                rule = self.resample_rules.get(str(var_name), 'bilinear')
                
                # Implement a safety override ensuring composite precipitation models conserve physical volumes
                if 'bio' in str(var_name).lower():
                    # If it's bio12 (Annual pr), bio13 (Wettest month pr), or bio14 (Driest month pr), force average
                    rule = 'average' if var_name.lower() in ['bio12', 'bio13', 'bio14'] else 'bilinear'
                    
                # Convert the string rule into the actual Rasterio enumerator object expected by Xarray
                res_enum = getattr(Resampling, rule, Resampling.bilinear)
                
                # Clean the array's coordinate framework, forcing missing CRSs to default to geographic WGS84
                combined_da = self._sanitize_spatial_geometry(combined_da, default_crs="EPSG:4326", logger=logger)

                # ==========================================================
                # METADATA CATCH: Save all non-spatial coordinates before 
                # they are destroyed by the GeoTIFF format
                # ==========================================================
                non_spatial_coords = {
                    k: v for k, v in combined_da.coords.items() 
                    if k not in ['x', 'y', 'spatial_ref']
                }

                # Define a structured output path for the GDAL Warp
                cache_dir = os.path.join(base_dir, "warp_cache", level)
                os.makedirs(cache_dir, exist_ok=True)
                out_filepath = os.path.join(cache_dir, f"{var_name}_aligned.tif")

                # --- The Parent Class Affine Reprojection Engine Call ---
                warped_da = self.affine_reproject(
                    input_data=combined_da,
                    output_filepath=out_filepath,
                    grid_name=target_grid_key,    
                    resample_keyword=rule,        
                    logger=logger
                )

                # Crop the excess padding generated by the grid snap, explicitly declaring WGS84 bounds
                warped_da = warped_da.rio.clip_box(*target_bounds)
                
                log_execution(
                    logger, 
                    f"[CHECKPOINT 4] '{var_name}' Final Clipped | CRS: {warped_da.rio.crs} | Shape: {warped_da.shape} | Bounds: {warped_da.rio.bounds()}", 
                    logging.INFO
                )

                # ==========================================================
                # METADATA RELEASE: Re-inject the lost metadata
                # ==========================================================
                # Rename the generic GeoTIFF 'band' dimension back to 'time' or 'projection'
                warped_da = warped_da.rename({'band': z_dim})
                # Re-attach the rich coordinate values and sub-indexes
                warped_da = warped_da.assign_coords(non_spatial_coords)
                # Restore the physical variable name
                warped_da.name = var_name

                # Append the flawlessly aligned, fully-annotated array
                reprojected_vars.append(warped_da)

            # Merge all the perfectly aligned variables belonging to this level into a single Xarray Dataset
            level_cube = xr.merge(reprojected_vars, combine_attrs='drop_conflicts', join='outer')
            
            # Finalize the MultiIndex compilation phase now that spatial warping is safely completed
            if "projection" in level_cube.dims:
                
                # Bundle the independent metadata arrays into a true MultiIndex for climatologies
                if level == 'climatologies':
                    level_cube = level_cube.set_index(projection=["ensemble", "scenario", "time_range", "month"])
                    
                # Bundle the independent metadata arrays into a true MultiIndex for bioclim
                elif level == 'bioclim':
                    level_cube = level_cube.set_index(projection=["ensemble", "scenario", "time_range"])
            
            # Insert the completed, MultiIndexed, perfectly aligned Dataset into the master output dictionary
            final_cubes[level] = level_cube

        # Log the absolute completion of the entire multi-level spatiotemporal pipeline
        log_execution(logger, "=== CHELSA Data Cube Framework Generation Complete ===", logging.INFO)
        
        # Return the dictionary of datasets to the user or downstream DataTree constructor
        return final_cubes
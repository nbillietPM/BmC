import logging
from typing import Optional, Union, Dict, Any, List, Tuple
from abc import ABC, abstractmethod
from concurrent.futures import ThreadPoolExecutor
from functools import partial
import gc

import yaml
import sys
import os
import shutil
import zipfile
import glob
from pathlib import Path

import numpy as np
import xarray as xr
import pandas as pd
import rioxarray
from osgeo import gdal
from pyproj import CRS, Transformer
from rasterio.warp import transform_bounds
from rasterio.transform import from_origin
from rasterio.enums import Resampling

from bmc.utils.spatial import build_envelope_from_file
from bmc.utils.io import parallel_fetch_rasters
from bmc.utils.logger import log_execution

from bmc.engine.spatial import spatial_engine

class spatiotemporal_cube(spatial_engine, ABC):
    """
    Base class for constructing multidimensional ecological data lakes/cubes.

    This class provides the fundamental spatial physics, directory generation, 
    logging, and core GDAL/xarray processing engines required to build 
    spatiotemporal data cubes. It handles the ingestion, out-of-core warping, 
    and N-dimensional alignment of raw raster data (e.g., WEkEO, CHELSA) 
    to rigidly defined master grids.

    Attributes
    ----------
    GRID_REGISTRY : dict
        A master registry of supported coordinate reference systems (CRS) and 
        their exact spatial boundaries. Used to ensure flawless mathematical 
        alignment across disparate datasets.
    _GDAL_RESAMPLERS : dict
        Internal mapping of human-readable resampler string keys to their 
        corresponding GDAL C++ integer constants.
    _RESAMPLER_DECODER : dict
        Internal reverse-mapping of GDAL integer constants back to human-readable 
        resampler strings, utilized primarily for clear execution logging.

    Methods
    -------
    generate_cube_recipe(config_path, logger=None)
        Parses a YAML configuration file and generates a standardized execution recipe.
    resolve_grid_registry_key(target_grid, target_resolution, logger=None)
        Dynamically constructs and validates the master grid key from user configuration.
    gather_tifs_from_zips(source_directory, target_directory, logger=None)
        Iterates through zip archives and extracts .tif files into a flattened directory.
    cleanup_raw_storage(recipe, logger=None)
        Safely purges the raw data directory based on the user configuration.
    build_virtual_mosaic(input_folder, output_vrt_path, logger=None)
        Creates a lightweight Virtual Raster (VRT) blueprint from multiple GeoTIFF tiles.
    process_virtual_mosaic(vrt_path, strategy, grid_name, output_dir_or_file, logger=None, **kwargs)
        Routes a VRT blueprint to either standard reprojection or categorical fractional coverage.
    affine_reproject(input_data, output_filepath, grid_name, resample_keyword='bilinear', compress_mode='lzw', memory_limit_bytes=4096, logger=None)
        Performs out-of-core spatial reprojection and snapping to a strictly defined master grid.
    calculate_fractional_coverages(ds, grid_name, output_dir, class_values=None, class_mapping=None, file_prefix='fractional', logger=None)
        Calculates fractional coverage of categorical classes and exports single-band COGs.
    export_to_cog(ds, output_filepath, compress_mode='deflate', logger=None)
        Exports a lazy xarray object to disk as a Cloud Optimized GeoTIFF (COG).
    da_layer_constructor(data_layer_func, param)
        General layer constructor that fetches all slices for a layer sequentially.
    da_layer_constructor_concurrent(layer_func, param, max_workers=4)
        General layer constructor that fetches all slices for a layer concurrently.
    da_concat(data_arrays, dim_name, coordinates)
        Combines a stack of 2D data arrays into a 3D data array along a new dimension.    
    
    Notes
    -----
    The choice of GDAL resampling algorithm during affine reprojection is critical 
    for spatial accuracy. Below is a guide to the supported resamplers and their 
    optimal ecological use cases:

    Categorical & Discrete Data (e.g., Land Cover, Forest Type):
    * nearestNeighbour : Assigns the value of the single closest source pixel, 
      preserving original discrete values without interpolation.
    * mode : Assigns the most frequently occurring value among contributing pixels. 
      The mathematical standard for downsampling categorical data.

    Continuous Data Smoothing (e.g., Elevation, Temperature):
    * bilinear : Distance-weighted average of the 4 closest source pixels.
    * cubic : Distance-weighted cubic polynomial curve over the 16 nearest pixels.
    * cubicSpline : 2D B-spline mathematical function over the 16 nearest pixels. 
      Heavily smooths data and prevents "overshoot" (Runge's phenomenon). The 
      gold standard for realistic, continuous gradients.
    * lanczos : Complex windowed sinc function over the 36 nearest source pixels. 
      Preserves high-frequency details and sharpness.

    Continuous Data Statistical Aggregation (Downsampling):
    * average : Arithmetic mean of all valid intersecting source pixels.
    * max / min : Highest or lowest data value within the target footprint.
    * med : Exact middle value (50th percentile) of contributing pixels.
    * q1 / q3 : First (25th) or third (75th) quartile of contributing pixels.
    * sum : Addition of all valid intersecting source pixels.
    * rms : Root Mean Square (quadratic mean). Emphasizes higher magnitude values.
    """  
    def __init__(self):
        pass

    #################################
    # Interface & helper functions  #
    #################################

 
    def _parse_res_to_meters(self, res_str: str) -> float:
        """
        Converts a resolution string (e.g., '10m', '1km') into a float in meters.
        
        This helper is required for mathematical comparisons between different 
        available raw data resolutions.
        """
        res_str = res_str.lower().strip()
        if 'km' in res_str:
            return float(res_str.replace('km', '')) * 1000
        elif 'm' in res_str:
            return float(res_str.replace('m', ''))
        else:
            # Fallback for unexpected formats (like arc-seconds)
            # You can extend this logic as needed for Global_WGS84 grids
            return 999999.0

    def _resolve_query_resolution(
        self, 
        strategy: str, 
        available_res: List[str], 
        logger: Optional[logging.Logger] = None
    ) -> str:
        """
        Determines the single best resolution string to use based on a strategy.

        Parameters
        ----------
        strategy : str
            Options: 'highest' (smallest meters), 'lowest' (largest meters), 
            or a specific value like '20m'.
        available_res : list of str
            The unique resolution strings found in the inventory for a specific product.
        """
        if strategy not in ['highest', 'lowest'] and strategy in available_res:
            return strategy
            
        # Create a mapping: {meters: 'string_name'}
        res_map = {self._parse_res_to_meters(r): r for r in available_res}
        
        if not res_map:
            return "UNKNOWN"

        if strategy == 'highest':
            # Smallest distance = Highest resolution
            return res_map[min(res_map.keys())]
        elif strategy == 'lowest':
            # Largest distance = Lowest resolution
            return res_map[max(res_map.keys())]
        else:
            # If a specific res was requested but isn't available, 
            # we default to 'highest' and log a warning.
            best_guess = res_map[min(res_map.keys())]
            log_execution(
                logger, 
                f"Requested query resolution '{strategy}' not found. Falling back to highest available: {best_guess}", 
                logging.WARNING
            )
            return best_guess

    #################################
    #     General Cube pipeline     #
    #################################

    @abstractmethod
    def resolve_target_grid(self, spatial_cfg: Dict[str, Any], logger: logging.Logger) -> str:
        """
        Translates user-defined spatial configurations into a validated master grid key.

        This abstract method must interpret the ``spatial_cfg`` block from the YAML 
        recipe and map it to a physically safe, mathematically supported grid framework 
        present in the base class's ``GRID_REGISTRY``. Child classes must handle 
        vendor-specific logic here, such as safely degrading sub-kilometer CHELSA requests 
        to native 1km atmospheric scales, or allowing high-resolution WEkEO requests to pass.

        Parameters
        ----------
        spatial_cfg : dict
            The 'spatial' configuration dictionary extracted from the execution recipe. 
            Expected to contain keys such as 'target_grid' and 'target_resolution'.
        logger : logging.Logger
            The logger instance used to record validation steps or fallback warnings 
            if a requested resolution is actively overridden by the child class.

        Returns
        -------
        str
            The validated dictionary key (e.g., 'EEA_1km', 'Global_EqualArea_100m') 
            required to query the parent class's ``GRID_REGISTRY``.
        """
        pass

    @abstractmethod
    def generate_execution_plan(self, recipe: Dict[str, Any], logger: logging.Logger) -> pd.DataFrame:
        """
        Translates the execution recipe into a standardized data fetching queue.

        This method bridges the gap between the user's abstract configuration (e.g., 
        "Give me temperature for 2000-2010") and the vendor's actual data lake. Child 
        classes must implement the logic to query their specific catalogs (whether via 
        a local CSV inventory, directory crawling, or a live STAC API) and filter assets 
        based on spatial, temporal, and categorical constraints.

        Parameters
        ----------
        recipe : dict
            The fully loaded and parsed YAML configuration recipe dictating the 
            spatiotemporal bounds and requested variables.
        logger : logging.Logger
            The logger instance used to record catalog intersection progress, connection 
            status (if using STAC), and the final asset queue count.

        Returns
        -------
        pd.DataFrame
            A standardized execution queue. To be compatible with the parent processing 
            engine, the DataFrame must contain at minimum the following columns:
            - ``level`` (str): The processing family (e.g., 'daily', 'TCF').
            - ``variable`` (str): The specific scientific variable (e.g., 'tas', 'Tree Cover Density').
            - ``vsi_path`` (str): The direct /vsicurl/ or local path to the raw GeoTIFF.
        """
        pass

    @abstractmethod
    def parse_metadata(self, row: pd.Series, da: xr.DataArray) -> Tuple[str, xr.DataArray]:
        """
        Extracts dataset-specific metadata and injects it as dimensional coordinates.

        Raw Cloud-Optimized GeoTIFFs downloaded from remote storage are fundamentally 2D 
        and lack complex contextual metadata. This method expands the 2D spatial array 
        into a 3D or 4D array by parsing the metadata from the execution plan row (e.g., 
        extracting a year from a filename, or parsing CMIP6 ensembles) and assigning 
        those values to a new Z-axis dimension (like 'time' or 'projection').

        Parameters
        ----------
        row : pd.Series
            A single record from the execution plan DataFrame containing the contextual 
            metadata associated with the fetched array.
        da : xarray.DataArray
            The raw, mathematically flattened 2D spatial array returned by the 
            parallel network fetcher.

        Returns
        -------
        tuple
            A 2-element tuple containing:
            - ``level`` (str): The processing family grouping string.
            - ``da`` (xarray.DataArray): The structurally augmented 3D/4D DataArray 
              ready for Z-axis concatenation.
        """
        pass

    @abstractmethod
    def get_resample_rule(self, variable_name: str) -> str:
        """
        Determines the appropriate GDAL spatial resampling algorithm for a variable.

        Different physical and ecological variables require strictly different 
        mathematical algorithms during affine reprojection. Child classes must map 
        variable strings to valid GDAL resampling strings to prevent data corruption 
        (e.g., ensuring categorical land cover classes are never interpolated).

        Parameters
        ----------
        variable_name : str
            The name of the physical variable or product type currently being warped 
            (e.g., 'pr', 'Corine Land Cover 2018').

        Returns
        -------
        str
            The GDAL resampling string. Valid options include 'nearest', 'bilinear', 
            'cubic', 'average', 'mode', 'max', 'min', 'med', 'q1', 'q3', 'sum', 'rms'.
        """
        pass

    @abstractmethod
    def apply_multi_index(self, level: str, dataset: xr.Dataset) -> xr.Dataset:
        """
        Compiles independent dimensional coordinates into a vendor-specific MultiIndex.

        After the parent engine completes spatial warping and restores basic Z-axis 
        coordinates, some highly complex datasets (such as multidimensional climate 
        scenarios) require bundling individual string coordinates into a formalized 
        Pandas/Xarray MultiIndex. Child classes implement this to finalize the 
        Dataset structure.

        Parameters
        ----------
        level : str
            The processing family grouping string (e.g., 'climatologies', 'bioclim') 
            which dictates whether a MultiIndex is necessary.
        dataset : xarray.Dataset
            The fully warped, spatially aligned, and basic-coordinate-restored Dataset.

        Returns
        -------
        xarray.Dataset
            The finalized Dataset, optionally containing a `.set_index()` MultiIndex 
            on the Z-axis (e.g., grouping ensemble, scenario, and time_range).
        """
        pass

    def process_cube(
        self, 
        recipe: Dict[str, Any], 
        max_workers: int = 10,
        logger: Optional[logging.Logger] = None
    ) -> Dict[str, xr.Dataset]:
        """The universal single-pass rectangular processing loop."""
        
        # Handle configuration structure (from generate_cube_recipe)
        paths_cfg = recipe.get('paths', {})
        base_dir = paths_cfg.get('base_dir') or recipe.get('base_dir', './outputs/')
        
        if logger is None:
            log_dir = os.path.join(base_dir, 'logs')
            os.makedirs(log_dir, exist_ok=True)
            log_filepath = os.path.join(log_dir, 'spatiotemporal_cube_generation.log')
            logger = self._setup_pipeline_logger(logger_name="spatiotemporal_cube", log_filepath=log_filepath)
            self.logger = logger

        log_execution(logger, "\n=== Initiating Out-of-Core Data Cube Generation ===", logging.INFO)
        
        # 1. Ask Child to Generate Plan
        execution_plan = self.generate_execution_plan(recipe, logger)
        if execution_plan.empty:
            log_execution(logger, "Terminating pipeline: no candidate asset catalog generated.", logging.WARNING)
            return {}

        # 2. Resolve Master Grid
        spatial_cfg = recipe.get('spatial', {})
        target_grid_key = self.resolve_target_grid(spatial_cfg, logger)
        grid_info = self.GRID_REGISTRY[target_grid_key]
        target_crs = grid_info["crs"]
        target_res = grid_info["resolution"]
        
        # 3. Calculate Rectangular Bounds
        bbox_cfg = spatial_cfg.get('bbox', {})
        wgs84_bounds = (
            min(bbox_cfg.get('long_min', 0), bbox_cfg.get('long_max', 0)),
            min(bbox_cfg.get('lat_min', 0), bbox_cfg.get('lat_max', 0)),
            max(bbox_cfg.get('long_min', 0), bbox_cfg.get('long_max', 0)),
            max(bbox_cfg.get('lat_min', 0), bbox_cfg.get('lat_max', 0))
        )
        target_bounds = transform_bounds("EPSG:4326", target_crs, *wgs84_bounds)

        log_execution(logger, f"\n--- Target Spatial Framework Initialized ---", logging.INFO)
        log_execution(logger, f"  Master Grid : {target_grid_key} ({target_crs})", logging.INFO)
        log_execution(logger, f"  Resolution  : {target_res} (Native CRS Units)", logging.INFO)
        log_execution(logger, f"  WGS84 Bounds: [MinLon: {wgs84_bounds[0]:.4f}, MinLat: {wgs84_bounds[1]:.4f}, MaxLon: {wgs84_bounds[2]:.4f}, MaxLat: {wgs84_bounds[3]:.4f}]", logging.INFO)
        log_execution(logger, f"  Proj Bounds : [MinX: {target_bounds[0]:.2f}, MinY: {target_bounds[1]:.2f}, MaxX: {target_bounds[2]:.2f}, MaxY: {target_bounds[3]:.2f}]\n", logging.INFO)

        # 4. Fetch the Data
        sample_file_path = execution_plan.iloc[0]['vsi_path']
        source_bbox = build_envelope_from_file(
            target_crs=target_crs,
            target_bounds=target_bounds,
            source_file_path=sample_file_path,
            pixel_buffer=5,
            logger=logger
        )
        
        target_paths = execution_plan['vsi_path'].unique().tolist()
        raw_fetched_data = parallel_fetch_rasters(target_paths, source_bbox, max_workers)

        # 5. Ask Child to Inject Metadata
        level_variable_bins: Dict[str, Dict[str, List[xr.DataArray]]] = {
            lvl: {} for lvl in execution_plan['level'].unique()
        }
        for _, row in execution_plan.iterrows():
            raw_da = raw_fetched_data.get(row['vsi_path'])
            if raw_da is not None:
                level, structured_da = self.parse_metadata(row, raw_da)
                level_variable_bins[level].setdefault(str(structured_da.name), []).append(structured_da)

        final_cubes: Dict[str, xr.Dataset] = {}

        # 6. Harmonize and Warp
        for level, variables_dict in level_variable_bins.items():
            if not variables_dict: continue
            
            log_execution(logger, f"Harmonizing and warping level '{level}' cubes in single-pass...", logging.INFO)
            reprojected_vars = []
            
            for var_name, da_list in variables_dict.items():
                
                # Z-Stacking
                base_x, base_y = da_list[0].coords['x'], da_list[0].coords['y']
                snapped_list = [d.assign_coords(x=base_x, y=base_y) for d in da_list]
                
                z_dim = [dim for dim in snapped_list[0].dims if dim not in ['x', 'y']][0]
                snapped_list.sort(key=lambda da: da.coords[z_dim].values[0])
                
                combined_da = xr.concat(snapped_list, dim=z_dim)
                combined_da.name = var_name

                # GDAL Rules & Metadata Catch
                rule = self.get_resample_rule(var_name)
                combined_da = self._sanitize_spatial_geometry(combined_da, default_crs="EPSG:4326", logger=logger)
                non_spatial_coords = {k: v for k, v in combined_da.coords.items() if k not in ['x', 'y', 'spatial_ref']}

                # Affine Reprojection utilizing your parent method
                cache_dir = os.path.join(base_dir, "warp_cache", level)
                os.makedirs(cache_dir, exist_ok=True)
                out_filepath = os.path.join(cache_dir, f"{var_name}_aligned.tif")
                
                warped_da = self.affine_reproject(
                    input_data=combined_da, 
                    output_filepath=out_filepath, 
                    grid_name=target_grid_key, 
                    resample_keyword=rule, 
                    logger=logger
                )
                
                # Mathematical Rectangular Clip
                warped_da = warped_da.rio.clip_box(*target_bounds)
                
                # Metadata Release
                warped_da = warped_da.rename({'band': z_dim})
                warped_da = warped_da.assign_coords(non_spatial_coords)
                warped_da.name = var_name
                
                reprojected_vars.append(warped_da)
                
            # Merge and MultiIndex (using child's rules)
            level_cube = xr.merge(reprojected_vars, combine_attrs='drop_conflicts', join='outer')
            final_cubes[level] = self.apply_multi_index(level, level_cube)
            
        log_execution(logger, "=== Data Cube Framework Generation Complete ===", logging.INFO)
        return final_cubes
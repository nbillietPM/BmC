import os
import sys
import glob
import gc
from pathlib import Path
import tempfile

import logging
from typing import Optional, Union, Dict, Any, List, Tuple
from bmc.utils.logger import log_execution

import ot
import libpysal
import esda

try:
    from libpysal.weights import Queen, KNN
    from esda.moran import Moran
    HAS_TOPOLOGY = True
except ImportError:
    HAS_TOPOLOGY = False

import math
import numpy as np
import xarray as xr
import pandas as pd
import geopandas as gpd
import rioxarray
from osgeo import gdal
import uuid
from pyproj import CRS, Transformer
import rasterio
from rasterio.warp import transform_bounds
from rasterio.transform import from_origin
from rasterio.enums import Resampling
from shapely.geometry import box
from scipy.spatial import KDTree

class base_spatial_grid():
    """
    The foundational spatial truth of the spatiotemporal pipeline.
    
    This base class acts as the central source of truth for the physical laws 
    of the pipeline. It holds the definitive registry of all supported master grids 
    (Coordinate Reference Systems, resolutions, and absolute bounding boxes). 
    
    By abstracting this into a foundational class, both the raster-based 
    ``spatial_engine`` and the vector-based ``vector_spatial_engine`` inherit 
    the exact same mathematical blueprints, guaranteeing perfect 1-to-1 pixel 
    alignment when bridging continuous and discrete datasets.

    Attributes
    ----------
    GRID_REGISTRY : dict
        A master dictionary mapping human-readable grid keys (e.g., "EEA_10km") 
        to their rigid spatial definitions. Each definition contains:
        * ``crs``: The EPSG code string.
        * ``resolution``: The size of a single pixel in native CRS units.
        * ``bounds``: The definitive absolute extent (minx, miny, maxx, maxy).
    """

    GRID_REGISTRY = {
    # ---------------------------------------------------------
    # EEA Reference Grid (EPSG:3035) - Metric
    # ---------------------------------------------------------
    "EEA_10m": {"crs": "EPSG:3035", "resolution": 10, "bounds": (2000000, 1000000, 6000000, 5500000)},
    "EEA_100m": {"crs": "EPSG:3035", "resolution": 100, "bounds": (2000000, 1000000, 6000000, 5500000)},
    "EEA_250m": {"crs": "EPSG:3035", "resolution": 250, "bounds": (2000000, 1000000, 6000000, 5500000)},
    "EEA_500m": {"crs": "EPSG:3035", "resolution": 500, "bounds": (2000000, 1000000, 6000000, 5500000)},
    "EEA_1km":  {"crs": "EPSG:3035", "resolution": 1000, "bounds": (2000000, 1000000, 6000000, 5500000)},
    "EEA_10km": {"crs": "EPSG:3035", "resolution": 10000, "bounds": (2000000, 1000000, 6000000, 5500000)},

    # ---------------------------------------------------------
    # Global Equal Area (EPSG:6933) - Metric
    # ---------------------------------------------------------
    "Global_EqualArea_10m": {"crs": "EPSG:6933", "resolution": 10, "bounds": (-17367530, -7314540, 17367530, 7314540)},
    "Global_EqualArea_100m": {"crs": "EPSG:6933", "resolution": 100, "bounds": (-17367530, -7314540, 17367530, 7314540)},
    "Global_EqualArea_250m": {"crs": "EPSG:6933", "resolution": 250, "bounds": (-17367530, -7314540, 17367530, 7314540)},
    "Global_EqualArea_500m": {"crs": "EPSG:6933", "resolution": 500, "bounds": (-17367530, -7314540, 17367530, 7314540)},
    "Global_EqualArea_1km":  {"crs": "EPSG:6933", "resolution": 1000, "bounds": (-17367530, -7314540, 17367530, 7314540)},
    "Global_EqualArea_10km": {"crs": "EPSG:6933", "resolution": 10000, "bounds": (-17367530, -7314540, 17367530, 7314540)},

    # ---------------------------------------------------------
    # Global WGS84 (EPSG:4326) - Decimal Degrees
    # ---------------------------------------------------------
    # ~10m at the equator (0.3 arc-seconds)
    "Global_WGS84_0_3sec": {"crs": "EPSG:4326", "resolution": 0.00008333333333333333, "bounds": (-180.0, -90.0, 180.0, 90.0)},
    # ~100m at the equator (3 arc-seconds)
    "Global_WGS84_3sec": {"crs": "EPSG:4326", "resolution": 0.0008333333333333333, "bounds": (-180.0, -90.0, 180.0, 90.0)},
    # ~250m at the equator (7.5 arc-seconds)
    "Global_WGS84_7_5sec": {"crs": "EPSG:4326", "resolution": 0.0020833333333333333, "bounds": (-180.0, -90.0, 180.0, 90.0)},
    # ~500m at the equator (15 arc-seconds)
    "Global_WGS84_15sec": {"crs": "EPSG:4326", "resolution": 0.004166666666666667, "bounds": (-180.0, -90.0, 180.0, 90.0)},
    # ~1km at the equator (30 arc-seconds)
    "Global_WGS84_30sec": {"crs": "EPSG:4326", "resolution": 0.008333333333333333, "bounds": (-180.0, -90.0, 180.0, 90.0)},
    # ~10km at the equator (5 arc-minutes)
    "Global_WGS84_5min": {"crs": "EPSG:4326", "resolution": 0.08333333333333333, "bounds": (-180.0, -90.0, 180.0, 90.0)}}

    def resolve_grid_registry_key(
        self, 
        target_grid: str, 
        target_resolution: str, 
        logger: Optional[logging.Logger] = None
        ) -> str:
        """
        Dynamically constructs and validates the master grid key from user configuration.

        Parameters
        ----------
        target_grid : str
            The base coordinate reference system identifier (e.g., "EEA", "Global_WGS84").
        target_resolution : str
            The spatial resolution string (e.g., "100m", "10km", "30sec").
        logger : logging.Logger, optional
            The logger instance to record the error if the key doesn't exist. Default is None.

        Returns
        -------
        grid_key : str
            The validated dictionary key used to access `self.GRID_REGISTRY`.

        Raises
        ------
        ValueError
            If the concatenated string does not match a predefined grid.
        """
        grid_key = f"{target_grid}_{target_resolution}"
        
        if grid_key not in self.GRID_REGISTRY:
            available = "\n - ".join(self.GRID_REGISTRY.keys())
            error_msg = (
                f"\n[Spatial Config Error] Attempted to build grid key '{grid_key}', "
                f"but it does not exist in the registry.\n\n"
                f"Available Grids:\n - {available}"
            )
            # Log the critical error before stopping execution
            log_execution(logger, error_msg, logging.ERROR)
            raise ValueError(error_msg)    
            
        return grid_key

    def build_safe_fetch_envelope(
        self,
        target_grid_name: str,
        target_bounds: Optional[Tuple[float, float, float, float]] = None,
        source_crs_or_grid: str = "EPSG:4326",
        source_resolution: Optional[float] = None,
        pixel_buffer: int = 5,
        logger: Optional[logging.Logger] = None
    ) -> Tuple[float, float, float, float]:
        """
        Constructs a densified, buffered source envelope guaranteed to fully encapsulate 
        a target grid region without causing edge starvation or NaN boundary artifacts.

        This universal method resolves target grid geometries against the internal registry, 
        densifies the outer perimeter using vectorized linear interpolation to capture 
        projection curvature, applies spatial transformation to the native source coordinate 
        space, and buffers the resulting envelope outward to supply sufficient edge pixels 
        for multi-pixel GDAL resampling kernels.

        Parameters
        ----------
        target_grid_name : str
            The key of the destination grid defined in `GRID_REGISTRY` (e.g., "EEA_1km").
        target_bounds : tuple of float, optional
            Specific sub-region bounding box in target CRS units: (minx, miny, maxx, maxy). 
            If omitted, defaults to the master grid's full definitive bounds.
        source_crs_or_grid : str, optional
            Either a target key from `GRID_REGISTRY` (e.g., "Global_WGS84_30sec") or a 
            standard CRS string (e.g., "EPSG:4326"). Default is "EPSG:4326".
        source_resolution : float, optional
            The size of a single source pixel in native source CRS units. Automatically 
            inferred if `source_crs_or_grid` exists in the registry.
        pixel_buffer : int, optional
            Number of native source pixels added as an outer safety padding to support 
            multi-pixel GDAL resampling kernels. Default is 5.
        logger : logging.Logger, optional
            Logger instance for recording execution metadata. Default is None.

        Returns
        -------
        tuple of float
            The safe outer envelope in the source coordinate space: 
            (src_minx, src_miny, src_maxx, src_maxy).

        Raises
        ------
        KeyError
            If the requested `target_grid_name` does not exist in the registry.
        ValueError
            If spatial transformation yields entirely non-finite coordinates.

        Examples
        --------
        Case 1: Standard Master Grid Extraction
        Deriving the fetching envelope for an entire master grid extent. By omitting 
        ``target_bounds``, the method automatically defaults to the full definitive bounds 
        of the requested target grid registry entry.

        >>> safe_wgs_envelope = cube.build_safe_fetch_envelope(
        ...     target_grid_name="EEA_1km",
        ...     source_crs_or_grid="Global_WGS84_30sec",
        ...     pixel_buffer=5,
        ...     logger=pipeline_logger
        ... )
        >>> print(safe_wgs_envelope)
        (-11.45833, 34.04166, 31.95833, 72.87500)

        Case 2: Localized Sub-Region Ingestion
        Calculating a highly precise, buffered source envelope for a specific localized subset 
        (e.g., a localized study area in Belgium defined in metric EPSG:3035 coordinates). 
        Adding a 5-pixel buffer safely feeds downstream cubic or average C++ warping kernels.

        >>> study_area_3035 = (3800000, 2900000, 3900000, 3000000)
        >>> safe_subset_envelope = cube.build_safe_fetch_envelope(
        ...     target_grid_name="EEA_100m",
        ...     target_bounds=study_area_3035,
        ...     source_crs_or_grid="Global_WGS84_30sec",
        ...     pixel_buffer=5
        ... )
        >>> print(safe_subset_envelope)
        (4.19833, 50.71833, 5.66833, 51.65500)

        Case 3: Custom Source Resolution Fallback
        Querying an unlisted custom CRS string while manually supplying the native source 
        pixel spacing to ensure exact metric-to-degree buffer translation.

        >>> custom_envelope = cube.build_safe_fetch_envelope(
        ...     target_grid_name="Global_EqualArea_1km",
        ...     source_crs_or_grid="EPSG:4326",
        ...     source_resolution=0.0041666667,  # ~500m source pixels
        ...     pixel_buffer=8
        ... )
        """
        log_execution(logger, f"Computing safe fetch envelope for target grid '{target_grid_name}'...", logging.INFO)

        # Resolve Target Grid Configurations
        if target_grid_name not in self.GRID_REGISTRY:
            raise KeyError(f"Target grid '{target_grid_name}' not found in GRID_REGISTRY.")
            
        target_spec = self.GRID_REGISTRY[target_grid_name]
        target_crs = target_spec["crs"]
        
        if target_bounds is None:
            target_bounds = target_spec["bounds"]
            log_execution(logger, "Specific target_bounds omitted. Encapsulating full master grid extent.", logging.INFO)

        # Resolve Source Data Configurations
        if source_crs_or_grid in self.GRID_REGISTRY:
            src_spec = self.GRID_REGISTRY[source_crs_or_grid]
            actual_source_crs = src_spec["crs"]
            if source_resolution is None:
                source_resolution = src_spec["resolution"]
        else:
            actual_source_crs = source_crs_or_grid
            if source_resolution is None:
                # Fallback check: Assume CHELSA/WorldClim ~30 arc-second base spacing
                source_resolution = self.GRID_REGISTRY.get("Global_WGS84_30sec", {}).get(
                    "resolution", 0.008333333333333333
                )
                log_execution(
                    logger,
                    f"source_resolution omitted for custom CRS '{actual_source_crs}'. "
                    f"Applying default 30 arc-second fallback: {source_resolution}",
                    logging.WARNING
                )

        # 3. Vectorized Perimeter Densification
        minx, miny, maxx, maxy = target_bounds
        num_points = 100  # Granularity per edge

        bx = np.linspace(minx, maxx, num_points)
        by = np.full(num_points, miny)

        rx = np.full(num_points, maxx)
        ry = np.linspace(miny, maxy, num_points)

        tx = np.linspace(maxx, minx, num_points)
        ty = np.full(num_points, maxy)

        lx = np.full(num_points, minx)
        ly = np.linspace(maxy, miny, num_points)

        perimeter_x = np.concatenate([bx, rx, tx, lx])
        perimeter_y = np.concatenate([by, ry, ty, ly])

        # 4. Perform Coordinate Transformation
        transformer = Transformer.from_crs(target_crs, actual_source_crs, always_xy=True)
        src_x, src_y = transformer.transform(perimeter_x, perimeter_y)

        valid_mask = np.isfinite(src_x) & np.isfinite(src_y)
        if not np.any(valid_mask):
            raise ValueError(
                f"Failed to project target bounds from {target_crs} to {actual_source_crs}. "
                "Ensure target coordinates fall within allowable projection definitions."
            )
            
        src_x, src_y = src_x[valid_mask], src_y[valid_mask]

        src_minx, src_maxx = float(np.min(src_x)), float(np.max(src_x))
        src_miny, src_maxy = float(np.min(src_y)), float(np.max(src_y))

        # 5. Apply Resampling Safety Buffer
        buffer_padding = source_resolution * pixel_buffer
        
        safe_minx = src_minx - buffer_padding
        safe_maxx = src_maxx + buffer_padding
        safe_miny = src_miny - buffer_padding
        safe_maxy = src_maxy + buffer_padding

        # 6. Apply Geographic Domain Guardrails
        src_crs_obj = CRS.from_string(actual_source_crs)
        if src_crs_obj.is_geographic:
            safe_minx = max(-180.0, safe_minx)
            safe_maxx = min(180.0, safe_maxx)
            safe_miny = max(-90.0, safe_miny)
            safe_maxy = min(90.0, safe_maxy)

        log_execution(
            logger,
            f"Safe Source Envelope ({actual_source_crs}): "
            f"({safe_minx:.5f}, {safe_miny:.5f}, {safe_maxx:.5f}, {safe_maxy:.5f})",
            logging.INFO
        )
            
        return (safe_minx, safe_miny, safe_maxx, safe_maxy)

    def create_aligned_raster_template(
        self, 
        sample_bbox: Tuple[float, float, float, float], 
        grid_name: str
    ) -> Tuple[xr.DataArray, Tuple[float, float, float, float]]:
        """
        Generates an empty, mathematically rigid xarray DataArray template perfectly 
        snapped to a predefined master grid.

        This method acts as a spatial blueprint generator. It takes a loose, localized 
        bounding box and forces it to expand outward until its edges perfectly intersect 
        the integer-aligned pixel boundaries of the master grid. It then generates a 2D 
        matrix of zeros (with embedded CF-compliant spatial coordinates) that downstream 
        functions can use as a canvas for raster reprojection or vector fractional burning.

        Parameters
        ----------
        sample_bbox : tuple of float
            The localized region of interest bounds in the format 
            ``(minx, miny, maxx, maxy)``. These bounds must already be projected 
            into the native CRS of the target grid.
        grid_name : str
            The precise dictionary key corresponding to the target grid defined 
            in the ``GRID_REGISTRY`` (e.g., "EEA_1km").

        Returns
        -------
        template : xarray.DataArray
            A 2D spatial matrix filled with zeros (dtype: int32). Embedded attributes 
            include the mathematically derived `x` and `y` pixel center coordinates, 
            the rioxarray spatial reference topology, and critical metadata strings 
            (``crs``, ``res``, ``spatial_unit``).
        aligned_bbox : tuple of float
            The newly expanded, grid-snapped bounding box in the format 
            ``(aligned_minx, aligned_miny, aligned_maxx, aligned_maxy)``.

        Raises
        ------
        KeyError
            If the requested ``grid_name`` does not exist within the class 
            ``GRID_REGISTRY``.

        Notes
        -----
        The mathematical snapping relies on the absolute origin (``master_minx``, 
        ``master_miny``) defined in the registry. 
        
        It utilizes ``math.floor()`` for the minimum coordinates and ``math.ceil()`` 
        for the maximum coordinates. This guarantees that the localized bounding box 
        only ever grows outward, preventing edge-starvation where geometries resting 
        on the absolute boundary might otherwise be clipped.
        """
        if grid_name not in self.GRID_REGISTRY:
            raise KeyError(f"Grid '{grid_name}' not found in registry.")
            
        master = self.GRID_REGISTRY[grid_name]
        res = master["resolution"]
        master_minx, master_miny, master_maxx, master_maxy = master["bounds"]
        
        # sample_bbox is (minx, miny, maxx, maxy)
        s_minx, s_miny, s_maxx, s_maxy = sample_bbox
        
        # 1. Snap strictly to the Master Grid intervals
        aligned_minx = master_minx + math.floor((s_minx - master_minx) / res) * res
        aligned_miny = master_miny + math.floor((s_miny - master_miny) / res) * res
        aligned_maxx = master_minx + math.ceil((s_maxx - master_minx) / res) * res
        aligned_maxy = master_miny + math.ceil((s_maxy - master_miny) / res) * res
        
        # 2. Calculate integer dimensions safely
        width = int(round((aligned_maxx - aligned_minx) / res))
        height = int(round((aligned_maxy - aligned_miny) / res))
        
        # 3. Generate spatial coordinates (Pixel Centers)
        x_coords = aligned_minx + (np.arange(width) + 0.5) * res
        y_coords = aligned_maxy - (np.arange(height) + 0.5) * res
        
        # 4. Dynamically determine spatial units from the CRS
        crs_obj = CRS.from_string(master["crs"])
        spatial_unit = "degrees" if crs_obj.is_geographic else "meters"
        
        # 5. Create the DataArray template with robust metadata attributes
        template = xr.DataArray(
            data=np.zeros((height, width), dtype=np.int32), 
            coords={"y": y_coords, "x": x_coords},
            dims=("y", "x"),
            attrs={
                "grid_registry_key": grid_name,
                "res": res,
                "spatial_unit": spatial_unit
            }
        )
        
        # 6. Inject CF-compliant spatial topology FIRST
        template = template.rio.write_crs(master["crs"])
        
        # 7. Assign the text attribute explicitly 
        template.attrs["crs"] = str(master["crs"])
        
        return template, (aligned_minx, aligned_miny, aligned_maxx, aligned_maxy)

class spatial_engine(base_spatial_grid):
    """
    The fundamental spatial physics and geometric transformation engine.

    This base class acts as a universal spatial toolbox, providing the core 
    mathematics, out-of-core GDAL Python wrappings, and coordinate registries 
    required to process raw ecological datasets. It is completely decoupled from 
    data lifecycle management (downloading, unzipping, cataloging), focusing 
    exclusively on the physical transformation of arrays (e.g., affine reprojection, 
    fractional continuous area calculation, and Virtual Raster mosaicking).

    Both the runtime orchestrators (cubes) and offline data ingestion orchestrators 
    (lakes) inherit from this class to guarantee mathematical consistency across 
    the entire software architecture.

    Attributes
    ----------
    GDAL_RESAMPLERS : dict
        Public registry mapping human-readable string algorithms to their 
        corresponding GDAL C++ integer constants (e.g., "bilinear" -> gdal.GRA_Bilinear).
    RESAMPLER_DECODER : dict
        Public registry providing reverse-mapping of GDAL integer constants back 
        to human-readable algorithm strings, utilized primarily for clear execution logging.

    Methods
    -------
    build_safe_fetch_envelope(target_grid_name, target_bounds, source_crs_or_grid, source_resolution, pixel_buffer, logger=None)
        Constructs a densified, buffered bounding box guaranteed to encapsulate a target grid safely.
    build_virtual_mosaic(input_folder, output_vrt_path, logger=None)
        Creates a lightweight Virtual Raster (VRT) blueprint from multiple physical GeoTIFF tiles.
    process_virtual_mosaic(vrt_path, strategy, grid_name, output_dir_or_file, logger=None, **kwargs)
        Dispatcher routing a VRT blueprint to either affine reprojection or categorical calculation.
    affine_reproject(input_data, output_filepath, grid_name, resample_keyword='bilinear', compress_mode='lzw', memory_limit_bytes=4096, logger=None)
        Performs out-of-core spatial reprojection and grid snapping using highly optimized GDAL C++ bindings.
    calculate_fractional_coverages(ds, grid_name, output_dir, class_values=None, class_mapping=None, file_prefix='fractional', logger=None)
        Converts discrete categorical rasters into continuous, snapped fractional coverage matrices.
    export_to_cog(ds, output_filepath, compress_mode='deflate', logger=None)
        Streams lazy xarray objects to disk as highly compressed Cloud Optimized GeoTIFFs (COGs).

    Notes
    -----
    The choice of GDAL resampling algorithm requested via `affine_reproject` is 
    critical for ecological validity. 

    Categorical & Discrete Data (e.g., Land Cover, Forest Type):
    * `nearestNeighbour`: Assigns the single closest source pixel value.
    * `mode`: Assigns the most frequently occurring value. The standard for downsampling categories.

    Continuous Data Smoothing (e.g., Elevation, Temperature):
    * `bilinear`: Distance-weighted average of the 4 closest source pixels.
    * `cubic` / `cubicSpline`: Polynomial curves over 16 nearest pixels for ultra-smooth gradients.
    * `lanczos`: Windowed sinc function preserving high-frequency details.

    Continuous Data Statistical Aggregation (Downsampling):
    * `average`: Arithmetic mean of all valid intersecting source pixels (Volume conservation).
    * `max` / `min`: Extreme values within the target footprint.
    * `sum`: Addition of all valid intersecting source pixels.
    """
    GDAL_RESAMPLERS = {
    "nearestNeighbour": gdal.GRA_NearestNeighbour,
    "bilinear": gdal.GRA_Bilinear,
    "cubic": gdal.GRA_Cubic,
    "cubicSpline": gdal.GRA_CubicSpline,
    "lanczos": gdal.GRA_Lanczos,
    "average": gdal.GRA_Average,
    "mode": gdal.GRA_Mode,
    "max": gdal.GRA_Max,
    "min": gdal.GRA_Min,
    "med": gdal.GRA_Med,
    "q1": gdal.GRA_Q1,
    "q3": gdal.GRA_Q3,
    "sum": gdal.GRA_Sum,
    "rms": gdal.GRA_RMS}

    RESAMPLER_DECODER = {
    0: 'nearestNeighbour',
    1: 'bilinear',
    2: 'cubic',
    3: 'cubicSpline',
    4: 'lanczos',
    5: 'average',
    6: 'mode',
    8: 'max',
    9: 'min',
    10: 'med',
    11: 'q1',
    12: 'q3',
    13: 'sum',
    14: 'rms'}

    def _setup_pipeline_logger(self, logger_name, log_filepath):
        """
        Creates an instance of the standard python logging tool which can be called inside the spatiotemporal cube class
        and its children to automatically stream execution progress and potential errors/bugs to a log file during cube generation

        Parameters
        ----------

        logger_name : str
            logger_name description
        log_filepath : str
            The location where the .log file is written to. Must contain the directory path and the filename ending in .log

        Returns
        -------

        logger : logging.Logger
            Object that automates the handling off messages and errors

        Notes
        -----

        The function is made private and will be called at the initialization of any instance of the spatiotemporal class and its associated 
        children. The end user should ideally not be interfacing with the logger directly

        See Also
        --------

        bmc.utils.logger.log_execution
        
        """
        # Initialize the logger
        logger = logging.getLogger(logger_name)
        # Lowest level of messages that are being processed are those of the INFO category
        logger.setLevel(logging.INFO)
        
        # Handlers determine where the messages are being streamed to
        # Prevent adding duplicate handlers if this is called multiple times
        if not logger.handlers:
            # Setup that messages are written to a .log file
            file_handler = logging.FileHandler(log_filepath)
            # Control the level of the messages at the file_handler level
            file_handler.setLevel(logging.INFO)
            # Standard line writing format
            # asctime : ASCII time with datefmt year-month-day hour:minutes:seconds
            formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s', datefmt='%Y-%m-%d %H:%M:%S')
            # Add format to the file_handler
            file_handler.setFormatter(formatter)
            # Add file handler to the logger
            logger.addHandler(file_handler)
            
        # GLOBAL EXCEPTION HANDLER
        # Setup logger so that hard coded messages are being supplemented by global exceptions
        # This represents a safety net in case the code crashes or an unexpected bug is encountered
        def handle_exception(exc_type, exc_value, exc_traceback):
            # Ignore KeyboardInterrupt so you can still stop the script with Ctrl+C
            if issubclass(exc_type, KeyboardInterrupt):
                sys.__excepthook__(exc_type, exc_value, exc_traceback)
                return
            # Log the error and the full traceback as a CRITICAL issue
            logger.critical("Uncaught exception in pipeline:", exc_info=(exc_type, exc_value, exc_traceback))

        # Bind the custom exception handler to Python's default error hook
        sys.excepthook = handle_exception
        
        return logger
    
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

    def _sanitize_spatial_geometry(
        self, 
        ds: Union[xr.DataArray, xr.Dataset], 
        default_crs: str = "EPSG:4326",
        logger: Optional[logging.Logger] = None
    ) -> Union[xr.DataArray, xr.Dataset]:
        """
        Validates and sanitizes xarray spatial metadata for GDAL/rioxarray compatibility.

        This internal method ensures that dimension names are standardized to 'x' and 'y', 
        enforces a Coordinate Reference System (CRS) if missing, clears conflicting 
        dimension encodings, and corrects microscopic floating-point coordinate drift 
        that often occurs during spatial aggregations.

        Parameters
        ----------
        ds : xarray.DataArray or xarray.Dataset
            The lazy xarray object to be sanitized.
        default_crs : str, optional
            The fallback Coordinate Reference System to apply if the input object 
            lacks CRS metadata. Default is ``"EPSG:4326"``.
        logger : logging.Logger, optional
            The logger instance to use for recording execution messages. 
            Default is ``None``.

        Returns
        -------
        xarray.DataArray or xarray.Dataset
            The sanitized xarray object, mathematically aligned and ready for GDAL ingestion.
        """
        log_execution(logger, "Sanitizing spatial geometry...", logging.INFO)
        
        # Standardize Horizontal/Vertical Axes
        dim_map = {}
        for dim in ds.dims:
            dim_str = str(dim).lower()
            if dim_str in ['lon', 'longitude', 'long']:
                dim_map[dim] = 'x'
            elif dim_str in ['lat', 'latitude']:
                dim_map[dim] = 'y'
                
        if dim_map:
            log_execution(logger, f"Renaming dimensions to standard x/y: {dim_map}", logging.INFO)
            ds = ds.rename(dim_map)
        
        ds = ds.rio.set_spatial_dims(x_dim="x", y_dim="y")

        # Enforce Coordinate Reference System (CRS)
        if ds.rio.crs is None:
            log_execution(logger, f"Warning: No CRS found. Enforcing default {default_crs}.", logging.WARNING)
            ds = ds.rio.write_crs(default_crs)

        # Clean Dimension Encoding
        """
        Encoding deals with how data is stored and read from the file from disk. The dictionary holds info on
        which fill values to use, the dtype, scale factor, offset value, chunk sizes, etc.

        Processing data causes changes in memory but xarray holds on to the original encoding. The clear function
        erases how the dimensions were stored originally and makes sure that when we export the data that the new 
        encoding is upheld
        """
        for dim in ['x', 'y']:
            if dim in ds.coords:
                ds[dim].encoding.clear()

        # Safely Erase Microscopic Floating-Point Drift
        if 'x' in ds.coords and 'y' in ds.coords:
            ds = ds.assign_coords(
                x=np.linspace(float(ds.x[0]), float(ds.x[-1]), ds.sizes['x']),
                y=np.linspace(float(ds.y[0]), float(ds.y[-1]), ds.sizes['y'])
            )

        return ds

    def affine_reproject(
        self, 
        input_data: Any, 
        output_filepath: str, 
        grid_name: str, 
        resample_keyword: str = "bilinear",
        compress_mode: str = "lzw",
        memory_limit_bytes: int = 4096,
        logger: Optional[logging.Logger] = None
    ) -> xr.DataArray:
        """
        Out-of-core spatial reprojection and snapping to a strictly defined master grid.

        Accepts either a physical file path or a lazy xarray object. If an xarray object 
        is provided, it is automatically routed through `_sanitize_spatial_geometry` 
        and safely streamed to a temporary disk location to prevent RAM overload. 
        GDAL Warp is then utilized to snap the data perfectly to the master grid specs.

        Parameters
        ----------
        input_data : str or xarray.DataArray or xarray.Dataset
            The path to a physical raster file on disk, or a loaded xarray object.
        output_filepath : str
            The destination file path where the warped GeoTIFF will be saved.
        grid_name : str
            The key of the target grid defined in the class `GRID_REGISTRY`. 
            Supported grids dictate the target CRS, resolution, and exact alignment bounds.
        resample_keyword : str, optional
            The algorithm to use during GDAL reprojection. Options include ``"nearest"``, 
            ``"bilinear"``, ``"cubic"``, ``"average"``, ``"mode"``, ``"max"``, and ``"min"``. 
            Default is ``"bilinear"``.
        compress_mode : str, optional
            The GDAL creation option for output compression. Default is ``"lzw"``.
            * ``"lzw"``: Fast read/write, lossless. A highly compatible classic standard.
            * ``"deflate"``: The industry workhorse. Lossless, yields slightly better compression than LZW.
            * ``"zstd"``: Modern and extremely fast lossless compression (requires compatible GDAL build).
            * ``"packbits"``: Very fast run-length encoding. Effective only for categorical masks.
            * ``"lerc"``: Efficient lossy compression for continuous floating-point analytical data.
            * ``"jpeg"`` / ``"webp"``: Lossy compression strictly for visual RGB imagery.
        memory_limit_bytes : int, optional
            The maximum virtual memory limit (in MB) allocated to the GDAL Warp operation. 
            Default is ``4096`` (4GB).
        logger : logging.Logger, optional
            The logger instance to use for recording execution messages. Default is ``None``.

        Returns
        -------
        xarray.DataArray
            A lazily loaded, chunked DataArray (2048x2048) of the newly warped GeoTIFF.

        Raises
        ------
        Exception
            If the underlying GDAL Warp operation encounters a failure, the exception 
            is logged and re-raised to the main execution thread.

        Notes
        -----
        To ensure perfect alignment across distinct datasets, this function applies mathematical 
        snapping to the output bounding box. It forces the projected dataset to expand outward 
        (using `np.floor` for minimums and `np.ceil` for maximums) until its edges land precisely 
        on the integer-aligned pixel boundaries of the master grid.

        Examples
        --------
        Warping directly from a file path (highly efficient for large physical files):

        >>> reprojected_da = cube.affine_reproject(
        ...     input_data="raw_data/elevation_model.tif",
        ...     output_filepath="processed/elevation_1km.tif",
        ...     grid_name="EEA_1km",
        ...     resample_keyword="bilinear",
        ...     compress_mode="deflate"
        ... )

        Warping an in-memory or lazy xarray object (the function handles the temporary I/O):

        >>> my_array = xr.open_dataarray("temp_data.nc")
        >>> reprojected_da = cube.affine_reproject(
        ...     input_data=my_array,
        ...     output_filepath="processed/temp_data_snapped.tif",
        ...     grid_name="Global_Equal_Area_500m",
        ...     resample_keyword="nearest",
        ...     memory_limit_bytes=8192
        ... )
        """
        # Enable errors
        gdal.UseExceptions() 
        
        log_execution(logger, f"Preparing out-of-core reprojection to {grid_name}...", logging.INFO)
        
        # Fetch Master Grid specs
        spec = self.GRID_REGISTRY[grid_name]
        target_crs = spec["crs"]
        res = spec["resolution"]
        master_minx, master_miny, _, _ = spec["bounds"]

        # Get the right resampler from the class attribute.
        resampler = self.GDAL_RESAMPLERS.get(resample_keyword, gdal.GRA_Bilinear)
        resampler_name = self.RESAMPLER_DECODER.get(resampler, "unknown")
        log_execution(logger, f"Utilizing '{resampler_name}' resampling for reprojection.", logging.INFO)

        temp_file = None
        
        try:
            os.makedirs(os.path.dirname(os.path.abspath(output_filepath)), exist_ok=True)

            # Handle the Input (xarray vs file path)
            if isinstance(input_data, (xr.DataArray, xr.Dataset)):
                log_execution(logger, "Lazy xarray object detected. Preparing for disk stream...", logging.INFO)
                input_data = self._sanitize_spatial_geometry(input_data, logger=logger)
                
                temp_file = "temp_warp_input.tif"
                input_data.rio.to_raster(temp_file, 
                                         tiled=True, 
                                         compress=compress_mode, 
                                         windowed=True, 
                                         BIGTIFF="YES")
                source_path = temp_file
                
                src_crs = input_data.rio.crs
                src_minx, src_miny, src_maxx, src_maxy = input_data.rio.bounds()
                src_nodata = input_data.rio.nodata
            else:
                source_path = input_data
                with rioxarray.open_rasterio(source_path) as info:
                    src_crs = info.rio.crs
                    src_minx, src_miny, src_maxx, src_maxy = info.rio.bounds()
                    src_nodata = info.rio.nodata

            if src_crs is None:
                 log_execution(logger, "Source CRS missing. Assuming EPSG:4326 for GDAL fallback.", logging.WARNING)
                 src_crs = "EPSG:4326"

            # Transform the boundaries of the dataset so that they are aligned with the target grid
            dst_minx, dst_miny, dst_maxx, dst_maxy = transform_bounds(
                src_crs, target_crs, src_minx, src_miny, src_maxx, src_maxy
            )

            # Grid Snapping Math
            snap_minx = master_minx + np.floor((dst_minx - master_minx) / res) * res
            snap_maxx = master_minx + np.ceil((dst_maxx - master_minx) / res) * res
            snap_maxy = master_miny + np.ceil((dst_maxy - master_miny) / res) * res
            snap_miny = master_miny + np.floor((dst_miny - master_miny) / res) * res
            output_bounds = (snap_minx, snap_miny, snap_maxx, snap_maxy)

            # Windowed GDAL Warp
            log_execution(logger, f"Warping to {output_filepath} (Resampling: {resample_keyword})...", logging.INFO)
            
            nodata_val = -9999.0 if (src_nodata is None or np.isnan(src_nodata)) else float(src_nodata)

            warp_options = gdal.WarpOptions(
                format='GTiff',
                dstSRS=target_crs,
                xRes=res,
                yRes=res,
                outputBounds=output_bounds,
                resampleAlg=resampler,
                srcNodata=nodata_val,
                dstNodata=nodata_val,
                creationOptions=[f'COMPRESS={compress_mode.upper()}', 
                                 'TILED=YES',
                                 'BIGTIFF=YES'],
                warpMemoryLimit=memory_limit_bytes,
                warpOptions=['NUM_THREADS=ALL_CPUS'] 
            )
            
            gdal.Warp(output_filepath, source_path, options=warp_options)

            log_execution(logger, "Reprojection complete.", logging.INFO)

        except Exception as e:
            log_execution(logger, f"Error during affine reprojection: {e}", logging.ERROR, exc_info=True)
            raise

        finally:
            # Clean up temp files
            if temp_file and os.path.exists(temp_file):
                try:
                    os.remove(temp_file)
                except OSError:
                    pass             
        return rioxarray.open_rasterio(output_filepath, chunks={'x': 2048, 'y': 2048})
    
    def compute_class_fraction(
        self, 
        source_data: Union[str, xr.DataArray], 
        class_value: int, 
        grid_name: str, 
        output_filepath: str, 
        logger: Optional[logging.Logger] = None
    ) -> str:
        """
        Pure spatial math primitive: Isolates a single class and writes its fractional coverage.
        Automatically routes large disk-based VRTs to pure GDAL streaming, 
        and small in-memory xarray DataArrays to native xarray math.
        """
        import os
        import tempfile
        import numpy as np
        from bmc.utils.logger import log_execution
        
        # =====================================================================
        # BRANCH 1: IN-MEMORY XARRAY (For Small Datasets)
        # =====================================================================
        if isinstance(source_data, xr.DataArray):
            log_execution(logger, f"Processing small DataArray for class {class_value} in RAM...", logging.INFO)
            
            nodata_val = source_data.rio.nodata
            
            # 1. Create the Binary Mask in RAM
            mask = (source_data == class_value).astype(np.float32)
            if nodata_val is not None:
                mask = mask.where(source_data != nodata_val, np.nan)
            
            mask.rio.write_crs(source_data.rio.crs, inplace=True)
            mask.rio.write_transform(source_data.rio.transform(), inplace=True)
            mask.rio.write_nodata(np.nan, inplace=True)

            # 2. Route straight to affine_reproject (handles the small I/O dump safely)
            self.affine_reproject(
                input_data=mask, 
                output_filepath=output_filepath,  
                grid_name=grid_name, 
                resample_keyword="average",
                logger=logger
            )
            return output_filepath

        # =====================================================================
        # BRANCH 2: PURE GDAL STREAMING (For Massive VRTs)
        # =====================================================================
        elif isinstance(source_data, str):
            import rasterio
            
            # 1. Create a safe physical temp file for the raw un-warped mask
            fd, temp_mask_path = tempfile.mkstemp(suffix=".tif", prefix="raw_mask_")
            os.close(fd)

            try:
                log_execution(logger, f"Streaming VRT blocks to calculate mask for class {class_value}...", logging.INFO)
                
                # 2. Stream the VRT directly using GDAL/Rasterio blocks (Zero Dask Overhead)
                # 2. Stream the VRT directly using GDAL/Rasterio blocks (Zero Dask Overhead)
                with rasterio.open(source_data) as src:
                    meta = src.meta.copy()
                    meta.update({
                        "driver": "GTiff",  
                        "dtype": "float32",
                        "nodata": np.nan,
                        "compress": "lzw",
                        "tiled": True,
                        "blockxsize": 2048,
                        "blockysize": 2048,
                        "BIGTIFF": "YES"
                    })
                    with rasterio.open(temp_mask_path, "w", **meta) as dst:
                        for ji, window in src.block_windows(1):
                            block = src.read(1, window=window)
                            mask = (block == class_value).astype(np.float32)
                            
                            if src.nodata is not None:
                                mask[block == src.nodata] = np.nan
                                
                            dst.write(mask, 1, window=window)
                            
                # 3. Hand the physical mask directly to your robust GDAL C++ affine_reproject
                self.affine_reproject(
                    input_data=temp_mask_path, 
                    output_filepath=output_filepath,  
                    grid_name=grid_name, 
                    resample_keyword="average",
                    logger=logger
                )
            finally:
                # 4. Clean up the pre-warp mask file
                if os.path.exists(temp_mask_path):
                    try:
                        os.remove(temp_mask_path)
                    except OSError as e:
                        log_execution(logger, f"Warning: Could not delete temp mask {temp_mask_path}: {e}", logging.WARNING)
                        
            return output_filepath
        
        else:
            raise TypeError("source_data must be either a string path (for VRTs) or an xarray.DataArray")

import geopandas as gpd
import pandas as pd
from shapely import make_valid
from shapely.geometry import MultiPolygon, MultiLineString, MultiPoint
from shapely.geometry.collection import GeometryCollection
from shapely import force_2d
import logging
from typing import Optional


class spatial_vector_engine(base_spatial_grid):
    def coordinate_to_geometry(
    self,
    df: pd.DataFrame,
    x_col: str,
    y_col: str,
    uncert_col: Optional[str] = None,
    output_type: str = "polygon",
    input_crs: str = "EPSG:4326",
    on_missing_uncertainty: str = "fallback",
    quad_segs: int = 8,
    logger: Optional[logging.Logger] = None,
) -> gpd.GeoDataFrame:
        """
        Converts tabular spatial records into a GeoDataFrame, optionally applying
        a geometric buffer based on coordinate uncertainty.
    
        This function acts as a foundational geometric ingestion tool. It takes
        raw coordinates (X/Y) and translates them into strictly defined spatial
        objects. When configured to generate polygons from geographic coordinates
        (degrees), it mitigates high-latitude scaling errors by mathematically
        partitioning records into their respective localized projection zones —
        Universal Transverse Mercator (UTM) between 80°S and 84°N, and Universal
        Polar Stereographic (UPS) beyond those latitudes, where UTM itself
        becomes badly distorted. Buffering is executed natively in undistorted
        meters within each local zone before the final geometries are
        reprojected back to the target coordinate framework.
    
        Parameters
        ----------
        df : pandas.DataFrame
            The input dataframe containing tabular coordinate data and
            associated uncertainty measurements.
        x_col : str
            The exact string name of the column containing the X coordinate
            (e.g., Longitude or Easting).
        y_col : str
            The exact string name of the column containing the Y coordinate
            (e.g., Latitude or Northing).
        uncert_col : str, optional
            The string name of the column containing coordinate uncertainty
            measurements in meters. If this column is missing from the
            DataFrame or passed as None, behavior is controlled by
            `on_missing_uncertainty`. Default is None.
        output_type : {'point', 'polygon'}, optional
            The desired geometric output topology. Default is 'polygon'.
        input_crs : str, optional
            The EPSG code or standard string identifier for the source
            coordinate system. Default is "EPSG:4326" (WGS84).
        on_missing_uncertainty : {'fallback', 'raise'}, optional
            Controls behavior when `output_type='polygon'` but `uncert_col` is
            None or not found in `df.columns`.
            - 'fallback': log a warning and degrade to 'point' geometry
            generation (previous default behavior).
            - 'raise': raise a ValueError instead of silently degrading. This
            is recommended in pipelines where a mistyped column name should
            be caught rather than silently producing points instead of
            polygons.
            Default is 'fallback'.
        quad_segs : int, optional
            Number of line segments used to approximate a quarter-circle when
            buffering (passed to shapely's `buffer`). Higher values produce
            smoother, more accurate circles at the cost of more vertices.
            Matters most when output polygons will be used for areal
            intersection against a raster grid. Default is 8 (shapely default).
        logger : logging.Logger, optional
            An instance of the standard Python logger to track execution
            progress, coordinate fallbacks, and potential data mutation
            warnings. Default is None.
    
        Returns
        -------
        geopandas.GeoDataFrame
            A mathematically transformed and structurally complete GeoDataFrame
            assigned to the supplied input_crs.
    
        Raises
        ------
        ValueError
            If `output_type` is not exactly 'point' or 'polygon'; if
            `on_missing_uncertainty='raise'` and no valid `uncert_col` is
            supplied when `output_type='polygon'`; if `df` contains NaN values
            in `x_col`/`y_col`; or if `df` is empty.
    
        Notes
        -----
        Missing uncertainty values (NaN) within a valid `uncert_col` are
        dynamically filled with 0 prior to buffering. Negative uncertainty
        values are clipped to 0 with a warning, since a negative buffer
        distance on a point silently collapses to an empty geometry rather
        than raising — which would otherwise cause silent row loss in
        downstream areal-intersection use.
        """
    
        # Validate the core output parameter to prevent downstream geometric failures
        if output_type not in ("point", "polygon"):
            raise ValueError("output_type must be either 'point' or 'polygon'")
    
        if on_missing_uncertainty not in ("fallback", "raise"):
            raise ValueError("on_missing_uncertainty must be either 'fallback' or 'raise'")
    
        if df.empty:
            raise ValueError("Input DataFrame is empty; cannot construct geometries.")
    
        # Coordinates are the one thing this function cannot safely default or
        # guess around. A NaN here would silently produce an invalid Point
        # geometry, so we fail loudly instead.
        coord_nan_mask = df[x_col].isna() | df[y_col].isna()
        if coord_nan_mask.any():
            raise ValueError(
                f"Found {int(coord_nan_mask.sum())} row(s) with NaN in '{x_col}' or "
                f"'{y_col}'. Resolve or filter these rows before calling "
                "coordinate_to_geometry."
            )
    
        # =========================================================================
        # PIPELINE SAFETY: HANDLE MISSING UNCERTAINTY COLUMNS
        # =========================================================================
        # If the user requests polygons but fails to provide a valid uncertainty
        # column, the geometry engine cannot mathematically construct a radius.
        # Depending on `on_missing_uncertainty`, we either degrade to Points
        # (previous default) or raise, so a mistyped column name doesn't silently
        # change the output topology in a pipeline.
        if output_type == "polygon":
            if not uncert_col or uncert_col not in df.columns:
                if on_missing_uncertainty == "raise":
                    raise ValueError(
                        f"uncert_col '{uncert_col}' not found in input data and "
                        "output_type='polygon' was requested. Pass a valid "
                        "uncert_col, or set on_missing_uncertainty='fallback' to "
                        "silently degrade to point geometry."
                    )
                log_execution(
                    logger,
                    f"Uncertainty column '{uncert_col}' not found in input data. "
                    "Defaulting to 'point' geometry generation.",
                    level=logging.WARNING,
                )
                output_type = "point"
    
        log_execution(
            logger,
            f"Converting tabular coordinates to {output_type.upper()} geometries in {input_crs}...",
            level=logging.INFO,
        )
    
        # =========================================================================
        # INITIALIZE BASE SPATIAL TOPOLOGY
        # =========================================================================
        # Convert the raw floating-point columns into actual Shapely Point objects.
        # We copy the source dataframe explicitly to shield the underlying raw
        # dataset from in-place property modifications during spatial clustering.
        gdf = gpd.GeoDataFrame(
            df.copy(),
            geometry=gpd.points_from_xy(df[x_col], df[y_col]),
            crs=input_crs,
        )
    
        # =========================================================================
        # POINT STRATEGY RESOLUTION
        # =========================================================================
        # If the requested (or fallback) topology is just points, our work is done.
        # The original uncertainty column (if it exists) is preserved as a standard
        # DataFrame attribute rather than being structurally baked into the geometry.
        if output_type == "point":
            log_execution(logger, "Point generation complete.", level=logging.INFO)
            return gdf
    
        # =========================================================================
        # POLYGON STRATEGY - SANITIZE MISSING / INVALID DATA
        # =========================================================================
        # The spatial buffer method will mathematically fail if fed a NaN value,
        # and will silently return an EMPTY geometry (not an error) if fed a
        # negative value. Both are sanitized explicitly here so bad uncertainty
        # data produces a loud log message rather than a silently missing row
        # downstream.
        missing_count = gdf[uncert_col].isna().sum()
        if missing_count > 0:
            log_execution(
                logger,
                f"Filling {missing_count} missing uncertainty values with 0m to prevent buffering failure.",
                level=logging.WARNING,
            )
            gdf[uncert_col] = gdf[uncert_col].fillna(0)
    
        negative_mask = gdf[uncert_col] < 0
        negative_count = negative_mask.sum()
        if negative_count > 0:
            log_execution(
                logger,
                f"Clipping {negative_count} negative uncertainty value(s) to 0m; "
                "a negative buffer distance silently collapses points to empty "
                "geometry rather than raising.",
                level=logging.WARNING,
            )
            gdf[uncert_col] = gdf[uncert_col].clip(lower=0)
    
        # =========================================================================
        # STEP 4: GEOMETRIC EXPANSION (BUFFERING) WITH CRS AWARENESS
        # =========================================================================
        log_execution(logger, "Applying metric coordinate uncertainty buffers...", level=logging.INFO)
    
        # Check if the user-supplied CRS is geographic (degrees) or projected (meters).
        if gdf.crs.is_geographic:
            log_execution(
                logger,
                "Geographic CRS detected. Dynamically mapping records to localized "
                "UTM/UPS zones to optimize linear scale accuracy...",
                level=logging.INFO,
            )
    
            # To compute true geographic longitudinal strips, we temporarily mirror
            # coordinates into WGS84. Comparing on the resolved EPSG code (rather
            # than the raw input string) means this correctly no-ops for any
            # equivalent spelling of EPSG:4326 (e.g. 4326, "epsg:4326", a
            # pyproj.CRS object), not just the exact string "EPSG:4326".
            if gdf.crs.to_epsg() == 4326:
                gdf_wgs84 = gdf
            else:
                gdf_wgs84 = gdf.to_crs("EPSG:4326")
    
            longitudes = gdf_wgs84.geometry.x
            latitudes = gdf_wgs84.geometry.y
    
            # ---------------------------------------------------------------
            # Zone assignment: UTM for |lat| within standard bounds, UPS for
            # the polar caps where UTM's convergence/scale distortion becomes
            # unacceptable. This mirrors the standard MGRS convention:
            #   - North polar cap:  lat >= 84°N   -> EPSG:32661 (UPS North)
            #   - South polar cap:  lat <  80°S   -> EPSG:32761 (UPS South)
            #   - Everything else:  standard UTM zone/hemisphere
            # ---------------------------------------------------------------
            UPS_NORTH_EPSG = 32661
            UPS_SOUTH_EPSG = 32761
    
            north_polar_mask = latitudes >= 84.0
            south_polar_mask = latitudes < -80.0
            utm_mask = ~(north_polar_mask | south_polar_mask)
    
            zone_epsg = pd.Series(index=gdf.index, dtype="int64")
            zone_epsg.loc[north_polar_mask] = UPS_NORTH_EPSG
            zone_epsg.loc[south_polar_mask] = UPS_SOUTH_EPSG
    
            if utm_mask.any():
                # UTM zones are 6-degree longitudinal strips numbered 1-60.
                # Longitude of exactly +180 must clamp to zone 60, not roll
                # over into a nonexistent zone 61.
                utm_zones = np.clip(
                    ((longitudes[utm_mask] + 180) / 6).astype(int) + 1, 1, 60
                )
                # 32600 handles the Northern Hemisphere, 32700 the Southern.
                epsg_prefixes = np.where(latitudes[utm_mask] >= 0, 32600, 32700)
                zone_epsg.loc[utm_mask] = epsg_prefixes + utm_zones
    
            # Namespaced temp column name to avoid silently colliding with a
            # pre-existing column of the same name in the caller's data.
            zone_col = "_coord_to_geom_zone_epsg"
            gdf[zone_col] = zone_epsg.astype(int)
    
            # Execute buffering grouped by localized metric zone (UTM or UPS).
            log_execution(logger, "Batch-processing polygons within localized metric zones...", level=logging.INFO)
            buffered_chunks = []
    
            for zone_epsg_code, group in gdf.groupby(zone_col):
                # Cast the localized group into its respective conformal metric projection
                group_metric = group.to_crs(f"EPSG:{zone_epsg_code}")
    
                # Draw spatial boundaries using real-world meters with minimal scale distortion
                group_metric["geometry"] = group_metric.geometry.buffer(
                group_metric[uncert_col], resolution=quad_segs
            )
    
                # Snap the newly formed polygons straight back to the user's target system
                buffered_chunks.append(group_metric.to_crs(input_crs))
    
            # Re-aggregate structural subsets, clear grid-zone columns, and restore indices.
            # Wrapping explicitly in gpd.GeoDataFrame guards against pd.concat
            # silently downgrading to a plain DataFrame / dropping CRS metadata
            # on some geopandas/pandas version combinations.
            gdf_polygon = gpd.GeoDataFrame(
                pd.concat(buffered_chunks).sort_index(), crs=input_crs
            )
            gdf_polygon = gdf_polygon.drop(columns=[zone_col])
        else:
            # If the input CRS is already a metric projection (e.g., EPSG:3035),
            # we completely bypass regional batching and draw buffers natively.
            log_execution(logger, "Projected CRS detected. Buffering directly in native units.", level=logging.INFO)
            gdf_polygon = gdf.copy()
            gdf_polygon["geometry"] = gdf_polygon.geometry.buffer(
            gdf_polygon[uncert_col], resolution=quad_segs
        )
    
        log_execution(logger, "Coordinate uncertainty polygon generation complete.", level=logging.INFO)
    
        return gdf_polygon

    def sanitize_geometries(
    self, 
    gdf: gpd.GeoDataFrame, 
    allowed_types: Optional[List[str]] = None,
    force_multi: bool = True,
    deduplicate: bool = False,
    make_valid_method: str = "linework",
    logger: Optional[logging.Logger] = None
) -> gpd.GeoDataFrame:
        """
        Cleans, flattens, normalizes, and validates dirty vector geometries using 
        highly optimized vectorized Shapely C-operations where possible.

        Operational Mechanics:
        1. Removes completely null, missing, or structurally empty geometries.
        2. Drops the Z/M coordinates to enforce a strict 2D planar workspace.
        3. Isolates topologically invalid shapes (e.g., self-intersections) and heals them.
        4. Unpacks messy GeometryCollections, extracting only the highest-dimensional assets.
        5. Runs a safety re-validation check to drop unresolvable geometric anomalies.
        6. Homogenizes features into their Multi* variants to prevent downstream schema mismatches.
        7. Optionally drops exact geometric duplicates and filters by case-sensitive geometry types.
        8. Resets the index to guarantee safe table merges and joins downstream.

        Parameters
        ----------
        gdf : geopandas.GeoDataFrame
            The raw input vector dataset containing potentially corrupt or mixed geometries.
        allowed_types : list of str, optional
            Case-sensitive geometry types allowed in the final output layer 
            (e.g., ['Polygon', 'MultiPolygon']). **If left empty or None, all valid 
            geometry types are permitted to pass through.**
        force_multi : bool, default True
            If True, normalizes single/atomic geometries to their Multi* counterparts 
            (e.g., Polygon -> MultiPolygon) to ensure index/schema uniformity.
        deduplicate : bool, default False
            If True, executes an exact spatial lookup to drop duplicate geometry rows.
        make_valid_method : str, default 'linework'
            The underlying GEOS algorithm used for fixing broken topologies. 
            Options are 'linework' (standard) or 'structure' (preserves grid lines better).
        logger : logging.Logger, optional
            Active pipeline logger instance for streaming system telemetry.

        Returns
        -------
        geopandas.GeoDataFrame
            A pristine, topologically valid, schema-homogenized dataset with a clean contiguous index.
        """
        log_execution(logger, "Initiating vector geometry sanitization...", level=logging.INFO)
        
        # -------------------------------------------------------------------------
        # STRUCTURAL INPUT GUARDRAILS & TYPO PROTECTION
        # -------------------------------------------------------------------------
        if gdf.empty:
            log_execution(logger, "[WARNING] Input GeoDataFrame is empty. Returning empty copy.", level=logging.WARNING)
            return gdf.copy()

        # Define the strict internal vocabulary recognized by Shapely's .geom_type property
        VALID_GEOM_TYPES = {
            "Point", "MultiPoint", "LineString", "MultiLineString", 
            "Polygon", "MultiPolygon", "GeometryCollection"
        }
        
        # If a filter list is provided, validate it upfront to prevent silent runtime failure
        if allowed_types:
            for t in allowed_types:
                if t not in VALID_GEOM_TYPES:
                    raise ValueError(
                        f"Invalid type '{t}' in allowed_types. Must match standard Shapely case-sensitive "
                        f"vocabulary: {list(VALID_GEOM_TYPES)}"
                    )

        initial_count = len(gdf)
        
        # -------------------------------------------------------------------------
        # PURGE NULLS AND STRUCTURALLY EMPTY GEOMETRIES (THE GHOSTS)
        # -------------------------------------------------------------------------
        # dropna avoids triggering the messy GeoPandas warning when dropping missing geometries
        gdf = gdf.dropna(subset=['geometry'])
        # Strip empty representations like "Polygon()" which contain no vertices
        gdf = gdf[~gdf.geometry.is_empty].copy()
        
        dropped_empty = initial_count - len(gdf)
        if dropped_empty > 0:
            log_execution(logger, f"Dropped {dropped_empty} empty or null geometries.", level=logging.INFO)

        # -------------------------------------------------------------------------
        # VECTORIZED FORCE 2D PLANAR GEOMETRIES (DROP Z/M AXES)
        # -------------------------------------------------------------------------
        # Fast C-Array Operation: Flattens 3D/4D dimensions down to native X and Y coordinates
        gdf.geometry = shapely.force_2d(gdf.geometry.values)

        # -------------------------------------------------------------------------
        # VECTORIZED TOPOLOGY HEALING (THE BOWTIES)
        # -------------------------------------------------------------------------
        # Isolate only the invalid rows to save processing cycles over large datasets
        invalid_mask = ~gdf.geometry.is_valid.values
        invalid_count = invalid_mask.sum()
        
        if invalid_count > 0:
            log_execution(logger, f"Healing {invalid_count} topologically invalid geometries via '{make_valid_method}'...", level=logging.INFO)
            # Vectorized array healing; generates structural representations or collections if needed
            healed_geoms = shapely.make_valid(gdf.loc[invalid_mask, 'geometry'].values, method=make_valid_method)
            gdf.loc[invalid_mask, 'geometry'] = healed_geoms

        # -------------------------------------------------------------------------
        # UNPACK GEOMETRYCOLLECTIONS (SCOPED STRICTLY TO HEALED ROW SUBSET)
        # -------------------------------------------------------------------------
        healed_subset = gdf.loc[invalid_mask]
        collection_mask = (healed_subset.geometry.geom_type == "GeometryCollection").values
        
        if collection_mask.any():
            collection_indices = healed_subset.index[collection_mask]
            log_execution(logger, f"Unpacking {len(collection_indices)} complex GeometryCollections...", level=logging.INFO)
            
            audited_dropped_parts = 0
            audited_dropped_features = 0
            updated_geometries = []

            # Iterate only through the row indices flagged as structural collections
            for idx in collection_indices:
                geom = gdf.loc[idx, 'geometry']
                parts = list(geom.geoms)
                
                # Sort individual parts based on their structural complexity/topological dimensions
                polygons = [p for p in parts if p.geom_type in ['Polygon', 'MultiPolygon']]
                lines = [p for p in parts if p.geom_type in ['LineString', 'MultiLineString']]
                points = [p for p in parts if p.geom_type in ['Point', 'MultiPoint']]
                
                selected_geom = None
                dropped_count = 0
                
                # Prioritize Polygons > Lines > Points to pick the dominant dimension
                if polygons:
                    # shapely.get_parts recursively flattens internal MultiPolygons to avoid an atomic list crash
                    flattened_polys = shapely.get_parts(polygons)
                    selected_geom = shapely.MultiPolygon(flattened_polys)
                    dropped_count = len(lines) + len(points)
                elif lines:
                    flattened_lines = shapely.get_parts(lines)
                    selected_geom = shapely.MultiLineString(flattened_lines)
                    dropped_count = len(points)
                elif points:
                    flattened_points = shapely.get_parts(points)
                    selected_geom = shapely.MultiPoint(flattened_points)

                # Keep track of dropped dimension fragments for audit logs
                if dropped_count > 0:
                    audited_dropped_parts += dropped_count
                    audited_dropped_features += 1
                    
                updated_geometries.append(selected_geom)
                
            # Log a warning to make sure any loss of small slivers/lines is completely auditable
            if audited_dropped_parts > 0:
                log_execution(
                    logger, 
                    f"[AUDIT] Discarded {audited_dropped_parts} lower-dimension sliver parts across "
                    f"{audited_dropped_features} split features to preserve topological dimensionality.", 
                    level=logging.WARNING
                )
                
            gdf.loc[collection_indices, 'geometry'] = updated_geometries

        # -------------------------------------------------------------------------
        # PIPELINE SAFETY RE-VALIDATION CHECK
        # -------------------------------------------------------------------------
        # Double-check that the GEOS algorithms didn't generate any unresolvable geometric artifacts
        post_healing_invalid = ~gdf.geometry.is_valid.values
        if post_healing_invalid.any():
            failed_count = post_healing_invalid.sum()
            log_execution(
                logger, 
                f"[CRITICAL] {failed_count} features failed post-healing validation check. Purging unresolvable structures.", 
                level=logging.ERROR
            )
            gdf = gdf[~post_healing_invalid].copy()

        # -------------------------------------------------------------------------
        #: SCHEMA NORMALIZATION (ENFORCE TYPE HOMOGENEITY)
        # -------------------------------------------------------------------------
        # Converts singular primitives (e.g. Polygon) into single-element Multi-primitives.
        # Prevents runtime crashes when exporting to rigid vector formats (like shapefiles/Parquet schemas).
        if force_multi and not gdf.empty:
            gdf.geometry = gdf.geometry.apply(
                lambda g: shapely.multipoints([g]) if g.geom_type == 'Point' else (
                        shapely.multilinestrings([g]) if g.geom_type == 'LineString' else (
                        shapely.multipolygons([g]) if g.geom_type == 'Polygon' else g))
            )

        # -------------------------------------------------------------------------
        # GEOMETRY DEDUPLICATION (OPTIONAL)
        # -------------------------------------------------------------------------
        if deduplicate and not gdf.empty:
            pre_dedup = len(gdf)
            gdf = gdf.drop_duplicates(subset=['geometry']).copy()
            dedup_delta = pre_dedup - len(gdf)
            if dedup_delta > 0:
                log_execution(logger, f"Deduplication removed {dedup_delta} exact geometry overlaps.", level=logging.INFO)

        # -------------------------------------------------------------------------
        # TYPE ENFORCEMENT
        # -------------------------------------------------------------------------
        # Note: If allowed_types was left as None/Empty, this whole block is skipped
        # and all valid geometry types are safely permitted to remain in the dataset.
        if allowed_types and not gdf.empty:
            type_mask = gdf.geometry.geom_type.isin(allowed_types).values
            dropped_type = len(gdf) - type_mask.sum()
            gdf = gdf[type_mask].copy()
            if dropped_type > 0:
                log_execution(
                    logger, 
                    f"Filtered out {dropped_type} features not matching allowed types: {allowed_types}", 
                    level=logging.INFO
                )

        # -------------------------------------------------------------------------
        # RESET INDEX FOR SEAMLESS MERGES
        # -------------------------------------------------------------------------
        # Clears fragmented, non-contiguous indexing caused by row-dropping operations
        gdf = gdf.reset_index(drop=True)

        # -------------------------------------------------------------------------
        # PIPELINE TERMINATION ASSESSMENT
        # -------------------------------------------------------------------------
        final_count = len(gdf)
        if final_count == 0:
            # Elevated warning level flags that the sanitization stripped everything
            log_execution(
                logger, 
                f"[WARNING] Geometry sanitization reduced feature count from {initial_count} to 0 rows. Pipeline halted.", 
                level=logging.WARNING
            )
        else:
            log_execution(logger, f"Sanitization complete. Final feature count: {final_count}", level=logging.INFO)
            
        return gdf

    def transform_cellCollection_to_template(
        self, 
        source_gdf: gpd.GeoDataFrame, 
        target_grid_name: str, 
        value_column: str, 
        data_type: str = 'discrete', 
        method: str = 'kdtree',
        target_bbox: Optional[Tuple[float, float, float, float]] = None,
        logger: Optional[logging.Logger] = None) -> gpd.GeoDataFrame:
        """
        Transforms data from a source GeoDataFrame to match a strictly aligned master grid.

        Parameters:
        -----------
        source_gdf : geopandas.GeoDataFrame
            The input data to transform. Must have a defined CRS.
        target_grid_name : str
            The key of the template grid defined in `self.GRID_REGISTRY`.
        value_column : str
            The name of the numeric column to aggregate.
        data_type : str
            'discrete' (sum whole integers/majority rule) or 'continuous' (areal weighting).
        method : str
            'kdtree' (representative point snapping) or 'intersect' (geometric clipping).
        target_bbox : tuple of float, optional
            A specific bounding box (minx, miny, maxx, maxy) in the target CRS to force 
            the grid generation. If None, it dynamically calculates from the source data.
        """
        if source_gdf.crs is None:
            raise ValueError("Source GeoDataFrame is missing a CRS. Cannot project.")

        target_crs = self.GRID_REGISTRY[target_grid_name]["crs"]

        # 1. Determine the Bounding Box
        if target_bbox is not None:
            if logger: logger.info(f"Using explicitly provided target bounding box: {target_bbox}")
            dst_bbox = target_bbox
        else:
            if logger: logger.info("No target_bbox provided. Dynamically calculating from source data extent...")
            src_bounds = source_gdf.total_bounds 
            dst_bbox = transform_bounds(source_gdf.crs, target_crs, *src_bounds)

        # 2. Fetch the pristine blueprint
        template_da, _ = self.create_aligned_raster_template(dst_bbox, target_grid_name)
        res = template_da.attrs["res"]
        
        x_centers = template_da.x.values
        y_centers = template_da.y.values

        # 3. Build the target grid mesh (Pristine Squares)
        if logger: logger.info(f"Building {res}m target mesh in {target_crs}...")
        
        xx, yy = np.meshgrid(x_centers, y_centers)
        x_flat, y_flat = xx.flatten(), yy.flatten()
        half_res = res / 2.0
        
        polygons = [box(x - half_res, y - half_res, x + half_res, y + half_res) for x, y in zip(x_flat, y_flat)]
        
        target_grid_gdf = gpd.GeoDataFrame(
            {'grid_id': np.arange(len(polygons))}, 
            geometry=polygons, 
            crs=target_crs
        )

        # 4. Prepare Source Data Attributes
        source_df = source_gdf.copy()
        source_df['src_uid'] = source_df.index 
        
        ignore_cols = ['geometry', value_column, 'src_uid', 'source_area']
        preserve_cols = [col for col in source_df.columns if col not in ignore_cols]
        group_cols = ['grid_id'] + preserve_cols

        # ==========================================
        # STRATEGY A: KDTREE (Representative Point Snapping)
        # ==========================================
        if method == 'kdtree':
            if data_type != 'discrete':
                raise ValueError("KDTree routing mathematically requires 'discrete' data_type.")
            if logger: logger.info("Executing KDTree snapping via representative points...")
                
            source_points = source_df.copy()
            source_points["geometry"] = source_points.geometry.representative_point()
            source_points = source_points.to_crs(target_crs)
            
            src_coords = np.column_stack([source_points.geometry.x, source_points.geometry.y])
            tgt_coords = np.column_stack([x_flat, y_flat]) 
            
            tree = KDTree(tgt_coords)
            _, matched_grid_ids = tree.query(src_coords)
            source_df['grid_id'] = matched_grid_ids
            
            aggregated = source_df.groupby(group_cols)[value_column].sum().reset_index()

        # ==========================================
        # STRATEGY B: INTERSECT (Geometric Overlay & Areal Weighting)
        # ==========================================
        elif method == 'intersect':
            import warnings
            if logger: logger.info("Executing precise geometric overlay (Warning: RAM intensive)...")
            
            source_reprojected = source_df.to_crs(target_crs)
            source_reprojected['source_area'] = source_reprojected.geometry.area
            
            # Temporarily mute the benign keep_geom_type overlay warning
            with warnings.catch_warnings():
                warnings.simplefilter("ignore")
                intersections = gpd.overlay(source_reprojected, target_grid_gdf, how='intersection')
            
            if intersections.empty:
                return target_grid_gdf.iloc[0:0].copy()
                
            intersections['intersect_area'] = intersections.geometry.area

            if data_type == 'continuous':
                intersections['weight'] = intersections['intersect_area'] / intersections['source_area']
                intersections['weighted_val'] = intersections[value_column] * intersections['weight']
                aggregated = intersections.groupby(group_cols)['weighted_val'].sum().reset_index()
                aggregated.rename(columns={'weighted_val': value_column}, inplace=True)

            elif data_type == 'discrete':
                idx_max = intersections.sort_values('intersect_area', ascending=False).groupby('src_uid').head(1)
                aggregated = idx_max.groupby(group_cols)[value_column].sum().reset_index()
            else:
                raise ValueError("Invalid data_type for intersect. Choose 'continuous' or 'discrete'.")
                
        else:
            raise ValueError("Invalid method. Choose 'kdtree' or 'intersect'.")

        # 5. Re-attach the pristine target grid geometries
        if logger: logger.info("Merging aggregated attributes back to pristine template grid...")
        target_geometries = target_grid_gdf[['grid_id', 'geometry']]
        result_grid = target_geometries.merge(aggregated, on='grid_id', how='inner')
        
        return result_grid
    
    def _calculate_distributional_fidelity(
        self, 
        orig_gdf: gpd.GeoDataFrame, 
        targ_gdf: gpd.GeoDataFrame, 
        value_col: str
    ) -> dict:
        r"""
        Computes the Distributional Fidelity (Value Conservation and Spatial Drift).

        Calculates the Center of Mass (CoM) shift and the Wasserstein (Earth Mover's) 
        Distance between two spatial distributions. Includes dynamic baseline shifting 
        to support intensive continuous variables (e.g., negative temperatures).

        Mathematical Formulas
        ---------------------
        Center of Mass Shift ($\Delta\vec{c}$):
        $$\Delta\vec{c}=\sqrt{(x_{t}-x_{o})^2+(y_{t}-y_{o})^2}$$
        Where $(x_{o},y_{o})$ and $(x_{t},y_{t})$ are the weighted average coordinates 
        of the original and target feature clouds.

        Wasserstein Distance ($W_2$):
        $$W_2(\mu,\nu)=\left(\inf_{\gamma\in\Gamma(\mu,\nu)}\int_{M\timesM}d(x,y)^2d\gamma(x,y)\right)^{1/2}$$
        Where $\mu$ and $\nu$ represent the normalized probability distribution of the measured values.

        Interpretation
        --------------
        * value_delta: Absolute difference in the aggregated variable. Values $>0$ indicate dropped data volume.
        * com_shift_meters: Absolute translation of the dataset's center. $0.0$ is perfect.
        * wasserstein_meters: The spatial cost to restructure the data. Higher values indicate 
          severe "smearing" or dislocation of the values across the geometry.
        """
        w_orig = orig_gdf[value_col].values.astype(float)
        w_targ = targ_gdf[value_col].values.astype(float)
        
        value_diff = w_orig.sum() - w_targ.sum()
        
        if w_orig.sum() == 0 or w_targ.sum() == 0:
            return {'value_delta': value_diff, 'com_shift_meters': np.nan, 'wasserstein_meters': np.nan}

        # Dynamic Baseline Shift for Intensive/Continuous Variables
        global_min = min(np.min(w_orig), np.min(w_targ))
        if global_min < 0:
            shift_factor = abs(global_min) + 1e-6
            w_orig = w_orig + shift_factor
            w_targ = w_targ + shift_factor

        orig_cx = np.average(orig_gdf.geometry.centroid.x.values, weights=w_orig)
        orig_cy = np.average(orig_gdf.geometry.centroid.y.values, weights=w_orig)
        targ_cx = np.average(targ_gdf.geometry.centroid.x.values, weights=w_targ)
        targ_cy = np.average(targ_gdf.geometry.centroid.y.values, weights=w_targ)
        
        com_shift = np.sqrt((targ_cx - orig_cx)**2 + (targ_cy - orig_cy)**2)
        
        coords_orig = np.column_stack((orig_gdf.geometry.centroid.x, orig_gdf.geometry.centroid.y))
        coords_targ = np.column_stack((targ_gdf.geometry.centroid.x, targ_gdf.geometry.centroid.y))
        
        p_orig = w_orig / w_orig.sum()
        p_targ = w_targ / w_targ.sum()
        
        cost_matrix = ot.dist(coords_orig, coords_targ, metric='euclidean')
        
        # Eliminate microscopic floating-point drift that confuses the POT solver
        cost_matrix[cost_matrix < 1e-4] = 0.0
        
        # Calculate with the cleaned matrix and a higher iteration ceiling
        emd_meters = ot.emd2(p_orig, p_targ, cost_matrix, numItermax=5000000)
        
        return {
            'value_delta': value_diff,
            'com_shift_meters': com_shift,
            'wasserstein_meters': emd_meters
        }

    def _calculate_geometric_fidelity(
        self, 
        orig_gdf: gpd.GeoDataFrame, 
        targ_gdf: gpd.GeoDataFrame
    ) -> float:
        r"""
        Computes the Geometric Fidelity (Shape and Boundary Conservation).

        Dynamically evaluates the Intersection over Union (IoU) depending on the 
        detected geometry type to quantify footprint warping.

        Mathematical Formulas
        ---------------------
        Intersection over Union ($IoU$):
        $$IoU=\frac{|A\capB|}{|A\cupB|}$$
        Where $A$ is the original macroscopic footprint and $B$ is the target footprint. 
        Calculated using area for polygons and length for linestrings.

        Interpretation
        --------------
        * mean_iou: Ranges from $0.0$ to $1.0$. 
          - $1.0$: Perfect boundary preservation.
          - $<0.8$: Noticeable boundary warping or clipping.
          - $<0.5$: Severe geometric artefacting.
        """
        geom_type = orig_gdf.geometry.iloc[0].geom_type
        
        poly_orig = orig_gdf.geometry.unary_union
        poly_targ = targ_gdf.geometry.unary_union
        
        if geom_type in ['Polygon', 'MultiPolygon']:
            intersection_val = poly_orig.intersection(poly_targ).area
            union_val = poly_orig.union(poly_targ).area
        elif geom_type in ['LineString', 'MultiLineString']:
            intersection_val = poly_orig.intersection(poly_targ).length
            union_val = poly_orig.union(poly_targ).length
        else:
            return np.nan
            
        return intersection_val / union_val if union_val > 0 else 0.0

    def _calculate_topological_fidelity(    
        self, 
        orig_gdf: gpd.GeoDataFrame, 
        targ_gdf: gpd.GeoDataFrame, 
        value_col: str
    ) -> float:
        r"""
        Computes Topological Fidelity (Clustering and Neighborhoods).

        Calculates the change in Spatial Autocorrelation (Moran's I) to determine 
        if local spatial hotspots or data gradients were artificially smoothed out.

        Mathematical Formulas
        ---------------------
        Moran's I ($I$):
        $$I=\frac{N}{W}\frac{\sum_{i}\sum_{j}w_{ij}(x_{i}-\bar{x})(x_{j}-\bar{x})}{\sum_{i}(x_{i}-\bar{x})^2}$$
        Where $N$ is the number of spatial units, $W$ is the sum of all spatial weights 
        $w_{ij}$, and $x$ represents the measured values.

        Interpretation
        --------------
        * morans_i_delta: Difference between target and original $I$.
          - $0.0$: Local clustering relationships perfectly preserved.
          - Highly Negative: Clustered hotspots/gradients were scattered into random noise.
        """
        import warnings
        import os
        from contextlib import redirect_stdout, redirect_stderr
        
        if not HAS_TOPOLOGY or len(orig_gdf) < 4 or len(targ_gdf) < 4:
            return np.nan
            
        geom_type = orig_gdf.geometry.iloc[0].geom_type
        w_orig = orig_gdf[value_col].values
        w_targ = targ_gdf[value_col].values
        
        try:
            # 1. Mute standard Python warnings (for esda divide-by-zero alerts)
            with warnings.catch_warnings():
                warnings.simplefilter("ignore")
                
                # 2. Mute hardcoded print statements (for libpysal island spam)
                with open(os.devnull, 'w') as fnull:
                    with redirect_stdout(fnull), redirect_stderr(fnull):
                        
                        if geom_type in ['Polygon', 'MultiPolygon']:
                            w_mat_orig = Queen.from_dataframe(orig_gdf, use_index=False)
                            w_mat_targ = Queen.from_dataframe(targ_gdf, use_index=False)
                        else:
                            w_mat_orig = KNN.from_dataframe(orig_gdf, k=4)
                            w_mat_targ = KNN.from_dataframe(targ_gdf, k=4)
                            
                        w_mat_orig.transform = 'r'
                        w_mat_targ.transform = 'r'
                        
                        m_orig = Moran(w_orig, w_mat_orig)
                        m_targ = Moran(w_targ, w_mat_targ)
                        
                        return m_targ.I - m_orig.I
        except Exception:
            return np.nan

    def evaluate_transformation_fidelity(
        self, 
        orig_gdf: gpd.GeoDataFrame, 
        targ_gdf: gpd.GeoDataFrame, 
        value_col: str, 
        group_cols: list,
        shared_crs: str = "EPSG:3035",
        logger: logging.Logger = None
    ) -> tuple[pd.DataFrame, dict]:
        """
        Evaluates the comprehensive spatial fidelity between an original and 
        transformed GeoDataFrame across multiple dimensions.

        This synthesis function acts as a mathematical Quality Assurance (QA) gate 
        for spatial pipelines. It stratifies the data using `group_cols` to ensure 
        "apples-to-apples" comparisons, then computes Distributional, Geometric, 
        and Topological fidelity metrics. Finally, it aggregates global statistics 
        and triggers logging warnings if severe data loss, boundary warping, or 
        spatial drift is detected.

        Parameters
        ----------
        orig_gdf : geopandas.GeoDataFrame
            The original, pre-transformation spatial dataset.
        targ_gdf : geopandas.GeoDataFrame
            The transformed, post-regridding spatial dataset.
        value_col : str
            The name of the column containing the numeric measurements (e.g., 
            species occurrences, temperature, pollutant concentration). Used 
            to calculate mass conservation and weighted centers of mass.
        group_cols : list of str
            Columns used to partition the data before evaluation 
            (e.g., ``['scientificname', 'year', 'month']``). This ensures the 
            metrics evaluate specific isolated spatial networks rather than the 
            entire bulk dataset at once.
        shared_crs : str, optional
            A metric Coordinate Reference System (EPSG code) into which both datasets 
            are temporarily projected. This is strictly required to ensure that Earth 
            Mover's Distance and Area Intersection math operates in flat, real-world 
            meters regardless of the native projections. Default is ``"EPSG:3035"``.
        logger : logging.Logger, optional
            A standard Python logger. If provided, the function will log execution 
            progress and throw ``[WARNING]`` or ``[CRITICAL]`` alerts if the spatial 
            distortion exceeds predefined mathematical thresholds.

        Returns
        -------
        df_results : pandas.DataFrame
            A highly detailed, stratum-level breakdown. Contains one row for every 
            unique combination of `group_cols`, alongside the following metrics:
            - ``original_value_sum``: Total value mass of the original stratum.
            - ``value_delta``: Absolute loss or hallucination of values.
            - ``com_shift_meters``: Distance the Center of Mass drifted.
            - ``wasserstein_meters``: Total spatial restructuring cost (Earth Mover's).
            - ``mean_iou``: Intersection over Union of the physical footprint.
            - ``morans_i_delta``: Change in spatial autocorrelation/clustering.
        summary_stats : dict
            High-level global aggregation of the metrics used for pipeline monitoring.
            Contains keys: ``total_value_delta``, ``avg_com_shift``, ``max_com_shift``, 
            ``avg_iou``, and ``avg_wasserstein``.

        Notes
        -----
        The function applies an automated logging triage based on the calculated 
        ``summary_stats``.

        * **Warning Level 1 (Data Volume):** 
          Triggers a ``[CRITICAL]`` error if ``total_value_delta > 1e-6``. This 
          guarantees that the physical mass or count of your data was perfectly 
          conserved during the transformation.
        * **Warning Level 2 (Geometric Destruction):**
          Triggers a ``[WARNING]`` if ``avg_iou < 0.85`` and a ``[CRITICAL]`` error 
          if ``avg_iou < 0.6``. 
          *Note:* If you are intentionally upscaling or downscaling resolution 
          (e.g., mapping 250m to 500m), a low IoU is mathematically expected 
          due to the 4x area expansion, and this warning can be safely ignored.
        * **Warning Level 3 (Spatial Drift):**
          Triggers a ``[WARNING]`` if the ``avg_com_shift`` exceeds 1000 meters, 
          indicating that the regridding algorithm dragged your spatial features 
          unacceptably far from their geographic origins.
        """
        log_execution(logger, "Initiating Universal Spatial Fidelity Profiling...", level=logging.INFO)

        orig_proj = orig_gdf.to_crs(shared_crs).copy()
        targ_proj = targ_gdf.to_crs(shared_crs).copy()
        
        results = []
        groups = orig_proj.groupby(group_cols)
        
        for group_keys, orig_group in groups:
            group_dict = dict(zip(group_cols, group_keys if isinstance(group_keys, tuple) else [group_keys]))
            
            query_mask = np.ones(len(targ_proj), dtype=bool)
            for col, val in group_dict.items():
                query_mask &= (targ_proj[col] == val)
            targ_group = targ_proj[query_mask]
            
            if targ_group.empty:
                continue

            dist_metrics = self._calculate_distributional_fidelity(orig_group, targ_group, value_col)
            geom_iou = self._calculate_geometric_fidelity(orig_group, targ_group)
            topo_delta = self._calculate_topological_fidelity(orig_group, targ_group, value_col)

            results.append({
                **group_dict,
                'original_value_sum': orig_group[value_col].sum(),
                **dist_metrics,
                'mean_iou': geom_iou,
                'morans_i_delta': topo_delta
            })

        df_results = pd.DataFrame(results)

        # ---------------------------------------------------------
        # AGGREGATE SUMMARY STATISTICS & LOGGING THRESHOLDS
        # ---------------------------------------------------------
        summary_stats = {
            'total_value_delta': df_results['value_delta'].sum() if not df_results.empty else 0.0,
            'avg_com_shift': df_results['com_shift_meters'].mean() if not df_results.empty else np.nan,
            'max_com_shift': df_results['com_shift_meters'].max() if not df_results.empty else np.nan,
            'avg_iou': df_results['mean_iou'].mean() if not df_results.empty else np.nan,
            'avg_wasserstein': df_results['wasserstein_meters'].mean() if not df_results.empty else np.nan
        }

        if not df_results.empty:
            log_execution(logger, f"Fidelity Profiling Complete. Evaluated {len(df_results)} unique strata.", level=logging.INFO)
            
            # Use nan-safe logging for IoU (handles Point geometries gracefully)
            if not np.isnan(summary_stats['avg_iou']):
                log_execution(logger, f"Global Average IoU: {summary_stats['avg_iou']:.4f}", level=logging.INFO)
            
            log_execution(logger, f"Global Average Drift: {summary_stats['avg_com_shift']:.2f} meters", level=logging.INFO)

            # Warning Level 1: Data Loss / Baseline Shift
            if abs(summary_stats['total_value_delta']) > 1e-6:
                log_execution(logger, f"[CRITICAL] Pipeline altered absolute data volume. Net value delta: {summary_stats['total_value_delta']:.4f} units.", level=logging.ERROR)

            # Warning Level 2: Severe Artefacting (Geometric Destruction) - Only flags if IoU is a valid number
            if not np.isnan(summary_stats['avg_iou']):
                if summary_stats['avg_iou'] < 0.6:
                    log_execution(logger, f"[CRITICAL] Average IoU is critically low ({summary_stats['avg_iou']:.2f}). Severe geometric warping occurred.", level=logging.ERROR)
                elif summary_stats['avg_iou'] < 0.85:
                    log_execution(logger, f"[WARNING] Moderate boundary distortion detected. Average IoU: {summary_stats['avg_iou']:.2f}.", level=logging.WARNING)

            # Warning Level 3: Unacceptable Spatial Drift
            if not np.isnan(summary_stats['avg_com_shift']) and summary_stats['avg_com_shift'] > 1000:
                log_execution(logger, f"[WARNING] High spatial drift detected. Average center of mass shifted by {summary_stats['avg_com_shift']:.2f} meters.", level=logging.WARNING)

        return df_results, summary_stats
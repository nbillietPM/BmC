import os
import sys
import glob
import gc
from pathlib import Path
import tempfile
import uuid

import logging
from typing import Optional, Union, Dict, Any, List, Tuple
from bmc.utils.logger import log_execution

import numpy as np
import xarray as xr
import pandas as pd
import rioxarray
from osgeo import gdal
import uuid
from pyproj import CRS, Transformer
import rasterio
from rasterio.warp import transform_bounds
from rasterio.transform import from_origin
from rasterio.enums import Resampling

class spatial_engine():
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
    GRID_REGISTRY : dict
        The master registry of supported target projection frameworks. Contains exact 
        Coordinate Reference Systems (CRS), absolute metric or degree resolutions, 
        and mathematically rigid definitive bounding boxes to ensure perfect 
        pixel alignment across disparate environmental products.

    Methods
    -------
    resolve_grid_registry_key(target_grid, target_resolution, logger=None)
        Dynamically constructs and validates the master grid key from user configuration.
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

    GRID_REGISTRY = {
    # ---------------------------------------------------------
    # EEA Reference Grid (EPSG:3035) - Metric
    # ---------------------------------------------------------
    "EEA_100m": {"crs": "EPSG:3035", "resolution": 100, "bounds": (2000000, 1000000, 6000000, 5500000)},
    "EEA_250m": {"crs": "EPSG:3035", "resolution": 250, "bounds": (2000000, 1000000, 6000000, 5500000)},
    "EEA_500m": {"crs": "EPSG:3035", "resolution": 500, "bounds": (2000000, 1000000, 6000000, 5500000)},
    "EEA_1km":  {"crs": "EPSG:3035", "resolution": 1000, "bounds": (2000000, 1000000, 6000000, 5500000)},
    "EEA_10km": {"crs": "EPSG:3035", "resolution": 10000, "bounds": (2000000, 1000000, 6000000, 5500000)},

    # ---------------------------------------------------------
    # Global Equal Area (EPSG:6933) - Metric
    # ---------------------------------------------------------
    "Global_EqualArea_100m": {"crs": "EPSG:6933", "resolution": 100, "bounds": (-17367530, -7314540, 17367530, 7314540)},
    "Global_EqualArea_250m": {"crs": "EPSG:6933", "resolution": 250, "bounds": (-17367530, -7314540, 17367530, 7314540)},
    "Global_EqualArea_500m": {"crs": "EPSG:6933", "resolution": 500, "bounds": (-17367530, -7314540, 17367530, 7314540)},
    "Global_EqualArea_1km":  {"crs": "EPSG:6933", "resolution": 1000, "bounds": (-17367530, -7314540, 17367530, 7314540)},
    "Global_EqualArea_10km": {"crs": "EPSG:6933", "resolution": 10000, "bounds": (-17367530, -7314540, 17367530, 7314540)},

    # ---------------------------------------------------------
    # Global WGS84 (EPSG:4326) - Decimal Degrees
    # ---------------------------------------------------------
    # ~100m at the equator (3 arc-seconds)
    "Global_WGS84_3sec": {"crs": "EPSG:4326", "resolution": 0.0008333333333333333, "bounds": (-180.0, -90.0, 180.0, 90.0)},
    # ~250m at the equator (7.5 arc-seconds)
    "Global_WGS84_7_5sec": {"crs": "EPSG:4326", "resolution": 0.0020833333333333333, "bounds": (-180.0, -90.0, 180.0, 90.0)},
    # ~500m at the equator (15 arc-seconds)
    "Global_WGS84_15sec": {"crs": "EPSG:4326", "resolution": 0.004166666666666667, "bounds": (-180.0, -90.0, 180.0, 90.0)},
    # ~1km at the equator (30 arc-seconds)
    "Global_WGS84_30sec": {"crs": "EPSG:4326", "resolution": 0.008333333333333333, "bounds": (-180.0, -90.0, 180.0, 90.0)},
    # ~10km at the equator (5 arc-minutes)
    "Global_WGS84_5min": {"crs": "EPSG:4326", "resolution": 0.08333333333333333, "bounds": (-180.0, -90.0, 180.0, 90.0)}
}
    
    def __init__(self):
        pass

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
                
                # =================================================================
                # THREAD-SAFE TEMPORARY FILE GENERATION
                # =================================================================
                # Generates a completely unique UUID for this specific thread's operation
                unique_hash = uuid.uuid4().hex
                temp_file = f"temp_warp_input_{unique_hash}.tif"
                
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
                warpOptions=['NUM_THREADS=1'] 
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

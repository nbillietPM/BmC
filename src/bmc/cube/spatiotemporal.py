import xarray as xr
import numpy as np
from concurrent.futures import ThreadPoolExecutor
from functools import partial
from rasterio.warp import transform_bounds
from rasterio.transform import from_origin
from rasterio.enums import Resampling
import os
import glob
import rioxarray
from osgeo import gdal
import logging
from typing import Optional, Union, Dict, Any, List
from bmc.utils.logger import log_execution
import yaml
import sys
import os
import shutil


class spatiotemporal_cube():
    _GDAL_RESAMPLERS = {
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

    _RESAMPLER_DECODER = {
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

    def export_to_cog(
        self, 
        ds: Union[xr.DataArray, xr.Dataset], 
        output_filepath: str, 
        compress_mode: str = "deflate",
        logger: Optional[logging.Logger] = None
    ) -> str:
        """
        Exports an xarray object to disk as a Cloud Optimized GeoTIFF (COG).

        Parameters
        ----------
        ds : xarray.DataArray or xarray.Dataset
            The spatial data to be exported. If a Dataset is provided, it iterates 
            and saves each variable as a separate COG (or bands, depending on structure).
        output_filepath : str
            The target file path (must end in .tif).
        compress_mode : str, optional
            The compression algorithm. 'deflate' is highly recommended for SDM data.
        logger : logging.Logger, optional
            Logger for recording execution progress.

        Returns
        -------
        output_filepath : str
            The path to the successfully created COG.
        """
        log_execution(logger, f"Exporting to Cloud Optimized GeoTIFF: {output_filepath}", logging.INFO)
        
        # Ensure the directory exists
        os.makedirs(os.path.dirname(os.path.abspath(output_filepath)), exist_ok=True)

        try:
            # rioxarray has native support for the GDAL COG driver
            if isinstance(ds, xr.Dataset):
                # For datasets (like fractional coverages), we can write them out 
                # as multi-band COGs or iteratively. rioxarray handles Datasets by 
                # writing each data_var as a band if they share coordinates.
                ds.rio.to_raster(
                    output_filepath,
                    driver="COG",
                    compress=compress_mode,
                    tiled=True,
                    windowed=True # Crucial for keeping RAM usage low during write
                )
            else:
                ds.rio.to_raster(
                    output_filepath,
                    driver="COG",
                    compress=compress_mode,
                    tiled=True,
                    windowed=True
                )
            
            log_execution(logger, f"COG successfully generated.", logging.INFO)
            return output_filepath

        except Exception as e:
            log_execution(logger, f"Failed to export COG: {e}", logging.ERROR, exc_info=True)
            raise

    def generate_cube_recipe(self, config_path: str, logger: Optional[logging.Logger] = None) -> Dict[str, Any]:
        """
        Parses a YAML configuration file and generates a standardized execution recipe.

        This base-class method handles universal data cube setup tasks. It reads the 
        user's YAML file, creates the master output directory structures, and dynamically 
        resolves the spatial mathematical grid. By centralizing this logic, all child 
        data cubes (e.g., WEkEO, Earth Engine, GBIF) share the exact same spatial and 
        file-system foundations. It explicitly separates the core engine parameters 
        from the diverse data payload APIs nested under the 'sources' block.

        Parameters
        ----------
        config_path : str
            The absolute or relative file path to the user's YAML configuration file.
        logger : logging.Logger, optional
            The logger instance to use for recording execution messages. Default is None.

        Returns
        -------
        recipe : dict
            A highly structured dictionary containing the parsed configuration, derived 
            operational paths, safely resolved spatial parameters, and isolated data 
            source configurations.
            
        Raises
        ------
        FileNotFoundError
            If the specified YAML config path does not exist on the file system.
        ValueError
            If the YAML is malformed, or if the requested target grid and resolution 
            cannot be successfully resolved against the internal `GRID_REGISTRY`.

        See Also
        --------
        resolve_grid_registry_key : The internal method used to validate the spatial grid.
        """
        # Extract the base name immediately so we have a fallback name if things go wrong
        yaml_basename = os.path.splitext(os.path.basename(config_path))[0]

        if not logger:
            print(f"Attempting to load configuration from: {config_path}")
        else:
            log_execution(logger, f"Attempting to load configuration from: {config_path}", logging.INFO)

        # 1. Load the YAML Configuration with Emergency Crash Logging
        try:
            with open(config_path, 'r') as file:
                config = yaml.safe_load(file)
                
        except (FileNotFoundError, yaml.YAMLError) as exc:
            msg = f"FATAL ERROR: Failed to load configuration from '{config_path}': {exc}"
            
            if logger: 
                log_execution(logger, msg, logging.CRITICAL)
            else:
                # --- EMERGENCY CRASH LOGGER ---
                # Spin up a temporary logger in the current directory to permanently record the death
                crash_log_path = f"{yaml_basename}_CRASH.log"
                crash_logger = self._setup_pipeline_logger(logger_name=f"{yaml_basename}_crash", log_filepath=crash_log_path)
                log_execution(crash_logger, msg, logging.CRITICAL)
                print(f"Critical error securely logged to: {crash_log_path}")
                
            # Re-raise the exact error so the execution thread properly halts
            if isinstance(exc, yaml.YAMLError):
                raise ValueError(msg)
            else:
                raise FileNotFoundError(msg)

        # 2. Extract and Build Base Directory Structures
        cube_name = config.get("cube_name", "standard_export")
        base_out_dir = os.path.join(".", cube_name) 
        raw_dir = os.path.join(base_out_dir, "raw_downloads")
        
        try:
            os.makedirs(raw_dir, exist_ok=True)
            
            # --- INITIALIZE INCREMENTAL SUCCESS LOGGER ---
            if not logger:
                log_filepath = os.path.join(base_out_dir, f"{yaml_basename}.log")
                
                # Check if it exists, and increment a number until we find an empty slot
                counter = 1
                while os.path.exists(log_filepath):
                    log_filepath = os.path.join(base_out_dir, f"{yaml_basename}_{counter}.log")
                    counter += 1
                
                unique_logger_name = f"{cube_name}_{counter}" if counter > 1 else cube_name
                logger = self._setup_pipeline_logger(logger_name=unique_logger_name, log_filepath=log_filepath)
                self.wekeo_logger = logger 
                
            # --- RETROACTIVE SUCCESS LOGGING ---
            log_execution(logger, f"Successfully loaded configuration from: {config_path}", logging.INFO)
            log_execution(logger, f"Lake root directory initialized at: {base_out_dir}", logging.INFO)
            log_execution(logger, f"Session log file created at: {log_filepath}", logging.INFO)
            
        except Exception as e:
            msg = f"Failed to create lake root directory: {e}"
            if logger: log_execution(logger, msg, logging.ERROR, exc_info=True)
            else: print(msg)
            raise

        # 3. Resolve Spatial Grid via the Helper Function
        spatial_config = config.get('spatial', {})
        base_grid = spatial_config.get('target_grid', 'EEA')
        res = spatial_config.get('target_resolution', '100m')
        
        target_grid_key = self.resolve_grid_registry_key(base_grid, res, logger)
        log_execution(logger, f"Target spatial grid safely resolved to: {target_grid_key}", logging.INFO)

        # 4. Package the Standardized Recipe
        recipe = {
            "paths": {
                "base_dir": base_out_dir,
                "raw_dir": raw_dir
            },
            "spatial": {
                "target_grid_key": target_grid_key,
                "resampling_strategies": config.get('resampling', {}),
                "bbox": spatial_config.get('bbox')
            },
            "temporal": config.get('temporal', {}),
            "sources": config.get('sources', {}),
            "raw_config": config 
        }
        
        return recipe
    
    def gather_tifs_from_zips(
        self, 
        source_directory: str, 
        target_directory: str, 
        logger: Optional[logging.Logger] = None
    ) -> None:
        """
        Iterates through zip archives in a source directory and extracts 
        only the .tif files, flattening them into a single target directory.

        This method acts as a data preparation step, isolating raw spatial 
        arrays from nested archive structures and auxiliary metadata files 
        prior to Virtual Raster (VRT) mosaic construction.

        Parameters
        ----------
        source_directory : str
            The path to the directory containing the downloaded .zip archives.
        target_directory : str
            The destination directory where extracted .tif files will be saved.
            If the directory does not exist, it will be created.
        logger : logging.Logger, optional
            The logger instance for recording execution progress and errors. 
            Defaults to None.

        Returns
        -------
        None
        """
        import zipfile
        import shutil
        from pathlib import Path
        
        # 1. Set up the paths
        source_path = Path(source_directory)
        target_path = Path(target_directory)
        
        # Create the target directory if it doesn't exist yet
        target_path.mkdir(parents=True, exist_ok=True)

        # 2. Find all zip files in the source folder
        zip_files = list(source_path.glob("*.zip"))
        log_execution(logger, f"Found {len(zip_files)} zip files. Starting extraction...", logging.INFO)

        # 3. Iterate over each zip file
        for zip_file_path in zip_files:
            try:
                with zipfile.ZipFile(zip_file_path, 'r') as zip_ref:
                    
                    # Look at every file hidden inside the zip archive
                    for file_info in zip_ref.infolist():
                        
                        # Check if the file is a .tif
                        if file_info.filename.endswith('.tif'):
                            
                            # Extract the filename without any internal zip folder structure
                            tif_filename = Path(file_info.filename).name 
                            destination_file = target_path / tif_filename
                            
                            # 4. Copy the .tif file to the shared folder
                            with zip_ref.open(file_info) as source_file:
                                with open(destination_file, 'wb') as target_file:
                                    shutil.copyfileobj(source_file, target_file)
            except zipfile.BadZipFile:
                log_execution(logger, f"Warning: {zip_file_path.name} is corrupted or not a valid zip file.", logging.WARNING)
                continue
            except Exception as e:
                log_execution(logger, f"Error extracting from {zip_file_path.name}: {e}", logging.ERROR)

        log_execution(logger, "All .tif files have been gathered successfully!", logging.INFO)

    def cleanup_raw_storage(self, recipe: Dict, logger: Optional[logging.Logger] = None) -> None:
        """
        Safely purges the raw data directory based on the user configuration.

        This function checks the execution recipe for the `keep_raw` parameter.
        If `keep_raw` is explicitly set to False, it will aggressively remove 
        the raw downloads directory to free up disk space. It uses `ignore_errors=True` 
        during removal to prevent phantom file locks (e.g., [WinError 32] on Windows) 
        from crashing the pipeline. If `keep_raw` is True or missing, the raw 
        data is safely retained.

        Parameters
        ----------
        recipe : dict
            The parsed execution configuration dictionary. It is expected to contain 
            the `keep_raw` boolean under `recipe['raw_config']['keep_raw']` and the 
            target directory path under `recipe['paths']['raw_dir']`.
        logger : logging.Logger, optional
            The logger instance used to record the execution and cleanup status. 
            Default is None.

        Returns
        -------
        None
            This function does not return a value.
        """
        keep_raw = recipe.get('raw_config', {}).get('keep_raw', True)
        raw_dir = recipe.get('paths', {}).get('raw_dir')

        if not keep_raw and raw_dir and os.path.exists(raw_dir):
            if logger:
                logger.info(f"keep_raw is False. Purging raw data directory: {raw_dir}")
            try:
                shutil.rmtree(raw_dir, ignore_errors=True)
                if logger:
                    logger.info("Raw data successfully deleted.")
            except Exception as e:
                if logger:
                    logger.warning(f"Could not completely delete raw directory: {e}")
        elif keep_raw:
            if logger:
                logger.info("keep_raw is True. Retaining raw downloaded data.")

    def da_layer_constructor(self, data_layer_func, param):
        """
        General layer constructor that can take any function from the layers submodule and fetch all slices for the layer
        based on the parameters defined by the param dict

        returns (var_name, data_array)
        """
        static_param = list(param.values())[1:]
        data_arrays = []
        for var in param["var"]:
            data_arrays.append(data_layer_func(var, *static_param))
        return data_arrays
    
    def da_layer_constructor_concurrent(self, layer_func, param, max_workers=4):
        """
        Concurrent layer constructor.
        
        CRITICAL: This relies on 'param' dictionary keys being in the EXACT order 
        expected by 'layer_func'.
        """
        
        # 1. Extract Static Parameters
        # Slice [1:] to skip 'var' and keep the rest (bbox, year_ranges, etc.)
        # strict order is preserved here.
        static_param = list(param.values())[1:]
        
        # 2. Build Task Arguments
        # Create a list of tuples: [(var1, bbox, list_of_years...), (var2, bbox, list_of_years...)]
        # The 'var' is inserted as the FIRST argument.
        task_arguments = [ (var, *static_param) for var in param["var"] ]

        # 3. Define Worker
        def _worker(args):
            # Unpack the tuple into positional arguments
            return layer_func(*args)

        # 4. Execute
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            # map guarantees results are returned in the same order as 'param["var"]'
            results = list(executor.map(_worker, task_arguments))
            
        return results

    def da_concat(self, data_arrays, dim_name, coordinates):
        """
        Combines a stack of data layers into one large data layer where stacking results in a new dimension
        being created with the name dim_name with associated values coordinates
        """
        combined_data_array = xr.concat(data_arrays, dim=dim_name)
        combined_data_array = combined_data_array.assign_coords({dim_name: coordinates})
        return combined_data_array
        
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

    def build_virtual_mosaic(
        self,
        input_folder: str, 
        output_vrt_path: str, 
        logger: Optional[logging.Logger] = None
    ) -> Optional[str]:
        """
        Creates a lightweight Virtual Raster (VRT) blueprint from multiple GeoTIFF tiles.

        This method discovers all `.tif` files in a folder and creates an XML-based `.vrt` 
        file, mosaicking them together at their native resolution.

        Parameters
        ----------
        input_folder : str
            The directory containing the raw `.tif` tiles.
        output_vrt_path : str
            The destination file path for the blueprint (must end in `.vrt`).
        logger : logging.Logger, optional
            The logger instance to use for recording execution messages. Default is ``None``.

        Returns
        -------
        str or None
            The file path to the generated `.vrt` file. Returns ``None`` if no tiles were found.
        """
        # Enable errors
        gdal.UseExceptions()
        
        tif_files = glob.glob(f"{input_folder}/*.tif")
        if not tif_files:
            log_execution(logger, f"No .tif files found in '{input_folder}'.", logging.WARNING)
            return None

        log_execution(logger, f"Found {len(tif_files)} files. Building VRT blueprint...", logging.INFO)

        try:
            os.makedirs(os.path.dirname(os.path.abspath(output_vrt_path)), exist_ok=True)

            # Build the VRT without any resampling options
            vrt = gdal.BuildVRT(output_vrt_path, tif_files)
            
            # Critical: Flush cache and destroy the python object to force GDAL to write the XML to disk
            vrt.FlushCache()
            vrt = None 

            log_execution(logger, f"Virtual mosaic successfully saved to: {output_vrt_path}", logging.INFO)
            return output_vrt_path

        except Exception as e:
            log_execution(logger, f"Error building virtual mosaic: {e}", logging.ERROR, exc_info=True)
            raise

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
        resampler = self._GDAL_RESAMPLERS.get(resample_keyword, gdal.GRA_Bilinear)
        resampler_name = self._RESAMPLER_DECODER.get(resampler, "unknown")
        log_execution(logger, f"Utilizing '{resampler_name}' resampling for reprojection.", logging.INFO)

        temp_file = None
        
        try:
            os.makedirs(os.path.dirname(os.path.abspath(output_filepath)), exist_ok=True)

            # Handle the Input (xarray vs file path)
            if isinstance(input_data, (xr.DataArray, xr.Dataset)):
                log_execution(logger, "Lazy xarray object detected. Preparing for disk stream...", logging.INFO)
                input_data = self._sanitize_spatial_geometry(input_data, logger=logger)
                
                temp_file = "temp_warp_input.tif"
                input_data.rio.to_raster(temp_file, tiled=True, compress=compress_mode, windowed=True)
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
                creationOptions=[f'COMPRESS={compress_mode.upper()}', 'TILED=YES'],
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

    def calculate_fractional_coverages(
        self,
        ds: Union[xr.DataArray, xr.Dataset, str], 
        grid_name: str, 
        output_dir: str, 
        class_values: Optional[list[int]] = None,
        class_mapping: Optional[dict] = None,  # <--- NEW: Dictionary mapping ints to names
        file_prefix: str = "fractional",
        logger: Optional[logging.Logger] = None
    ) -> List[str]:
        """
        Calculates the fractional coverage of categorical classes snapped to a target grid,
        exporting each specific class as an independent Single-Band Cloud Optimized GeoTIFF.
        """
        import os
        import numpy as np
        import xarray as xr
        import rioxarray
        from pathlib import Path
        import gc

        os.makedirs(output_dir, exist_ok=True)
        
        if isinstance(ds, (str, Path)):
            log_execution(logger, f"Lazy-loading categorical raster from path: {ds}", logging.INFO)
            ds = rioxarray.open_rasterio(ds, chunks=True) 
        
        if isinstance(ds, xr.Dataset):
            var_name = list(ds.data_vars)[0]
            da = ds[var_name]
        else:
            da = ds
            
        nodata_val = da.rio.nodata

        if class_values is None:
            log_execution(logger, "Finding unique classes...", logging.INFO)
            unique_vals = da.data.map_blocks(np.unique).compute()
            unique_vals = np.unique(unique_vals) 
            class_values = [int(v) for v in unique_vals if v not in [nodata_val, np.nan]]
            
        log_execution(logger, f"Calculating single-band fractions for {len(class_values)} classes...", logging.INFO)

        generated_cogs = []

        for cls in class_values:
            cls_int = int(cls)
            
            if class_mapping and cls_int in class_mapping:
                cls_name = str(class_mapping[cls_int]).replace(" ", "_").replace("/", "_").replace("-", "_")
            else:
                cls_name = f"class_{cls_int}"
                
            log_execution(logger, f"\n--- Processing Class: {cls_int} ({cls_name}) ---", logging.INFO)
            
            # 1. Create the Lazy Mask
            mask = (da == cls).astype(np.float32)

            if nodata_val is not None:
                mask = mask.where(da != nodata_val, np.nan)
            
            mask.rio.write_crs(da.rio.crs, inplace=True)
            mask.rio.write_transform(da.rio.transform(), inplace=True)
            mask.rio.write_nodata(np.nan, inplace=True)
            
            # 2. The Disk Handoff
            raw_mask_path = os.path.join(output_dir, f"temp_raw_mask_{cls_int}.tif")
            temp_warp_path = os.path.join(output_dir, f"temp_warp_{cls_int}.tif") 
            
            log_execution(logger, "Streaming binary mask to disk...", logging.INFO)
            mask.rio.to_raster(raw_mask_path, tiled=True, windowed=True)
            
            # 3. GDAL Warp to temporary file
            log_execution(logger, "Executing GDAL C++ Warp...", logging.INFO)
            
            # --- FIX 1: Save the raw dataset to a dedicated variable ---
            raw_frac_da = self.affine_reproject(
                input_data=raw_mask_path, 
                output_filepath=temp_warp_path, 
                grid_name=grid_name, 
                resample_keyword="average",
                logger=logger
            )
            
            # Clean up the output structure into a new view
            frac_da = raw_frac_da.squeeze().drop_vars("band", errors="ignore")
            frac_da.name = f"fraction_{cls_name}"
            
            # 4. Bake the final Cloud Optimized GeoTIFF 
            final_cog_name = f"{file_prefix}_{cls_name}.tif"
            final_cog_path = os.path.join(output_dir, final_cog_name)
            
            self.export_to_cog(frac_da, final_cog_path, logger=logger)
            generated_cogs.append(final_cog_path)
            
            # 5. Clean up RAM and Disk Locks
            raw_frac_da.close() # --- FIX 2: Explicitly close the ORIGINAL file handle ---
            frac_da.close()     
            del raw_frac_da
            del frac_da
            
            # Force the garbage collector to run BEFORE we try to delete files
            gc.collect()
            
            # --- FIX 3: Defensive File Cleanup ---
            if os.path.exists(raw_mask_path):
                try:
                    os.remove(raw_mask_path)
                except Exception as e:
                    log_execution(logger, f"Warning: Could not delete temp mask {raw_mask_path}: {e}", logging.WARNING)
                    
            if os.path.exists(temp_warp_path):
                try:
                    os.remove(temp_warp_path) 
                except Exception as e:
                    log_execution(logger, f"Warning: Could not delete temp warp {temp_warp_path}: {e}", logging.WARNING)

        log_execution(logger, f"All {len(generated_cogs)} single-band fractional COGs baked into '{output_dir}'.", logging.INFO)
        return generated_cogs
    
    def process_virtual_mosaic(
        self, 
        vrt_path: str, 
        strategy: str, 
        grid_name: str,
        output_dir_or_file: str,
        logger: Optional[logging.Logger] = None,
        **kwargs
    ) -> Union[xr.DataArray, xr.Dataset]:
        """
        Routes a Virtual Raster (VRT) blueprint to the appropriate spatial processing algorithm.

        This dispatcher function delays heavy pixel-crunching until the exact mathematical 
        strategy is determined. It handles the I/O handoff, routing the lightweight XML 
        blueprint to either a standard GDAL affine reprojection or a categorical 
        fractional coverage calculator.

        Parameters
        ----------
        vrt_path : str
            The file path to the source `.vrt` file.
        strategy : str
            The processing algorithm to apply. Valid options: 'reproject', 'coverage'.
        grid_name : str
            The key of the target grid defined in the class grid registry.
        output_dir_or_file : str
            If strategy is 'reproject', this should be the output file path (.tif).
            If strategy is 'coverage', this should be the output directory.
        logger : logging.Logger, optional
            The logger instance to use for recording execution messages. Default is ``None``.
        **kwargs : dict
            Keyword arguments passed directly to the chosen processing function 
            (e.g., class_values, resample_keyword, compress_mode).

        Returns
        -------
        xarray.DataArray or xarray.Dataset
            The lazily loaded result of the processing step. Returns a single DataArray 
            for standard reprojections, or a multi-variable Dataset for fractional coverages.

        Raises
        ------
        FileNotFoundError
            If the specified VRT file does not exist on disk.
        ValueError
            If an invalid strategy string is provided.
        Exception
            If the underlying processing pipeline encounters a failure.

        See Also
        --------
        affine_reproject : The underlying method utilized when the 'reproject' strategy is selected.
        calculate_fractional_coverages : The underlying method utilized when the 'coverage' strategy is selected.

        Examples
        --------
        Case 1: Standard Reprojection
        Passing a VRT blueprint to be reprojected into a single GeoTIFF. In this case, 
        ``output_dir_or_file`` must be a file path. We utilize all available optional 
        arguments for the underlying ``affine_reproject`` method via ``**kwargs``.

        >>> output_array = cube.process_virtual_mosaic(
        ...     vrt_path="temp/elevation_blueprint.vrt",
        ...     strategy="reproject",
        ...     grid_name="EEA_1km",
        ...     output_dir_or_file="outputs/reprojected_elevation.tif",
        ...     logger=my_logger,
        ...     resample_keyword="bilinear",
        ...     compress_mode="deflate",
        ...     memory_limit_bytes=8192
        ... )
        >>> type(output_array)
        <class 'xarray.core.dataarray.DataArray'>

        Case 2: Fractional Coverage Calculation
        Passing a categorical VRT blueprint to compute fractional coverages for specific 
        classes. In this case, ``output_dir_or_file`` must be a directory path. We 
        utilize all available optional arguments for the underlying 
        ``calculate_fractional_coverages`` method via ``**kwargs``.

        >>> output_dataset = cube.process_virtual_mosaic(
        ...     vrt_path="temp/landcover_blueprint.vrt",
        ...     strategy="coverage",
        ...     grid_name="EEA_1km",
        ...     output_dir_or_file="outputs/fractional_layers/",
        ...     logger=my_logger,
        ...     class_values=[11, 12, 41, 42]
        ... )
        >>> type(output_dataset)
        <class 'xarray.core.dataset.Dataset'>
        """
        if not os.path.exists(vrt_path):
            raise FileNotFoundError(f"VRT blueprint not found at: {vrt_path}")

        log_execution(logger, f"Initializing '{strategy}' processing pipeline for VRT...", logging.INFO)

        try:
            if strategy.lower() == 'reproject':
                return self.affine_reproject(
                    input_data=vrt_path, 
                    output_filepath=output_dir_or_file, 
                    grid_name=grid_name, 
                    logger=logger, 
                    **kwargs
                )
                
            elif strategy.lower() == 'coverage':
                return self.calculate_fractional_coverages(
                    ds=vrt_path, 
                    grid_name=grid_name, 
                    output_dir=output_dir_or_file, 
                    logger=logger, 
                    **kwargs
                )
                
            else:
                raise ValueError(f"Unknown processing strategy: '{strategy}'. Must be 'reproject' or 'coverage'.")

        except Exception as e:
            log_execution(logger, f"Pipeline failure during {strategy}: {e}", logging.ERROR, exc_info=True)
            raise
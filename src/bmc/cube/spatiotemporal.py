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
from typing import Optional, Union, Any
from bmc.utils.logger import log_execution

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

        src.utils.logger.log_execution
        
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
        ds: Union[xr.DataArray, xr.Dataset], 
        grid_name: str, 
        output_dir: str, 
        class_values: Optional[list[int]] = None,
        logger: Optional[logging.Logger] = None
    ) -> xr.Dataset:
        """
        Calculates the fractional coverage of categorical classes snapped to a target grid.

        Because categorical data (like land cover) cannot be accurately resampled directly 
        using continuous algorithms, this method utilizes a memory-efficient binary masking 
        technique. It iterates through each unique categorical class, isolates it as a binary 
        presence/absence mask at the native resolution, and streams that mask through an 
        out-of-core spatial warp using an 'average' resampling algorithm. This translates 
        discrete pixels into mathematically precise fractional percentages per target pixel.

        Parameters
        ----------
        ds : xarray.DataArray or xarray.Dataset
            The high-resolution categorical input data (e.g., a lazy loaded VRT or physical file).
            If a Dataset is provided, the first data variable is automatically extracted.
        grid_name : str
            The key of the target grid defined in the class `GRID_REGISTRY` 
            (e.g., ``"EEA_1km"``) to which the fractional maps will be perfectly snapped.
        output_dir : str
            The directory where the individual fractional `.tif` files will be saved. 
            The folder is created automatically if it does not exist.
        class_values : list of int, optional
            A specific list of integer classes to process. If ``None``, the function 
            will pull the array into memory to automatically discover unique values. 
            For massive datasets or lazy VRTs, providing this list manually is highly 
            recommended to prevent RAM spikes. Default is ``None``.
        logger : logging.Logger, optional
            The logger instance to use for recording execution messages. Default is ``None``.

        Returns
        -------
        xarray.Dataset
            A unified Dataset containing lazy references to all generated fractional 
            coverage layers. Each data variable is cleanly formatted and named as 
            `fraction_class_{N}`.

        See Also
        --------
        affine_reproject : The underlying method handling the out-of-core warping for each mask.

        Notes
        -----
        The intermediate masks are created using `float32` precision to guarantee that the 
        averaging algorithm calculates fractions accurately. `nodata` values in the source 
        array are rigorously protected and passed through to the binary masks as `np.nan` 
        to ensure they do not incorrectly skew the output percentages.

        Examples
        --------
        Calculate fractional coverage for all automatically discovered classes:

        >>> frac_ds = cube.calculate_fractional_coverages(
        ...     ds=landcover_array,
        ...     grid_name="Global_Equal_Area_500m",
        ...     output_dir="./output/fractions/"
        ... )
        >>> print(frac_ds.data_vars)
        Data variables:
            fraction_class_1  (y, x) float32 ...
            fraction_class_2  (y, x) float32 ...

        Calculate fractional coverage for specific classes to save memory and time:

        >>> target_classes = [11, 12, 41]  # E.g., Forest types
        >>> frac_ds = cube.calculate_fractional_coverages(
        ...     ds=landcover_array,
        ...     grid_name="EEA_1km",
        ...     output_dir="./output/fractions/",
        ...     class_values=target_classes,
        ...     logger=my_logger
        ... )
        """
        # Ensure the output directory exists before we start generating files
        os.makedirs(output_dir, exist_ok=True)
        
        if isinstance(ds, xr.Dataset):
            var_name = list(ds.data_vars)[0]
            da = ds[var_name]
        else:
            da = ds
            
        nodata_val = da.rio.nodata

        if class_values is None:
            log_execution(logger, "Finding unique classes...", logging.INFO)
            # For a massive file, passing the class list manually is safer!
            # da.data accesses the underlying Dask array instead of forcing a NumPy array
            unique_vals = da.data.map_blocks(np.unique).compute()
            unique_vals = np.unique(unique_vals) # Run unique one more time on the aggregated results
            
        log_execution(logger, f"Calculating fractional coverage for {len(class_values)} classes...", logging.INFO)

        fractional_layers = {}

        for cls in class_values:
            log_execution(logger, f"\n--- Processing Class: {cls} ---", logging.INFO)
            
            # The Memory-Safe Magic Trick
            mask = (da == cls).astype(np.float32)

            if nodata_val is not None:
                mask = mask.where(da != nodata_val, np.nan)
            
            mask.rio.write_crs(da.rio.crs, inplace=True)
            mask.rio.write_transform(da.rio.transform(), inplace=True)
            mask.rio.write_nodata(np.nan, inplace=True)
            
            # Define the targeted output path for the out-of-core warp
            file_name = f"fractional_class_{int(cls)}_{grid_name}.tif"
            temp_out_path = os.path.join(output_dir, file_name)
            
            # Call the GDAL out-of-core reprojector via self
            frac_da = self.affine_reproject(
                input_data=mask, 
                output_filepath=temp_out_path, 
                grid_name=grid_name, 
                resample_keyword="average",
                logger=logger
            )
            
            # Clean up the Dataset structure
            # rioxarray loads tifs with a 'band' dimension (shape: 1, y, x). 
            # We drop the band dimension to make it a clean 2D layer (y, x).
            frac_da = frac_da.squeeze().drop_vars("band", errors="ignore")
            frac_da.name = f"fraction_class_{int(cls)}"
            
            fractional_layers[frac_da.name] = frac_da

        # Combine the lazy file references into one giant Dataset
        final_ds = xr.Dataset(fractional_layers)

        log_execution(logger, f"All fractional coverages saved to '{output_dir}' and packaged.", logging.INFO)
        
        return final_ds

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
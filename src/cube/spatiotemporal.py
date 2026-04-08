import xarray as xr
import numpy as np
from concurrent.futures import ThreadPoolExecutor
from functools import partial
from rasterio.warp import transform_bounds
from rasterio.transform import from_origin
from rasterio.enums import Resampling
import logging

class spatiotemporal_cube():
    def __init__(self):
        self.grid_registry = {
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
        
    def regrid_spatial_coordinates(self, data_array, target_lat, target_long,
                                   harmonization="upscale", method="linear", fill_nearest=True):
        """
        Rudimentary regrid function that (currently) only upscales a given data array to a target grid defined by the
        target_lat and target_long values. Fills in the borders that are NaN due to linear interpolation which requires
        neighbours from all sides 
        """
        if harmonization=="upscale":
            upscaled_data_array = data_array.interp(lat=target_lat, long=target_long, method=method)
            if fill_nearest:
                upscaled_filled_data_array = upscaled_data_array.ffill(dim="lat").bfill(dim="lat").ffill(dim="long").bfill(dim="long")
                return upscaled_filled_data_array
            else:
                return upscaled_data_array

    def _sanitize_spatial_geometry(self, ds, default_crs="EPSG:4326"):
        """
        Validates and sanitizes xarray spatial metadata for GDAL/rioxarray compatibility.
        Seamlessly accepts BOTH xarray.Dataset and xarray.DataArray.
        """
        # Standardize Horizontal/Vertical Axes
        dim_map = {}
        for dim in ds.dims:
            dim_str = str(dim).lower()
            # Convert potential lat/lon names to standard x/y
            if dim_str in ['lon', 'longitude', 'long']:
                dim_map[dim] = 'x'
            elif dim_str in ['lat', 'latitude']:
                dim_map[dim] = 'y'
                
        if dim_map:
            ds = ds.rename(dim_map)
        
        ds = ds.rio.set_spatial_dims(x_dim="x", y_dim="y")

        # Enforce Coordinate Reference System (CRS)
        # Check this BEFORE doing any other metadata manipulation!
        if ds.rio.crs is None:
            print(f"Warning: No CRS found. Assuming {default_crs}.")
            ds = ds.rio.write_crs(default_crs)

        # Clean Dimension Encoding
        # Only clear encoding on spatial dimensions to avoid breaking time/band metadata
        # Stores information on how data should be stored, clearing this removes artifacts
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

    def affine_reproject(self, input_data, output_filepath, grid_name, resample_keyword="bilinear",
                         compress_mode="lzw",
                         memory_limit_bytes=4096):
        """
        A universally robust version of affine_reproject. 
        Accepts either a physical file path OR an xarray DataArray.
        Uses GDAL Warp under the hood to ensure windowed, RAM-safe processing.

        compress_mode describes the intermediare file to which to which loaded data is written to.
            - standard mode is lzw, this comes at CPU cost
            - None disables compression but eats up hard drive space, only enable when sufficient space is assured

        memory_limit_bytes controls the amount of RAM memory gdal can utilize during the warping of the raster
            - standard options is 4GB of RAM which can be increased in case of stronger hardware
        """
        print(f"Preparing out-of-core reprojection to {grid_name}...")
        
        # 1. Fetch Master Grid specs
        spec = self.grid_registry[grid_name]
        target_crs = spec["crs"]
        res = spec["resolution"]
        master_minx, master_miny, _, _ = spec["bounds"]

        # 2. Map string keywords to GDAL resampling algorithms
        gdal_resamplers = {
            "nearest": gdal.GRA_NearestNeighbour,
            "bilinear": gdal.GRA_Bilinear,
            "cubic": gdal.GRA_Cubic,
            "average": gdal.GRA_Average,
            "mode": gdal.GRA_Mode,
            "max": gdal.GRA_Max,
            "min": gdal.GRA_Min,
        }
        resampler = gdal_resamplers.get(resample_keyword, gdal.GRA_NearestNeighbour)

        # 3. Handle the Input (xarray vs file path)
        temp_file = None
        if isinstance(input_data, (xr.DataArray, xr.Dataset)):
            print("Lazy xarray object detected. Streaming to temporary disk file...")
            temp_file = "temp_warp_input.tif"
            
            # 'windowed=True' safely streams the Dask chunks to disk without RAM spikes
            input_data.rio.to_raster(temp_file, tiled=True, compress=compress_mode, windowed=True)
            source_path = temp_file
            
            src_crs = input_data.rio.crs
            src_minx, src_miny, src_maxx, src_maxy = input_data.rio.bounds()
            src_nodata = input_data.rio.nodata
        else:
            # It's already a physical file
            source_path = input_data
            with rioxarray.open_rasterio(source_path) as info:
                src_crs = info.rio.crs
                src_minx, src_miny, src_maxx, src_maxy = info.rio.bounds()
                src_nodata = info.rio.nodata

        # 4. Your Grid Snapping Math
        dst_minx, dst_miny, dst_maxx, dst_maxy = transform_bounds(
            src_crs, target_crs, src_minx, src_miny, src_maxx, src_maxy
        )

        snap_minx = master_minx + np.floor((dst_minx - master_minx) / res) * res
        snap_maxx = master_minx + np.ceil((dst_maxx - master_minx) / res) * res
        snap_maxy = master_miny + np.ceil((dst_maxy - master_miny) / res) * res
        snap_miny = master_miny + np.floor((dst_miny - master_miny) / res) * res
        output_bounds = (snap_minx, snap_miny, snap_maxx, snap_maxy)

        # 5. The Windowed GDAL Warp
        print(f"Warping to {output_filepath} (Resampling: {resample_keyword})...")
        
        # If the input nodata is nan, we tell GDAL to use -9999 as a safe fallback
        nodata_val = -9999.0 if (src_nodata is None or np.isnan(src_nodata)) else src_nodata

        warp_options = gdal.WarpOptions(
            format='GTiff',
            dstSRS=target_crs,
            xRes=res,
            yRes=res,
            outputBounds=output_bounds,
            resampleAlg=resampler,
            srcNodata=nodata_val,
            dstNodata=nodata_val,
            creationOptions=['COMPRESS=LZW', 'TILED=YES'],
            warpMemoryLimit=memory_limit_bytes,
            # Allow GDAL to use all CPU cores to counter the slowdown from disk reads!
            warpOptions=['NUM_THREADS=ALL_CPUS'] 
        )
        
        gdal.Warp(output_filepath, source_path, options=warp_options)

        # 6. Clean up temp files
        if temp_file and os.path.exists(temp_file):
            os.remove(temp_file)
            
        print("Reprojection complete.")
    
        # Return the newly warped file as a lazy, chunked xarray dataset!
        return rioxarray.open_rasterio(output_filepath, chunks={'x': 2048, 'y': 2048})
    
    def calculate_fractional_coverages(self, ds, grid_name, class_values=None):
        """
        Iterates over discrete classes, creates a temporary binary mask for each,
        and uses affine_reproject to calculate fractional coverage.

        Returns a single xarray.Dataset where each variable is a class fraction.
        """
        # 1. Handle Dataset vs DataArray
        if isinstance(ds, xr.Dataset):
            # Assuming the first data variable contains your land cover data
            var_name = list(ds.data_vars)[0]
            da = ds[var_name]
        else:
            da = ds
            
        nodata_val = da.rio.nodata

        # 2. Determine which classes to process
        if class_values is None:
            print("Finding unique classes in memory...")
            unique_vals = np.unique(da.values)
            # Filter out the NoData value and any random NaNs
            class_values = [v for v in unique_vals if v != nodata_val and not np.isnan(v)]
            
        print(f"Calculating fractional coverage for {len(class_values)} classes...")

        fractional_layers = {}

        # 3. Process each class serially
        for cls in class_values:
            print(f"Warping fractional mask for class: {cls}...")
            
            # -- THE MAGIC TRICK --
            # Create the binary mask (1.0 for target class, 0.0 for others)
            # We must cast to float32 so the average resampler can generate decimals
            mask = xr.where(da == cls, 1.0, 0.0).astype(np.float32)

            # Re-apply the NoData mask. For float arrays in xarray, np.nan is standard
            if nodata_val is not None:
                mask = xr.where(da == nodata_val, np.nan, mask)
            
            # Note: xr.where() strips rioxarray spatial metadata, so we must put it back
            mask.rio.write_crs(da.rio.crs, inplace=True)
            mask.rio.write_transform(da.rio.transform(), inplace=True)
            mask.rio.write_nodata(np.nan, inplace=True)
            
            # 4. Pass the mask to your existing affine_reproject method
            frac_da = self.affine_reproject(mask, grid_name, resample_keyword="average")
            
            # Rename the DataArray for clarity in the final Dataset
            frac_da.name = f"fraction_class_{int(cls)}"
            
            # Store it in our dictionary
            fractional_layers[frac_da.name] = frac_da

        # 5. Combine all the separate DataArrays into one multi-variable Dataset
        final_ds = xr.Dataset(fractional_layers)

        print("Done! All fractional coverages computed.")
        return final_ds

    """
    def affine_reproject(self, ds, grid_name, resample_keyword="bilinear"):
        spec = self.grid_registry[grid_name]
        target_crs = spec["crs"]
        res = spec["resolution"]
        master_minx, master_miny, _, _ = spec["bounds"]

        # 1 & 2. Get bounds and project them to the target CRS
        # Extract the coordinates of the bounds of the dataset
        src_minx, src_miny, src_maxx, src_maxy = ds.rio.bounds()
        # Calculate the destination that the bounds will have in the target CRS system
        dst_minx, dst_miny, dst_maxx, dst_maxy = transform_bounds(
            ds.rio.crs, target_crs, src_minx, src_miny, src_maxx, src_maxy
        )

        # 3. Snap to the Master Grid lines
        snap_minx = master_minx + np.floor((dst_minx - master_minx) / res) * res
        snap_maxx = master_minx + np.ceil((dst_maxx - master_minx) / res) * res
        snap_maxy = master_miny + np.ceil((dst_maxy - master_miny) / res) * res
        snap_miny = master_miny + np.floor((dst_miny - master_miny) / res) * res

        # 4. Calculate final parameters
        width = int(round((snap_maxx - snap_minx) / res))
        height = int(round((snap_maxy - snap_miny) / res))
        
        # from_origin requires (Left, Top, X_resolution, Y_resolution)
        transform = from_origin(snap_minx, snap_maxy, res, res)

        params = {"shape": (height, width), "transform": transform, "crs": target_crs}

        RESAMPLING_METHODS = {"nearest": Resampling.nearest,
                            "bilinear": Resampling.bilinear,
                            "cubic": Resampling.cubic,
                            "average": Resampling.average,
                            "mode": Resampling.mode,
                            "sum": Resampling.sum,
                            "max": Resampling.max,
                            "min": Resampling.min,}
        
        resampler = RESAMPLING_METHODS.get(resample_keyword, Resampling.nearest)
        print(resample_keyword)
        result = ds.rio.reproject(params["crs"], 
                                shape=params["shape"],
                                transform=params["transform"],
                                resampling=resampler)
        return result

    def fractional_resample(self, da, target_resolution, classes=None):
        
        # 1. Identify the unique land cover classes
        if classes is None:
            classes = np.unique(da.values)
            
            # Remove the NoData value from our list of classes if it exists
            if da.rio.nodata is not None:
                classes = classes[classes != da.rio.nodata]
                
        # 2. One-Hot Encoding: Create a boolean mask for each class
        # This stacks them along a new dimension called 'class_val'
        masks = [da == c for c in classes]
        one_hot = xr.concat(masks, dim=xr.DataArray(classes, name='class_val'))
        
        # Convert booleans to floats so rasterio can compute a continuous average
        one_hot = one_hot.astype(float)
        
        # Inherit the CRS from the original dataset
        one_hot.rio.write_crs(da.rio.crs, inplace=True)
        one_hot.rio.write_nodata(np.nan, inplace=True)

        # 3. Calculate new grid dimensions
        minx, miny, maxx, maxy = da.rio.bounds()
        width = int(round((maxx - minx) / target_resolution))
        height = int(round((maxy - miny) / target_resolution))
        
        transform = from_origin(minx, maxy, target_resolution, target_resolution)

        print(f"Aggregating {len(classes)} classes to {target_resolution}m fractional cover...")

        # 4. Resample using AVERAGE to compute the fractions
        fractional_ds = one_hot.rio.reproject(
            da.rio.crs,
            shape=(height, width),
            transform=transform,
            resampling=Resampling.average
        )
        
        return fractional_ds
    """

import xarray as xr
import numpy as np
from concurrent.futures import ThreadPoolExecutor
from functools import partial
from rasterio.warp import transform_bounds
from rasterio.transform import from_origin
from rasterio.enums import Resampling

class spatiotemporal_cube():
    def __init__(self):
        self.grid_registry = {"EEA_1km": {"crs": "EPSG:3035",
                                          "resolution": 1000, 
                                          "bounds": (2000000, 1000000, 6000000, 5500000)},
                              "Global_EqualArea_1km": {"crs": "EPSG:6933",
                                                       "resolution": 1000,
                                                       "bounds": (-17367530, -7314540, 17367530, 7314540)},
                              "Global_WGS84_30sec": {"crs": "EPSG:4326",
                                                     # ~1km at the equator (30 arc-seconds)
                                                     "resolution": 0.008333333333333333,
                                                     "bounds": (-180.0, -90.0, 180.0, 90.0)}}

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

    def prep_for_reprojection(ds, default_crs="EPSG:4326"):
        """
        Validates and sanitizes an xarray Dataset/DataArray to ensure it 
        meets all strict rioxarray/GDAL criteria for reprojection.
        """
        # Work on a copy to avoid mutating the original dataset unintentionally
        ds = ds.copy()

        # Clean Metadata & Standardize Spatial Dimensions
        # Drop confusing metadata from other libraries
        if "spatial_ref" in ds.variables:
            ds = ds.drop_vars("spatial_ref")
        ds.encoding.clear()
        
        # Auto-detect and rename horizontal/vertical axes
        dim_map = {}
        for dim in ds.dims:
            dim_str = str(dim).lower()
            if dim_str in ['lon', 'longitude', 'long']:
                dim_map[dim] = 'x'
            elif dim_str in ['lat', 'latitude']:
                dim_map[dim] = 'y'
        
        # If the detector encountered non standard spatial dimension names rename to x and y
        if dim_map:
            ds = ds.rename(dim_map)
            
        ds = ds.rio.set_spatial_dims(x_dim="x", y_dim="y")

        # Valid CRS & Axis Order
        # If no CRS exists, assume default CRS (see opt param)
        if ds.rio.crs is None:
            ds = ds.rio.write_crs(default_crs)

        # Perfectly Uniform Grid Spacing
        # Erase microscopic floating-point drift in the coordinate arrays
        ds = ds.assign_coords(
            x=np.linspace(float(ds.x.values[0]), float(ds.x.values[-1]), len(ds.x)),
            y=np.linspace(float(ds.y.values[0]), float(ds.y.values[-1]), len(ds.y))
        )

        # Data Types and NoData Handling 
        # Cast to float32 to support np.nan and prevent integer math clipping during resampling
        ds = ds.astype("float32")
        
        # rioxarray requires writing nodata to each variable individually in a Dataset
        if isinstance(ds, xr.Dataset):
            for var in ds.data_vars:
                # Skip non-spatial variables if any exist (like a 1D time array)
                if 'x' in ds[var].dims and 'y' in ds[var].dims:
                    ds[var] = ds[var].rio.write_nodata(np.nan)
        else:
            # If it's a single DataArray
            ds = ds.rio.write_nodata(np.nan)

        return ds

    def affine_reproject(self, ds, grid_name, resample_keyword="bilinear"):
        """
        Calculates strict shape and transform parameters for rioxarray. 
        Subsequently reprojects the dataset to the 
        """

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
    #spatial homogeneity checker function

    #temporal homogeneity checker function

    #data array merging function

    #dataset constructor

    #exporting function

    #reproject crs

    #align rasters

    #fill temporal gaps
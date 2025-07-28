import xarray as xr

class spatiotemporal_cube():
    def __init__(self):
        pass

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

    #spatial homogeneity checker function

    #temporal homogeneity checker function

    #data array merging function

    #dataset constructor

    #exporting function

    #reproject crs

    #align rasters

    #fill temporal gaps
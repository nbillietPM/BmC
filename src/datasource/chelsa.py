import rasterio
from rasterio.mask import mask
import rasterio.transform
import rasterio.windows
import shapely
import geopandas as gpd
import os
import numpy

def format_url_month_ts(var, month, year, 
                        base_url="https://os.zhdk.cloud.switch.ch/chelsav2/GLOBAL/monthly", 
                        version="V.2.1",
                        year_range=range(1979,2020)):
    """
    Generates the link to the S3 bucket where the CHELSA monthly time series is stores

    Args
        var (str): The abbreviation of the variable of interest. The options for this are "clt", "cmi", "hurs", "pet", "pr", "rsds", "sfcWind", "tas", "tasmax", "tasmin", "vpd"
        month (int): An integer representing the month of interest
        year (int): An integer representing the year of interest
        base_url (str, optional): The base of the URL to the files of interest. This should not be changed expect in the case of a version upgrade or migration of the S3 bucket
        version (str, optional): The version of CHELSA
    Returns
        file_url (str): The URL that links to the file of interest
    """
    # Available variables for the monthly timeseries
    var_opt = ["clt", "cmi", "hurs", "pet", "pr", "rsds", "sfcWind", "tas", "tasmax", "tasmin", "vpd"]
    if var not in var_opt:
        raise ValueError(f"Invalid variable name: {var}. Variable must be one of the following options {var_opt}")
    if month not in range(1,13):
        raise ValueError(f"Month invalid: {month}. Please use a number between 1 and 12")
    if year not in year_range:
        raise ValueError(f"Year invalid: {year}. Please use a number between {year_range[0]} and {year[-1]}")
    #Some variables start at the second month of 1979 instead of the first one
    diff_ts = ["cmi","pet","sfcWind", "tas", "tasmax", "tasmin", "vpd"]
    if var in diff_ts and month==1 and year==1979:
        return 0
    #Returns the formatted string, months are automatically converted to the correct string format where single digits have zero padding
    return f"{base_url}/{var}/CHELSA_{var}_{month:02d}_{year}_{version}.tif"

def format_url_clim_sim_period(var, year_range, model_name, ensemble_member, 
                               base_url="https://os.zhdk.cloud.switch.ch/chelsav2/GLOBAL/climatologies",
                               version="V.2.1"):
    """
    generate url that link to the files in the S3 bucket that contain the future climate projections
    """
    var_opt=['bio10','bio11','bio12','bio13','bio14','bio15','bio16','bio17','bio18','bio19','bio1','bio2','bio3',
             'bio4','bio5','bio6','bio7','bio8','bio9','fcf','fgd','gdd0','gdd10','gdd5','gddlgd0','gddlgd10',
             'gddlgd5','gdgfgd0','gdgfgd10','gdgfgd5','gsl','gsp','gst','kg0','kg1','kg2','kg3','kg4','kg5',
             'lgd','ngd0','ngd10','ngd5','npp','scd','swe']
    if var not in var_opt:
        raise ValueError(f"Variable invalid:{var} Variable must be one the following options {var_opt}")
    model_names = ['GFDL-ESM4','IPSL-CM6A-LR','MPI-ESM1-2-HR','MRI-ESM2-0','UKESM1-0-LL','gfdl-esm4','ipsl-cm6a-lr','mpi-esm1-2-hr','mri-esm2-0','ukesm1-0-ll']
    if model_name not in model_names or model_name.upper() not in model_names:
        raise ValueError(f"Modelname invalid: {model_name} Please use on of the following model names {model_names}")
    ensemble_members = ["ssp126","ssp370","ssp585"]
    if ensemble_member not in ensemble_members:
        raise ValueError(f"Ensemble member invalid: {ensemble_member} Please use one of the following ensemble members {ensemble_members}")
    year_ranges = ["2011-2040","2041-2070","2071-2100"]
    if year_range not in year_ranges:
        raise ValueError(f"Year range invalid: {year_range} Please use on of the following year ranges {year_ranges}")
    return f"{base_url}/{year_range}/{model_name.upper()}/{ensemble_member.lower()}/bio/CHELSA_{var.lower()}_{year_range}_{model_name.lower()}_{ensemble_member.lower()}_{version}.tif"

def format_url_clim_sim_month(var, year_range, month, model_name, ensemble_member, 
                              base_url="https://os.zhdk.cloud.switch.ch/chelsav2/GLOBAL/climatologies"):
    """
    generate url that link to the files in the S3 bucket that contain the future climate projections for a specific month
    """
    var_opt=["pr", "tas", "tasmax", "tasmin"]
    if var not in var_opt:
        raise ValueError(f"Variable invalid:{var} Variable must be one the following options {var_opt}")
    model_names = ['GFDL-ESM4','IPSL-CM6A-LR','MPI-ESM1-2-HR','MRI-ESM2-0','UKESM1-0-LL','gfdl-esm4','ipsl-cm6a-lr','mpi-esm1-2-hr','mri-esm2-0','ukesm1-0-ll']
    if model_name not in model_names or model_name.upper() not in model_names:
        raise ValueError(f"Modelname invalid: {model_name} Please use on of the following model names {model_names}")
    ensemble_members = ["ssp126","ssp370","ssp585"]
    if ensemble_member not in ensemble_members:
        raise ValueError(f"Ensemble member invalid: {ensemble_member} Please use one of the following ensemble members {ensemble_members}")
    year_ranges = ["2011-2040","2041-2070","2071-2100"]
    if year_range not in year_ranges:
        raise ValueError(f"Year range invalid: {year_range} Please use on of the following year ranges {year_ranges}")
    return f"{base_url}/{year_range}/{model_name.upper()}/{ensemble_member.lower()}/{var.lower()}/CHELSA_{model_name.lower()}_r1i1p1f1_w5e5_{ensemble_member.lower()}_{var.lower()}_{month:02d}_{year_range.replace("-", "_")}_norm.tif"

def generate_transform_coordinates(subset, transform, format="array"):
    """
    A function that generates coordinate arrays for the raster defined by the affine transform

    Args
        subset (np.array): A 2D numpy array that contains the raster where the relevant information is stored in
        transform (affine.Affine): The affine transformation matrix that is characteristic for the subset
        format (str, optional): Option to output format. Standard format is array (1D) format, alternative is matrix output.
    Returns
        longitudes, latitudes (np.array): coordinate arrays that have a similar dimension to the original subset array. Each cell is characterized by a longitude and latitude pair
    """
    #Extract subset array dimensions to determine grid dimensions
    height, width = subset.shape
    #Generate meshgrid to assign index to each pixel
    rows, cols = np.meshgrid(np.arange(height), np.arange(width), indexing="ij")
    #Generate (lat, long) pairs based on the affine transform of the window ordered according to the generated indices
    longitudes, latitudes = rasterio.transform.xy(transform, rows, cols)
    if format=="array":
        return longitudes[0,:], latitudes[:,0]
    elif format=="matrix":
        return longitudes.reshape(height, width), latitudes.reshape(height, width)


def read_bounding_box(url, bbox, generate_coordinates=True):
    """
    A function that reads a subset defined by a bounding box from a cloud hosted tif file and returns the data within to the local user

    Args
        url (str): A URL that point to a cloud optimized tif file. This function is written with URL's generated by the `format_url_month_ts` function in mind.
        bbox (tuple<float>): a bounding box defined in the standard 
    Returns
        subset (np.ndarray): An array counting the measurements within the the bounding box

         If generate_coordinates is True, also returns:
            longitude (np.ndarray): Longitude grid matching the subset shape.
            latitude (np.ndarray): Latitude grid matching the subset shape.
    """
    if url==0:
        return 0
    with rasterio.open(url) as src_file:
        #Define a window that will be used to sample the region of interest
        #Transform describes the affine transformation matrix that defines the raster that is being used
        window = rasterio.windows.from_bounds(*bbox, transform=src_file.transform)
        #Read the first band of the tif file. Files are single band
        subset = src_file.read(1, window=window)
    if generate_coordinates:
        window_transform = src_file.window_transform(window)
        longitudes, latitudes = generate_transform_coordinates(subset, window_transform)
        return longitudes, latitudes, subset
    else:
        return subset

def read_polygon_area(url, shp_file, shp_path="", generate_coordinates=True):
    """
    A function that reads all data contained within the boundary of a polygon defined by a shapefile

    Args
        url (str): A URL that point to a cloud optimized tif file. This function is written with URL's generated by the `format_url_month_ts` function in mind.
        shp_file (str): Filename of the shapefile that contains the the polygon that describes the area of interest
        shp_path (str, optional): Directory where the shapefile is stored
        generate_coordinates (bool, optional): Option to generate coordinate raster associated with the subset
    Returns
        subset (np.array): An array counting the measurements within the polygon. Values outside the polygon is set to a negative value.
        
        If generate_coordinates is True, also returns:
            longitude (np.ndarray): Longitude grid matching the subset shape.
            latitude (np.ndarray): Latitude grid matching the subset shape.
    """
    if url==0:
        return 0
    #Read the shapefile 
    polygon = gpd.read_file(os.path.join(shp_path, shp_file))
    with rasterio.open(url) as src:
        #Convert the polygon to the CRS used within the src file
        polygon = polygon.to_crs(src.crs)
        #Mask out the polygon of interest and crop it out of the image
        out_img, out_transform = mask(src, polygon.geometry.apply(shapely.geometry.mapping), crop=True)
    #Returned array is 3D where the first axis is the number of bands. The tif files contain a single band in this case so this dimension can be dropped
    subset = out_img[0]
    if generate_coordinates:
        longitudes, latitudes = generate_transform_coordinates(subset, out_transform)
        return longitudes, latitudes, subset
    else:
        return subset

def generate_month_year_range(start_month, end_month, start_year, end_year):
    """
    Function to generate a list of month year pairs that need to be sampled

    Args
        start_month (int): The first month of the time range that needs to be included
        end_month (int): The end month of the time range that needs to be included
        start_year (int): The first year of the time range that needs to be included
        end_year (int): The last month of the time range that needs to be included
    
    Returns
        datetimes (list<tuple<int>>): a list consisting of (month, year) tuples  
    """
    datetimes = []
    year, month = start_year, start_month
    while (year < end_year) or (year == end_year and month <= end_month):
        datetimes.append((month, year))
        # Increment month/year
        month += 1
        if month > 12:
            month = 1
            year += 1
    return datetimes

def chelsa_month_ts(var, bbox, start_month, end_month, start_year, end_year):
    """
    return a data array in xarray 
        - spatial dimensions described
        - temporal dimension described
    """
    datetimes = generate_month_year_range(start_month, end_month, start_year, end_year)
    urls = [format_url_month_ts(var, dt[0], dt[1]) for dt in datetimes]
    data = [read_bounding_box(url, bbox) for url in urls]
    # Extract first and second arrays
    first_arrays = [item[0] for item in data]
    second_arrays = [item[1] for item in data]
    
    # Check all are equal to the first one
    first_equal = all(np.array_equal(arr, first_arrays[0]) for arr in first_arrays)
    second_equal = all(np.array_equal(arr, second_arrays[0]) for arr in second_arrays)
    if first_equal==second_equal:
        var_data = [item[2] for item in  data]
        longitudes = first_arrays[0]
        latitudes = second_arrays[0]
        datetimes = np.array([f"{dt[1]}-{dt[0]:02d}" for dt in datetimes], dtype='datetime64[M]')
        dataArray = xr.DataArray(var_data, 
                                 dims=("time", "lat", "lon"),
                                 coords={"time":datetimes, "lat":latitudes, "lon":longitudes})
        return dataArray
    
def chelsa_clim()

def chelsa_month_ds(vars, bbox, start_month, end_month, start_year, end_year):
    """
    Construct a dataset containing the multuple 
    """
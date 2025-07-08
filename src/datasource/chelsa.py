import rasterio
from rasterio.mask import mask
import rasterio.windows
import shapely
import geopandas as gpd

def format_url_month_ts(var, month, year, 
                        base_url="https://os.zhdk.cloud.switch.ch/chelsav2/GLOBAL/monthly", 
                        version="V.2.1"):
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
    #Returns the formatted string, months are automatically converted to the correct string format where single digits have zero padding
    return f"{base_url}/{var}/CHELSA_{var}_{month:02d}_{year}_{version}.tif"


def read_bounding_box():

def read_polygon_area():

def mask_nan_values():


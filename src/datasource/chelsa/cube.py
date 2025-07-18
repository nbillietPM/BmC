import os
import numpy as np
import xarray as xr
from sampling import *
from s3 import *

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
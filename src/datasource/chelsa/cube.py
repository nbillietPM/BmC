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

def chech_spatial_homo(data):
    """
    Check if the raster that is associated with the extracted data is consistent homogenous across the different subsets
    """
    longitudes_arrays = [item[0] for item in data]
    latitude_arrays = [item[1] for item in data]
    # Check all are equal to the first one
    longitudes_equal = all(np.array_equal(arr, longitudes_arrays[0]) for arr in longitudes_arrays)
    latitudes_equal = all(np.array_equal(arr, latitude_arrays[0]) for arr in latitude_arrays)
    #Assure that both latitude and longitudes are homogenous
    return longitudes_equal==latitudes_equal


def chelsa_month_ts(var, bbox, start_month, end_month, start_year, end_year):
    """
    return a data array in xarray 
        - spatial dimensions described
        - temporal dimension described
    """
    datetimes = generate_month_year_range(start_month, end_month, start_year, end_year)
    urls = [format_url_month_ts(var, dt[0], dt[1]) for dt in datetimes]
    data = [read_bounding_box(url, bbox) for url in urls]
    #check spatial consistency across the different time slices
    if chech_spatial_homo(data):
        datetimes = np.array([f"{dt[1]}-{dt[0]:02d}" for dt in datetimes], dtype='datetime64[M]')
        dataArray = xr.DataArray([item[2] for item in  data], 
                                  dims=("time", "lat", "long"),
                                  coords={"time":datetimes, "lat":data[0][1], "long":data[0][0]})
        return var,dataArray

def chelsa_clim_ref_period(var, bbox, 
                           ref_period="1981-2010"):
    url = format_url_clim_ref_period(var)
    longitudes, latitudes, data = read_bounding_box(url, bbox)
    dataArray = xr.DataArray(data, 
                             dims=("lat", "long"), 
                             coords={"lat":latitudes,"long":longitudes})
    dataArray.attrs["year_range"] = ref_period
    return var,dataArray
    """
    else:
        urls = [format_url_clim_ref_period(var) for var in vars]
        data = [read_bounding_box(url, bbox) for url in urls]
        if chech_spatial_homo(data):
            clim_data = [item[2] for item in data]
            longitudes = data[0][0]
            latitudes = data[0][1]
            dataset = xr.Dataset(dict(zip(vars, [(["lat", "long"], var_data) for var_data in clim_data])),
                                 coords={"lat": lat,"lon": lon,})
            dataset.attrs["Reference Period"] = ref_period
            return dataset
    """


def chelsa_clim_ref_month(var, bbox,
                          begin_month=1, end_month=12):
    months = range(begin_month, end_month+1)
    urls = [format_url_clim_ref_monthly(var, month) for month in months]
    data = [read_bounding_box(url, bbox) for url in urls]
    dataArray = xr.DataArray([item[2] for item in  data], 
                             dims=("months", "lat", "long"),
                             coords={"months":months, "lat":data[0][1], "long":data[0][0]})
    return var,dataArray


#format_url_clim_sim_period
#def chelsa_clim_sim_period():

#format_url_clim_sim_month
#def chelsa_clim_sim_month():

#
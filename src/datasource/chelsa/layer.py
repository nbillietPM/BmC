import os
import numpy as np
import xarray as xr
import itertools
import tqdm

from .sampling import *
from .s3 import *

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

def batch_process_urls(urls, bbox, params, desc="Processing", unit="item"):
    """
    Iterates over urls and params_list in lockstep, calling func(url, *params).
    Displays an in-line tqdm progress bar.
    
    Returns:
      - results: list of successful func outputs
      - error_params: the params tuple that caused an exception, or None if all succeeded
    """
    data = []
    total = len(urls)
    pbar = tqdm(total=total, desc=desc, unit=unit)
    for url, param in zip(urls, params):
        try:
            output = read_bounding_box(url,bbox)
            data.append(output)
            pbar.update(1)
        except Exception as e:
            pbar.close()
            print(f"\nError with params {param!r}: {e}")
            return data, param
    pbar.close()
    return data

def check_spatial_homo(data):
    """
    Check if the raster that is associated with the extracted data is consistently homogenous across the different subsets
    """
    longitudes_arrays = [item[0] for item in data]
    latitude_arrays = [item[1] for item in data]
    # Check all are equal to the first one
    longitudes_equal = all(np.array_equal(arr, longitudes_arrays[0]) for arr in longitudes_arrays)
    latitudes_equal = all(np.array_equal(arr, latitude_arrays[0]) for arr in latitude_arrays)
    #Assure that both latitude and longitudes are homogenous
    return longitudes_equal==latitudes_equal


def chelsa_month_ts(var, bbox, start_month, end_month, start_year, end_year,
                    base_url='https://os.zhdk.cloud.switch.ch/chelsav2/GLOBAL/monthly',
                    version='V.2.1'):
    """
    Generates a xarray dataArray object that collects monthly information within the specified time frame determined 
    by a starting (month, year) and an end (month, year) for a given variable of interest. The data is associated with an area of 
    interested characterized by the bbox

    Args
        to be continued
    Returns
        to be continued
    """
    datetimes = generate_month_year_range(start_month, end_month, start_year, end_year)
    urls = [format_url_month_ts(var, dt[0], dt[1], base_url=base_url, version=version) for dt in datetimes]
    print(f"-----Retrieving monthly CHELSA data for variable '{var}' within bbox {bbox}-----")
    data = batch_process_urls(urls, bbox, datetimes)
    #check spatial consistency across the different time slices
    if check_spatial_homo(data):
        datetimes = np.array([f"{dt[1]}-{dt[0]:02d}" for dt in datetimes], dtype="datetime64[ns]")
        dataArray = xr.DataArray([item[2] for item in  data], 
                                  dims=("time", "lat", "long"),
                                  coords={"time":datetimes, "lat":data[0][1], "long":data[0][0]})
        return var,dataArray

def chelsa_clim_ref_period(var, bbox, 
                           ref_period="1981-2010",
                           base_url='https://os.zhdk.cloud.switch.ch/chelsav2/GLOBAL/climatologies/1981-2010',
                           version='V.2.1'):
    """
    Generates a xarray dataArray object that collects the information for the reference climatological data the 
    specified variable of interest. The data is associated with an area of interested characterized by the bbox

    Args
        to be continued
    Returns
        to be continued
    """
    url = format_url_clim_ref_period(var, base_url=base_url, version=version)
    longitudes, latitudes, data = read_bounding_box(url, bbox)
    dataArray = xr.DataArray(data, 
                             dims=("lat", "long"), 
                             coords={"lat":latitudes,"long":longitudes})
    dataArray.attrs["year_range"] = ref_period
    return var,dataArray


def chelsa_clim_ref_month(var, bbox, months,
                          ref_period="1981-2010",
                          base_url='https://os.zhdk.cloud.switch.ch/chelsav2/GLOBAL/climatologies/1981-2010',
                          version='V.2.1'):
    """
    Generate a xarray dataArray for reference climatological data on a monthly basis for the specified variable of interest.
    The data is associated with an area of interested characterized by the bbox
    """
    #Generate URL's for the given parameter combinations
    urls = [format_url_clim_ref_monthly(var, month, base_url=base_url, version=version) for month in months]
    #Read the data for the generated URL's within the specified bbox
    data = [read_bounding_box(url, bbox) for url in urls]
    #Check spatial homogeneity condition
    if check_spatial_homo(data):
        dataArray = xr.DataArray([item[2] for item in  data], 
                                dims=("months", "lat", "long"),
                                coords={"months":months, "lat":data[0][1], "long":data[0][0]})
        #Add metadata of the reference period to the array
        dataArray.attrs["year_range"] = ref_period
        return var,dataArray

def chelsa_clim_sim_period(var, bbox, year_ranges, model_names, ensemble_members,
                           base_url='https://os.zhdk.cloud.switch.ch/chelsav2/GLOBAL/climatologies',
                           version='V.2.1'):
    """
    
    """
    #Generate all parameter combinations for the given 
    params = list(itertools.product(year_ranges, model_names, ensemble_members))
    urls = [format_url_clim_sim_period(var, *param) for param in params]
    data = [read_bounding_box(url, bbox, base_url=base_url, version=version) for url in urls]
    if check_spatial_homo(data):
        dataArray = xr.DataArray(np.stack([item[2] for item in  data]).reshape(len(year_ranges), 
                                                                               len(model_names), 
                                                                               len(ensemble_members), 
                                                                               len(data[0][1]), 
                                                                               len(data[0][0])), 
                                  dims=("year_range", "model_name", "ensemble_member", "lat", "long"),
                                  coords={"year_range":year_ranges,
                                          "model_name":model_names,
                                          "ensemble_member":ensemble_members, 
                                          "lat":data[0][1], 
                                          "long":data[0][0]})
        return var,dataArray

def chelsa_clim_sim_month(var, bbox, year_ranges, months, model_names, ensemble_members,
                          base_url='https://os.zhdk.cloud.switch.ch/chelsav2/GLOBAL/climatologies',
                          version='V.2.1'):
    params = list(itertools.product(year_ranges, months, model_names, ensemble_members))
    urls = [format_url_clim_sim_month(var, *param, base_url=base_url, version=version) for param in params]
    data = [read_bounding_box(url, bbox) for url in urls]
    if check_spatial_homo(data):
        dataArray = xr.DataArray(np.stack([item[2] for item in  data]).reshape(len(year_ranges),
                                                                               len(months), 
                                                                               len(model_names), 
                                                                               len(ensemble_members), 
                                                                               len(data[0][1]), 
                                                                               len(data[0][0])), 
                                  dims=("year_range", "month", "model_name", "ensemble_member", "lat", "long"),
                                  coords={"year_range":year_ranges,
                                          "month":months,
                                          "model_name":model_names,
                                          "ensemble_member":ensemble_members, 
                                          "lat":data[0][1], 
                                          "long":data[0][0]})
        return var,dataArray

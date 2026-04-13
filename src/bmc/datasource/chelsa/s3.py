import requests

def validate_url_exists(url: str) -> bool:
    try:
        response = requests.head(url, timeout=10) 
        # Response code 200 indicates a valid URL
        if response.status_code == 200:
            return True
        return False
    except requests.exceptions.RequestException:
        return False

def format_url_month_ts(var, month, year, 
                        base_url="https://os.unil.cloud.switch.ch/chelsa02/chelsa/global/monthly", 
                        version="V.2.1",
                        year_range=list(range(1979,2021))):
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
        raise ValueError(f"Year invalid: {year}. Please use a number between {year_range[0]} and {year_range[-1]}")
    #Some variables start at the second month of 1979 instead of the first one
    diff_ts = ["cmi","pet","sfcWind", "tas", "tasmax", "tasmin", "vpd"]
    if var in diff_ts and month==1 and year==1979:
        raise ValueError(f"Variables [{diff_ts}] start at month 2 of year 1979. No data available for month 1")    
    return f"{base_url}/{var}/{year}/CHELSA_{var}_{month:02d}_{year}_{version}.tif"

def format_chelsa_drought_url(var_name, year, 
                              month=None, version="V.2.1", base_url="https://os.unil.cloud.switch.ch/chelsa02/chelsa/global/annual"):
    allowed_var_names = ["mymd", "mymd10", "qkndvi", "spei12", "spi12"] 
    #Variable name check
    if var_name not in allowed_var_names:
        raise ValueError(f"{var_name} invalid. Please select one of the following variables {allowed_var_names}")
    #Temporal validity check
    if (year not in list(range(1980, 2019))) and (var_name not in ["mymd", "mymd10", "spei12", "spi12"]):
        raise ValueError(f"For variables ['mymd', 'mymd10', 'spei12', 'spi12'] the year must be between [1980,2018]. Current value for year parameter: {year}")
    if (year<1982) and (var_name=="qkndvi"):
        raise ValueError(f"Index 'qkndvi' starts at year 1982. Selected year is {year}")
    if (month not in list(range(1, 13))) and (var_name in ["spei12", "spi12"]):
        raise ValueError(f"{var_name} requires a month. Given value for optional parameter 'month': {month}")
    if var_name in ["mymd", "mymd10", "qkndvi"]:
        return f"{base_url}/{var_name}/{year}/CHELSA_{var_name}_{year}_{version}.tif"
    else:
        return f"{base_url}/{var_name}/{year}/CHELSA_{var_name}_{month}_{year}_{version}.tif"
    
def format_url_clim_ref_monthly(var, month,
                                ref_period = "1981-2010",
                                base_url="https://os.unil.cloud.switch.ch/chelsa02/chelsa/global/climatologies",
                                version="V.2.1"):
    """
    Generate URL's that link to the reference data tif files on a monthly basis for the reference period 1980-2010 
    """
    var_opt=["clt","cmi","hurs","pet","pr","rsds","sfcWind","tas","tasmax","tasmin", "vpd"]
    if var not in var_opt:
        raise ValueError(f"Invalid variable name: {var}. Variable must be one of the following options {var_opt}")
    if month not in range(1,13):
        raise ValueError(f"Month invalid: {month}. Please use a number between 1 and 12")
    else:
        return f"{base_url}/{var}/{ref_period}/CHELSA_{var}_{month}_{ref_period}_{version}.tif"
    
def format_url_clim_ref_period(var,
                               ref_period = "1981-2010",
                               base_url="https://os.unil.cloud.switch.ch/chelsa02/chelsa/global/bioclim",
                               version="V.2.1"):
    """
    Generate URL's that link to the reference data tif files for the BIOCLIM+ variables for the reference period 1980-2010
    """
    var_opt = ["bio01", "bio02", "bio03", "bio04", "bio05", "bio06", "bio07", "bio08", "bio09", "bio10", 
               "bio11", "bio12", "bio13", "bio14", "bio15", "bio16", "bio17", "bio18", "bio19", "cltmax", 
               "cltmean", "cltmin", "cltrange", "cmimax", "cmimean", "cmimin", "cmirange", "fcf", "fgd", 
               "gdd0", "gdd10", "gdd5", "gddlgd0", "gddlgd10", "gddlgd5", "gdgfgd10", "gdgfgd5", "gsl", 
               "gsp", "gst", "hursmax", "hursmean", "hursmin", "hursrange", "kg0", "kg1", "kg2", "kg3", 
               "kg4", "kg5", "lgd", "ngd0", "ngd10", "ngd5", "npp", "petmax", "petmean", "petmin", "petrange",
               "rsdsmax", "rsdsmean", "rsdsmin", "rsdsrange", "scd", "sfcWindmax", "sfcWindmean", "sfcWindmin", 
               "sfcWindrange", "swb", "swe", "vpdmax", "vpdmean", "vpdmin", "vpdrange"]
    if var not in var_opt:
        raise ValueError(f"Invalid variable name: {var}. Variable must be one of the following options {var_opt}")
    else:  
        return f"{base_url}/{var}/CHELSA_{var}_{ref_period}_{version}.tif"
    
def format_url_clim_sim_period(var, year_range, model_name, scenario, 
                               base_url="https://os.unil.cloud.switch.ch/chelsa02/chelsa/global/bioclim",
                               version="V.2.1"):
    """
    generate url that link to the files in the S3 bucket that contain the future climate projections
    """
    var_opt = ["bio01", "bio02", "bio03", "bio04", "bio05", "bio06", "bio07", "bio08", "bio09", "bio10", 
               "bio11", "bio12", "bio13", "bio14", "bio15", "bio16", "bio17", "bio18", "bio19", "fcf", "fgd", 
               "gdd0", "gdd10", "gdd5", "gddlgd0", "gddlgd10", "gddlgd5", "gdgfgd10", "gdgfgd5", "gsl", "gsp", 
               "gst", "kg0", "kg1", "kg2", "kg3", "kg4", "kg5", "lgd", "ngd0", "ngd10", "ngd5", "npp","scd"]
    if var not in var_opt:
        raise ValueError(f"Variable invalid:{var} Variable must be one the following options {var_opt}")
    model_names = ['GFDL-ESM4','IPSL-CM6A-LR','MPI-ESM1-2-HR','MRI-ESM2-0','UKESM1-0-LL','gfdl-esm4','ipsl-cm6a-lr','mpi-esm1-2-hr','mri-esm2-0','ukesm1-0-ll']
    if model_name not in model_names or model_name.upper() not in model_names:
        raise ValueError(f"Modelname invalid: {model_name} Please use on of the following model names {model_names}")
    scenarios = ["ssp126","ssp370","ssp585"]
    if scenario not in scenarios:
        raise ValueError(f"Ensemble member invalid: {scenario} Please use one of the following ensemble members {scenarios}")
    year_ranges = ["2011-2040","2041-2070","2071-2100"]
    if year_range not in year_ranges:
        raise ValueError(f"Year range invalid: {year_range} Please use on of the following year ranges {year_ranges}")
    return f"{base_url}/{var}/{year_range}/{model_name.upper()}/{scenario.lower()}/CHELSA_{model_name.lower()}_{scenario.lower()}_{var}_{year_range}_{version}.tif"

def format_url_clim_sim_month(var, year_range, month, model_name, scenario, 
                              base_url="https://os.unil.cloud.switch.ch/chelsa02/chelsa/global/climatologies",
                              version="V.2.1"):
    """
    generate url that link to the files in the S3 bucket that contain the future climate projections for a specific month

    Currently the naming convention for the file is inconsistent with the other files and might get adjusted in the future
    """
    var_opt=["pr", "tas", "tasmax", "tasmin"]
    if var not in var_opt:
        raise ValueError(f"Variable invalid:{var} Variable must be one the following options {var_opt}")
    model_names = ['GFDL-ESM4','IPSL-CM6A-LR','MPI-ESM1-2-HR','MRI-ESM2-0','UKESM1-0-LL','gfdl-esm4','ipsl-cm6a-lr','mpi-esm1-2-hr','mri-esm2-0','ukesm1-0-ll']
    if model_name not in model_names or model_name.upper() not in model_names:
        raise ValueError(f"Modelname invalid: {model_name} Please use on of the following model names {model_names}")
    scenarios = ["ssp126","ssp370","ssp585"]
    if scenario not in scenarios:
        raise ValueError(f"Ensemble member invalid: {scenario} Please use one of the following ensemble members {scenarios}")
    year_ranges = ["2011-2040","2041-2070","2071-2100"]
    if year_range not in year_ranges:
        raise ValueError(f"Year range invalid: {year_range} Please use on of the following year ranges {year_ranges}")
    return (f"{base_url}/{var}/{year_range}/{model_name.upper()}/{scenario.lower()}/CHELSA_{model_name.lower()}_r1i1p1f1_w5e5_{scenario.lower()}_{var.lower()}_{month:02d}_{year_range}_{version}.tif")
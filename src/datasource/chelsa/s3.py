def format_url_month_ts(var, month, year, 
                        base_url="https://os.zhdk.cloud.switch.ch/chelsav2/GLOBAL/monthly", 
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
    if var == "rsds":
        #Naming inconsistency where year and month have switched place
        return f"{base_url}/{var}/CHELSA_{var}_{year}_{month:02d}_{version}.tif"
    elif var == "pet":
        #Naming inconsistency where '_penman' is added after variable name
        return f"{base_url}/{var}/CHELSA_{var}_penman_{month:02d}_{year}_{version}.tif"
    else:
        #Returns the formatted string, months are automatically converted to the correct string format where single digits have zero padding
        return f"{base_url}/{var}/CHELSA_{var}_{month:02d}_{year}_{version}.tif"
    
def format_url_clim_ref_period(var,
                               ref_period = "1981-2010",
                               base_url="https://os.zhdk.cloud.switch.ch/chelsav2/GLOBAL/climatologies/1981-2010",
                               version="V.2.1"):
    """
    Generate URL's that link to the reference data tif files for the BIOCLIM+ variables for the reference period 1980-2010
    """
    var_opt = ['ai','bio10','bio11','bio12','bio13','bio14','bio15','bio16','bio17','bio18','bio19','bio1','bio2','bio3',
               'bio4','bio5','bio6','bio7','bio8','bio9','clt_max','clt_mean','clt_min','clt_range','cmi_max','cmi_mean',
               'cmi_min','cmi_range','fcf','fgd','gdd0','gdd10','gdd5','gddlgd0','gddlgd10','gddlgd5','gdgfgd0','gdgfgd10',
               'gdgfgd5','gsl','gsp','gst','hurs_max','hurs_mean','hurs_min','hurs_range','kg0','kg1','kg2','kg3','kg4','kg5',
               'lgd','ngd0','ngd10','ngd5','npp','pet_penman_max','pet_penman_mean','pet_penman_min','pet_penman_range',
               'rsds_max','rsds_min','rsds_mean','rsds_range','scd','sfcWind_max','sfcWind_mean','sfcWind_min','sfcWind_range',
               'swb','swe','vpd_max','vpd_mean','vpd_min','vpd_range']
    if var not in var_opt:
        raise ValueError(f"Invalid variable name: {var}. Variable must be one of the following options {var_opt}")
    #Deviation in naming scheme
    if "rsds" in var:
        var_split = var.split("_")
        return f"{base_url}/bio/CHELSA_rsds_{ref_period}_{var_split[1]}_{version}.tif"
    else:  
        return f"{base_url}/bio/CHELSA_{var}_{ref_period}_{version}.tif"

def format_url_clim_ref_monthly(var, month,
                                ref_period = "1981-2010",
                                base_url="https://os.zhdk.cloud.switch.ch/chelsav2/GLOBAL/climatologies/1981-2010",
                                version="V.2.1"):
    """
    Generate URL's that link to the reference data tif files on a monthly basis for the reference period 1980-2010 
    """
    var_opt=["clt","cmi","hurs","pet","pr","rsds","sfcWind","tas","tasmax","tasmin", "vpd"]
    if var not in var_opt:
        raise ValueError(f"Invalid variable name: {var}. Variable must be one of the following options {var_opt}")
    if month not in range(1,13):
        raise ValueError(f"Month invalid: {month}. Please use a number between 1 and 12")
    if var == "rsds":
        #Naming inconsistency where year and month have switched place
        return f"{base_url}/{var}/CHELSA_{var}_{ref_period}_{month:02d}_{version}.tif"
    if var == "pet":
        #Naming inconsistency where '_penman' is added after variable name
        return f"{base_url}/{var}/CHELSA_{var}_penman_{month:02d}_{ref_period}_{version}.tif"
    else:
        return f"{base_url}/{var}/CHELSA_{var}_{month:02d}_{ref_period}_{version}.tif"
    

def format_url_clim_sim_period(var, year_range, model_name, scenario, 
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
    scenarios = ["ssp126","ssp370","ssp585"]
    if scenario not in scenarios:
        raise ValueError(f"Ensemble member invalid: {scenario} Please use one of the following ensemble members {scenarios}")
    year_ranges = ["2011-2040","2041-2070","2071-2100"]
    if year_range not in year_ranges:
        raise ValueError(f"Year range invalid: {year_range} Please use on of the following year ranges {year_ranges}")
    return f"{base_url}/{year_range}/{model_name.upper()}/{scenario.lower()}/bio/CHELSA_{var.lower()}_{year_range}_{model_name.lower()}_{scenario.lower()}_{version}.tif"

def format_url_clim_sim_month(var, year_range, month, model_name, scenario, 
                              base_url="https://os.zhdk.cloud.switch.ch/chelsav2/GLOBAL/climatologies",
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
    #Added a replace '-' with "_" to take the deviating naming structure of CHELSA into account
    year_range_clean = year_range.replace("-", "_")
    return (f"{base_url}/{year_range}/{model_name.upper()}/{scenario.lower()}/{var.lower()}/CHELSA_{model_name.lower()}_r1i1p1f1_w5e5_{scenario.lower()}_{var.lower()}_{month:02d}_{year_range_clean}_norm.tif")

import yaml
import os

def construct_bbox(param_dict):
    return (param_dict["spatial"]["bbox"]["long_min"],
            param_dict["spatial"]["bbox"]["lat_min"],
            param_dict["spatial"]["bbox"]["long_max"],
            param_dict["spatial"]["bbox"]["lat_max"])

def filter_variables(var_list, param_dict, layer_name):
    if param_dict["layers"][layer_name]["variables"]["included"]:
        #included list becomes variable list
        return param_dict["layers"][layer_name]["variables"]["included"]
    elif param_dict["layers"][layer_name]["variables"]["excluded"]:
        #filter out excluded variables by subtracting the set from all possible variables
        return list(set(var)-set(param_dict["layers"][layer_name]["variables"]["excluded"]))
    else:
        return var_list

def read_chelsa_month_param(param_file, param_path="../../../config"):
    var = ["clt", "cmi", "hurs", "pet", "pr", "rsds", "sfcWind", "tas", "tasmax", "tasmin", "vpd"]
    layer_name = "chelsa_month"
    #Construct the relative path to the parameter yaml file
    param_filepath = os.path.join(param_path, param_file)
    with open(param_filepath) as f:
        param_dict = yaml.safe_load(f)
    if param_dict["spatial"]["method"]=="bbox":
        #Read in the bbox coordinates from the parameter dictionary
        bbox = construct_bbox(param_dict)
    var = filter_variables(var, param_dict, layer_name) 
    chelsa_month_params = {"var":var, 
                           "bbox":bbox, 
                           "start_month":param_dict["layers"][layer_name]["time"]["start_month"],
                           "end_month":param_dict["layers"][layer_name]["time"]["end_month"],
                           "start_year":param_dict["layers"][layer_name]["time"]["start_year"],
                           "end_year":param_dict["layers"][layer_name]["time"]["end_year"],
                           "base_url":param_dict["layers"][layer_name]["source"]["base_url"],
                           "version":param_dict["layers"][layer_name]["source"]["version"]}
    return chelsa_month_params

def read_chelsa_clim_ref_period_param(param_file, param_path="../../../config"):
    var = ['ai','bio10','bio11','bio12','bio13','bio14','bio15','bio16','bio17','bio18','bio19','bio1','bio2','bio3',
           'bio4','bio5','bio6','bio7','bio8','bio9','clt_max','clt_mean','clt_min','clt_range','cmi_max','cmi_mean',
           'cmi_min','cmi_range','fcf','fgd','gdd0','gdd10','gdd5','gddlgd0','gddlgd10','gddlgd5','gdgfgd0','gdgfgd10',
           'gdgfgd5','gsl','gsp','gst','hurs_max','hurs_mean','hurs_min','hurs_range','kg0','kg1','kg2','kg3','kg4','kg5',
           'lgd','ngd0','ngd10','ngd5','npp','pet_penman_max','pet_penman_mean','pet_penman_min','pet_penman_range',
           'rsds_max','rsds_min','rsds_mean','rsds_range','scd','sfcWind_max','sfcWind_mean','sfcWind_min','sfcWind_range',
           'swb','swe','vpd_max','vpd_mean','vpd_min','vpd_range']
    layer_name = "chelsa_clim_ref_period"
    #Construct the relative path to the parameter yaml file
    param_filepath = os.path.join(param_path, param_file)
    with open(param_filepath) as f:
        param_dict = yaml.safe_load(f)
    if param_dict["spatial"]["method"]=="bbox":
        #Read in the bbox coordinates from the parameter dictionary
        bbox = construct_bbox(param_dict)
    var = filter_variables(var, param_dict, layer_name)
    chelsa_clim_ref_period_params = {"var":var, 
                                     "bbox":bbox, 
                                     "ref_period":param_dict["layers"][layer_name]["time"]["year_range"],
                                     "base_url":param_dict["layers"][layer_name]["source"]["base_url"],
                                     "version":param_dict["layers"][layer_name]["source"]["version"]}
    return chelsa_clim_ref_period_params  

def read_chelsa_clim_ref_month_param(param_file, param_path="../../../config"):
    var=['bio10','bio11','bio12','bio13','bio14','bio15','bio16','bio17','bio18','bio19','bio1','bio2','bio3',
         'bio4','bio5','bio6','bio7','bio8','bio9','fcf','fgd','gdd0','gdd10','gdd5','gddlgd0','gddlgd10',
         'gddlgd5','gdgfgd0','gdgfgd10','gdgfgd5','gsl','gsp','gst','kg0','kg1','kg2','kg3','kg4','kg5',
         'lgd','ngd0','ngd10','ngd5','npp','scd','swe']
    layer_name = "chelsa_clim_ref_month"
    #Construct the relative path to the parameter yaml file
    param_filepath = os.path.join(param_path, param_file)
    with open(param_filepath) as f:
        param_dict = yaml.safe_load(f)
    if param_dict["spatial"]["method"]=="bbox":
        #Read in the bbox coordinates from the parameter dictionary
        bbox = construct_bbox(param_dict)
    var = filter_variables(var, param_dict, layer_name)
    if param_dict["layers"][layer_name]["time"]["include_all"]:
        months = list(range(1,13))
    else:
        months = param_dict["layers"][layer_name]["time"]["months"]
    chelsa_clim_ref_month_params = {"var":var, 
                                     "bbox":bbox, 
                                     "months":months,
                                     "ref_period":param_dict["layers"][layer_name]["time"]["year_range"],
                                     "base_url":param_dict["layers"][layer_name]["source"]["base_url"],
                                     "version":param_dict["layers"][layer_name]["source"]["version"]}
    return chelsa_clim_ref_month_params 
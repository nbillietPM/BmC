import yaml
import os

def read_chelsa_month_param(param_file, param_path="../../../config"):
    var = ["clt", "cmi", "hurs", "pet", "pr", "rsds", "sfcWind", "tas", "tasmax", "tasmin", "vpd"]
    #Construct the relative path to the parameter yaml file
    param_filepath = os.path.join(param_path, param_file)
    with open(param_filepath) as f:
        param_dict = yaml.safe_load(f)
    if param_dict["spatial"]["method"]=="bbox":
        #Read in the bbox coordinates from the parameter dictionary
        bbox = (param_dict["spatial"]["bbox"]["long_min"],
                param_dict["spatial"]["bbox"]["lat_min"],
                param_dict["spatial"]["bbox"]["long_max"],
                param_dict["spatial"]["bbox"]["lat_max"])
    elif param_dict["layers"]["chelsa_month"]["variables"]["included"]:
        #included list becomes variable list
        var = param_dict["layers"]["chelsa_month"]["variables"]["included"]
    elif param_dict["layers"]["chelsa_month"]["variables"]["excluded"]:
        #filter out excluded variables by subtracting the set from all possible variables
        var = list(set(var)-set(param_dict["layers"]["chelsa_month"]["variables"]["excluded"]))
    chelsa_month_params = {"var":var, 
                           "bbox":bbox, 
                           "start_month":param_dict["layers"]["chelsa_month"]["time"]["start_month"],
                           "end_month":param_dict["layers"]["chelsa_month"]["time"]["end_month"],
                           "start_year":param_dict["layers"]["chelsa_month"]["time"]["start_year"],
                           "end_year":param_dict["layers"]["chelsa_month"]["time"]["end_year"],
                           "base_url":param_dict["layers"]["chelsa_month"]["source"]["base_url"],
                           "version":param_dict["layers"]["chelsa_month"]["source"]["version"]}
    return chelsa_month_params

    
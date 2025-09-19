from datasource.chelsa import layer
from utils.chelsa import extract_param
from .spatiotemporal import *
import yaml
import os

class chelsa_cube(spatiotemporal_cube):
    def __init__(self):
        self.layers = ["chelsa_month", 
                       "chelsa_clim_ref_period",
                       "chelsa_clim_ref_month",
                       "chelsa_clim_sim_period",
                       "chelsa_clim_sim_month"]

    #generate monthly data for a set of variables
    def generate_chelsa_month_layer(self, param_file, param_path):
        chelsa_month_param = extract_param.read_chelsa_month_param(param_file, param_path = param_path)
        var_names, data = zip(*self.da_layer_constructor(layer.chelsa_month_ts, chelsa_month_param)) #returns list of tuples (var_name, data)
        #Current assumption is that all variables will be requested
        #In future general harmonization functions will be implemented 
        #Select the coordinates from the second variable, clt is the only one that differs
        target_lat = data[1].lat.values
        target_long = data[1].long.values
        upscaled_clt = self.regrid_spatial_coordinates(data[0], target_lat, target_long)
        data = [upscaled_clt]+list(data[1:])
        ds = xr.Dataset(dict(zip(var_names, data)))
        return ds

    def generate_chelsa_ref_period_layer(self, param_file, param_path):
        chelsa_ref_period_param = extract_param.read_chelsa_clim_ref_period_param(param_file, param_path = param_path)
        var_names, data = zip(*self.da_layer_constructor(layer.chelsa_clim_ref_period, chelsa_ref_period_param))
        upscale_idx = [var_names.index(el) for el in ['clt_max', 'clt_mean', 'clt_min', 'clt_range'] if el in var_names]
        data = list(data)
        if upscale_idx:
            #Select the idx of data array that does not need to be upscaled
            #The difference between sets generates the set containing idx that are not in upscale idx
            target_idx = list(set(list(range(len(var_names))))-set(upscale_idx))[0]
            target_lat = data[target_idx].lat.values
            target_long = data[target_idx].long.values
            upscaled_da = [self.regrid_spatial_coordinates(data[idx], target_lat, target_long) for idx in upscale_idx]
            for idx, da in zip(upscale_idx, upscaled_da):
                data[idx] = da
        ds = xr.Dataset(dict(zip(var_names, data)))
        return ds

    def generate_chelsa_ref_month_layer(self, param_file, param_path):
        chelsa_ref_month_param = extract_param.read_chelsa_clim_ref_month_param(param_file, param_path = param_path)
        var_names, data = zip(*self.da_layer_constructor(layer.chelsa_clim_ref_month, chelsa_ref_month_param))
        #Current assumption is that all variables will be requested
        #In future general harmonization functions will be implemented 
        #Select the coordinates from the second variable, clt is the only one that differs
        target_lat = data[1].lat.values
        target_long = data[1].long.values
        upscaled_clt = self.regrid_spatial_coordinates(data[0], target_lat, target_long)
        data = [upscaled_clt]+list(data[1:])
        ds = xr.Dataset(dict(zip(var_names, data)))
        return ds

    def generate_chelsa_sim_period_layer(self, param_file, param_path):
        chelsa_sim_period_param = extract_param.read_chelsa_clim_sim_period_param(param_file, param_path = param_path)
        var_names, data = zip(*self.da_layer_constructor(layer.chelsa_clim_sim_period, chelsa_sim_period_param))
        ds = xr.Dataset(dict(zip(var_names, data)))
        return ds

    def generate_chelsa_sim_month_layer(self, param_file, param_path):
        chelsa_sim_month_param = extract_param.read_chelsa_clim_sim_month_param(param_file, param_path = param_path)
        var_names, data = zip(*self.da_layer_constructor(layer.chelsa_clim_sim_month, chelsa_sim_month_param))
        ds = xr.Dataset(dict(zip(var_names, data)))
        return ds

    def generate_chelsa_cube(self, param_file, param_path):
        param_filepath = os.path.join(param_path, param_file)
        with open(param_filepath) as f:
            param_dict = yaml.safe_load(f)
        enabled_layers = [param_dict["layers"]["chelsa"][layer] for layer in self.layers]
        functions = [self.generate_chelsa_month_layer, 
                     self.generate_chelsa_ref_period_layer,
                     self.generate_chelsa_ref_month_layer,
                     self.generate_chelsa_sim_period_layer,
                     self.generate_chelsa_sim_month_layer]
        enabled_layers_names = [name for name, call in zip(self.layers, enabled_layers) if call]
        data = [func(param_file, param_path) for func, call in zip(functions, enabled_layers) if call]
        return enabled_layers_names, data





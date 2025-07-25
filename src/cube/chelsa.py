from ..datasource.chelsa import layer
from ..utils.chelsa import extract_param
from .spatiotemporal import *

class chelsa_cube(spatiotemporal_cube):
    def __init__(self):
        self.layers = ["chelsa_month", 
                       "chelsa_clim_ref_period",
                       "chelsa_clim_ref_month",
                       "chelsa_clim_sim_period",
                       "chelsa_clim_sim_month"]
        self.data = []

    #generate monthly data for a set of variables
    def generate_chelsa_month_layer(self, param_file, param_path = "../../config"):
        """
        Generate the data layer for the monthly timeseries in the CHELSA data source
        """
        chelsa_month_param = extract_param.read_chelsa_month_param(param_file, param_path = param_path)
        var_names, data = zip(*self.da_layer_constructor(layer.chelsa_month_ts, chelsa_month_param)) #returns list of tuples (var_name, data)
        #Current assumption is that all variables will be requested
        #In future general harmonization functions will be implemented 
        #Select the coordinates from the second variable, clt is the only one that differs
        target_lat = data[1].lat.values
        target_long = data[1].long.values
        upscaled_clt = self.regrid_spatial_coordinates(data[0], target_lat, target_long)
        data = [upscaled_clt]+list(data[1:])
        return self.da_concat(data, "variable", list(var_names))

    #generate climate reference data for a set of variables in the specified ref period
    #def generate_chelsa_ref_period_layer():

    #generate climate reference data for a set of variables in the specified ref period on a monthly basis
    #def generate_chelsa_ref_month_layer():

    #generate climate simulation data for a set of variables for the desired periods and simulation options

    #generate climate simulation data for a set of variables for the desired periods and simulation options on a monthly basis





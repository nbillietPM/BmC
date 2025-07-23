from datasource.chelsa import *
from utils import read_params
from spatiotemporal import *

class chelsa_cube(spatiotemporal_cube):
    def __init__(self):
        self.layers = ["chelsa_month", 
                       "chelsa_clim_ref_period",
                       "chelsa_clim_ref_month",
                       "chelsa_clim_sim_period",
                       "chelsa_clim_sim_month"]
        self.data = []
        self.parameters = [read_params(layer_name) for layer_name in self.layers]

    #generate monthly data for a set of variables

    #generate climate reference data for a set of variables in the specified ref period

    #generate climate reference data for a set of variables in the specified ref period on a monthly basis

    #generate climate simulation data for a set of variables for the desired periods and simulation options

    #generate climate simulation data for a set of variables for the desired periods and simulation options on a monthly basis





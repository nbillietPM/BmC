from datasource.chelsa import *
from utils import read_params

class chelsa_cube():
    def __init__(self):
        self.layers = ["chelsa_month", 
                       "chelsa_clim_ref_period",
                       "chelsa_clim_ref_month",
                       "chelsa_clim_sim_period",
                       "chelsa_clim_sim_month"]
        self.data = [[],[],[],[],[]]
        self.parameters = [read_params(layer_name) for layer_name in self.layers]


from .chelsa import *
from datatree import DataTree
import xarray as xr

class bmd_cube(chelsa_cube):
    def __init__(self):
        super().__init__()
        # Separate the known layers into static and dynamic temporal layers
        self.layers = {"static":[], "dynamic":{}}
        #self.data should be structured as a tuple of layer name and the datatree that accompagnies it
        self.data = {"static":[], "dynamic":[]}

    def generate_bmd_data(self, param_file, param_path):
        return None

    def bmd_cube(self, param_file, param_path):
        #Construct the tree structure by building up the branches
        branch_names = []
        branch_trees = []
        if self.data["static"]:
            #Constructs a datatree where for the static layers which will be 
            static_dt = DataTree("static", children={dict(zip(self.layers["static"], 
                                                              self.data["static"]))})
            branch_names.append("static")
            branch_trees.append(static_dt)
        if self.data["dynamic"]:
            #Constructs a datatree where for the static layers which will be 
            dynamic_dt = DataTree("dynamic", children={dict(zip(self.layers["dynamic"], 
                                                                self.data["dynamic"]))})
            branch_names.append("dynamic")
            branch_trees.append(dynamic_dt)
        return None
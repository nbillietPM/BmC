from datasource.gbif import layer
from .spatiotemporal import *
import yaml
import os
import pandas as pd

class gbif_cube(spatiotemporal_cube):
    def __init__(self):
        pass

    """
    def fetch_data(self, param_file, param_path):
        param_filepath = param_file if os.path.isabs(param_file) else os.path.join(param_path, param_file)
        with open(param_filepath) as f:
            param_dict = yaml.safe_load(f)
    """ 

    def construct_gbif_layer(self, param_file, param_path):
        #fetch file to be read
        param_filepath = param_file if os.path.isabs(param_file) else os.path.join(param_path, param_file)
        with open(param_filepath) as f:
            param_dict = yaml.safe_load(f)
        raw_filepath = os.path.join(param_dict["layers"]["gbif"]["file"]["file_path"], 
                                    param_dict["layers"]["gbif"]["file"]["file_name"])
        species_df = pd.read_csv(raw_filepath, sep=param_dict["layers"]["gbif"]["file"]["sep"])
        da = layer.gbif_sparse_array(species_df, ["specieskey", "genuskey", "familykey", "classkey", "eeacellcode"], "occurrences")
        return da 
       
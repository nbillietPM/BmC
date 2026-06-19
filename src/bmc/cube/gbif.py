from bmc.datasource.gbif import layer
from bmc.cube.spatiotemporal import *
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
    
    def sparse_to_pseudodense(self,dataset, var_name):
        """
        Convert one sparse DataArray into a "pseudo‐dense" xr.Dataset, but
        hang onto the original var_name so load_sparse never needs to be told.
        """
        sparse_data = dataset[var_name].data  # sparse.COO
        
        ds_sparse = xr.Dataset({
            "coords": (("ndim", "nnz"), sparse_data.coords),
            "data":   ("nnz", sparse_data.data),
            "shape":  ("ndim", np.array(sparse_data.shape, dtype=np.int64)),
            "dims":   ("ndim", list(dataset[var_name].dims)),
        })

        # copy over the real coordinate variables
        for c in dataset[var_name].coords:
            ds_sparse[c] = dataset[var_name].coords[c]

        # save the original var name as a dataset attribute
        ds_sparse.attrs["__orig_var_name__"] = var_name

        #carry over any top‐level attrs
        ds_sparse.attrs.update(dataset[var_name].attrs)
        return ds_sparse

    def load_sparse(self, ds_sparse: xr.Dataset) -> xr.Dataset:
        """
        Rebuild the sparse.COO from a pseudo‐dense dataset and return
        a one‐variable xr.Dataset using the original var_name.
        """
        # pull back the components
        coords = ds_sparse["coords"].values
        data   = ds_sparse["data"].values
        shape  = tuple(ds_sparse["shape"].values)
        dims   = list(ds_sparse["dims"].values)

        # rebuild sparse.COO
        s = sparse.COO(coords, data, shape=shape)

        # recover any real coords (e.g. time, x, y)
        coord_vars = {
            c: ds_sparse[c]
            for c in ds_sparse.coords
            if c not in ("coords", "data", "shape", "dims")
        }

        # read the original var name from attrs
        var_name = ds_sparse.attrs.get("__orig_var_name__", None)
        if var_name is None:
            raise KeyError("Dataset is missing the __orig_var_name__ attribute")

        # build the DataArray
        da = xr.DataArray(
            s,
            dims=dims,
            coords=coord_vars,
            name=var_name,
            attrs={k: v for k, v in ds_sparse.attrs.items() if k != "__orig_var_name__"}
        )

        # wrap back into a Dataset
        return da.to_dataset()
       
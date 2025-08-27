from .chelsa import *
from .gbif import *
from datatree import DataTree
from datatree import open_datatree
import xarray as xr
import numpy as np
import sparse
import os

class bmd_cube(chelsa_cube, gbif_cube):
    def __init__(self):
        super().__init__()
        # Contains the names of the individual group which have been requested by the user
        self.group_names = {"static":[], "dynamic":[]}
        #self.group contains the individual layers which have been requested by the user 
        self.groups = {"static":[], "dynamic":[]}
        self.data_tree = None
        self.cube_dir = None
        self.cube_name = None

    def generate_bmd_data(self, param_file, param_path):
        param_filepath = param_file if os.path.isabs(param_file) else os.path.join(param_path, param_file)
        with open(param_filepath) as f:
            param_dict = yaml.safe_load(f)
        self.cube_dir = param_dict["cube_dir"]
        self.cube_name = param_dict["cube_name"]
        if param_dict["layers"]["chelsa"]["enabled"]:
            chelsa_layer_names, chelsa_layer = self.generate_chelsa_cube(param_file, param_path)
            for layer_name, layer in zip(chelsa_layer_names, chelsa_layer):
                if layer_name=="chelsa_month":
                    self.group_names["dynamic"].append(layer_name)
                    self.groups["dynamic"].append(layer)
                else:
                    self.group_names["static"].append(layer_name)
                    self.groups["static"].append(layer)
        if param_dict["layers"]["gbif"]["enabled"]:
            gbif_layer = self.construct_gbif_layer(param_file, param_path)
            self.groups["dynamic"].append(gbif_layer.to_dataset(name="occurrences"))
            self.group_names["dynamic"].append("gbif_occurences")
        return None
    
    def construct_datatree(self) -> str:
        #Assemble a fresh DataTree
        self.data_tree = DataTree(name=self.cube_name)
        if self.groups.get("static"):
            static_nodes = [DataTree(name=nm, data=ds) for nm, ds in 
                            zip(self.group_names["static"], self.groups["static"])]
            self.data_tree["static"] = DataTree(name="static",
                                                children={node.name: node for node in static_nodes})
        if self.groups.get("dynamic"):
            dynamic_nodes = [DataTree(name=nm, data=ds) for nm, ds in 
                             zip(self.group_names["dynamic"], self.groups["dynamic"])]
            self.data_tree["dynamic"] = DataTree(name="dynamic",
                                                 children={node.name: node for node in dynamic_nodes})

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

    def export_tree(self):
        #out_dir = os.path.join(self.cube_dir, self.cube_name)
        os.makedirs(self.cube_dir, exist_ok=True)
        #Make a copy of the datatree which will be written to the disc
        if not self.data_tree:
            raise ValueError("self.data_tree is empty, please use the self.construct_datatree method to build a tree")
        dt_copy = DataTree.copy(self.data_tree)
        #Iterate over the branches in the tree
        for branch in dt_copy:
            #iterate over the different leafs in the tree
            for leaf in dt_copy[branch]:
                #request the name of each data variable and data array
                for name, da, in dt_copy[branch][leaf].ds.data_vars.items():
                    #check if the data array contains data in the sparse representation
                    if isinstance(da.data, sparse.COO):
                        #convert the sparse data to a pseudo dense format that can be saved without complete densification
                        dt_copy[branch][leaf].ds = self.sparse_to_pseudodense(dt_copy[branch][leaf].ds, name)
        dt_copy.to_netcdf(os.path.join(self.cube_dir, f"{self.cube_name}.nc"), format="NETCDF4")
        return None        

    def import_tree(self, cube_dir, cube_name):
        cube_filepath = os.path.join(cube_dir, cube_name)
        self.data_tree = open_datatree(cube_filepath, format="NETCDF4")
        for branch in self.data_tree:
            for leaf in self.data_tree[branch]:
                for attr in self.data_tree[branch][leaf].ds.attrs:
                    if attr=="__orig_var_name__":
                        self.data_tree[branch][leaf].ds = self.load_sparse(self.data_tree[branch][leaf].ds)
        return self.data_tree

    """
    Currently we will focus on using the data tree in the prototype endproduct

    def export_group(self):
        out_dir = os.path.join(self.cube_dir, self.cube_name)
        os.makedirs(out_dir, exist_ok=True)
        #Iterate over both dictionairies in the same way
        for (group, datasets), (group, names) in zip(self.groups.items(), self.group_names.items()):
            group_path = os.path.join(out_dir, group)
            os.makedirs(group_path, exist_ok=True)
            for name, ds in zip(names, datasets):
                file_name = self.cube_name+"_"+name
                file_path = os.path.join(group_path, file_name)
                file_path+=".nc"
                for name, da, in ds.data_vars.items():
                ds.to_netcdf(file_path, mode="w", format="NETCDF4")

    def import_group(self, cube_name, cube_path):
        # Build and validate root folder
        root = os.path.join(cube_path, cube_name)
        if not os.path.isdir(root):
            raise FileNotFoundError(f"No folder at {root!r}")
        group = {}
        group_names = {}
        # Iterate over subfolders (groups)
        for group in sorted(os.listdir(root)):
            gdir = os.path.join(root, group)
            if not os.path.isdir(gdir):
                continue
            group[group] = []
            group_names[group] = []
            # Iterate over files in each group
            for fname in sorted(os.listdir(gdir)):
                base, ext = os.path.splitext(fname)
                prefix = f"{cube_name}_"
                # Skip non-NetCDF or wrong prefix
                if ext.lower() != ".nc" or not base.startswith(prefix):
                    continue
                key = base[len(prefix):]
                full = os.path.join(gdir, fname)
                # Open NetCDF
                ds = xr.open_dataset(full,engine="netcdf4",decode_cf=True,decode_times=True,use_cftime=True)
                group_names[group].append(key)
                group[group].append(ds)
        return group, group_names
    """
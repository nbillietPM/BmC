import rasterio
import xarray as xr
import pandas as pd 
import sparse
import numpy as np

def gbif_sparse_array(dataframe, idx_cols, var_col, add_time=True):
    """
    A function that takes a pandas data frame and extracts the data so that it can be converted into a data array object.
    The sparse representation prevents memory bloating when converting to an actual cube

    Arguments
        dataframe (pd.dataframe): A dataframe that is the result from a GBIF SQL query
        idx_cols (list<str>): A list of the columns through which the table should be indexed. These columns will serve as the dimensions
                                of the data array
        var_col (str): The column which will be used as the data variable in the data array
        add_time (opt, bool): Option to enable adding a year-month datetime column to the dataframe.
    Returns
        data array: A data array with the data in sparse coordinates representation
    """
    
    if add_time:
       dataframe["time"]= pd.to_datetime(dataframe[["year", "month"]].assign(day=1)) 
       idx_cols.append("time")  
    #Remove any rows that do not have entries in the indexation columns
    dataframe = dataframe.dropna(subset=idx_cols)

    #Index the column based on the provided indexation variables
    dataframe_idx = dataframe.set_index(idx_cols)

    #Extract the coordinates for each of the indexation variables
    coords = np.vstack(dataframe_idx.index.codes)

    # Pull out the data
    data = dataframe_idx[var_col].values

    #Select all coordinates that have an code different from -1. Negative code means a missing or placeholder level
    #(coords >= 0) is a (ndim, nnz) boolean array marking which level‐codes are nonnegative
    #.all(axis=0) collapses that to a 1-D mask of length nnz
    valid_mask = (coords >= 0).all(axis=0)
    #keep only fully valid coordinate columns
    coords = coords[:, valid_mask]
    #keep only the matching data values
    data   = data[valid_mask]

    # Compute the shape
    shape = [len(level) for level in dataframe_idx.index.levels]

    sparse_data = sparse.COO(coords, data, shape=shape)

    da = xr.DataArray(
        sparse_data,
        dims=dataframe_idx.index.names,
        coords={name: level for name, level in 
                zip(dataframe_idx.index.names,
                    dataframe_idx.index.levels)}
    )
    return da
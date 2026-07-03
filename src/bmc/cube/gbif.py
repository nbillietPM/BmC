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
       
import geopandas as gpd
from shapely.geometry import box
import logging
from typing import Tuple, Optional

def sanitize_gdf_by_bbox(
    gdf: gpd.GeoDataFrame, 
    sample_bbox: Tuple[float, float, float, float], 
    bbox_crs: str = "EPSG:4326",
    logger: Optional[logging.Logger] = None
) -> gpd.GeoDataFrame:
    """
    Filters out any rows in a GeoDataFrame whose geometries do not intersect 
    the intended sampling bounding box, safely accounting for CRS mismatches.
    
    Parameters:
    -----------
    gdf : geopandas.GeoDataFrame
        The input spatial dataframe to sanitize (e.g., your loaded GBIF cube data).
    sample_bbox : tuple of float
        The bounding box limits defined as (minx, miny, maxx, maxy).
    bbox_crs : str, optional
        The Coordinate Reference System of the input sample_bbox coordinates. 
        Default is "EPSG:4326" (WGS84 decimal degrees).
    logger : logging.Logger, optional
        An optional logger instance to track how many rows were dropped.
        
    Returns:
    --------
    geopandas.GeoDataFrame
        A cleaned copy of the input GeoDataFrame containing only the cells 
        that validly sit within or touch the sampling bounding box.
    """
    if gdf.empty:
        return gdf.copy()
        
    # 1. Represent the bounding box as a formal spatial geometry
    bbox_poly = box(*sample_bbox)
    bbox_gdf = gpd.GeoDataFrame(geometry=[bbox_poly], crs=bbox_crs)
    
    # 2. Match the bounding box CRS to the data's native projection
    # (Reprojecting the 1-row box is computationally instant compared to reprojecting the entire gdf)
    if gdf.crs != bbox_gdf.crs:
        if logger:
            logger.info(f"🔄 Aligning sampling box CRS from {bbox_crs} to match data CRS ({gdf.crs})...")
        bbox_aligned = bbox_gdf.to_crs(gdf.crs)
    else:
        bbox_aligned = bbox_gdf.copy()
        
    # Extract the reprojected polygon shape for the mask evaluation
    target_bbox_shape = bbox_aligned.geometry.values[0]
    
    # 3. Create a spatial mask using an intersection predicate
    # This evaluates whether any part of the cell geometry touches or falls inside the box
    spatial_mask = gdf.geometry.intersects(target_bbox_shape)
    
    # 4. Filter the data
    gdf_cleaned = gdf[spatial_mask].copy()
    
    # 5. Log execution metrics for validation tracking
    dropped_count = len(gdf) - len(gdf_cleaned)
    if logger:
        logger.info(
            f"🧹 Sanitization Complete: Retained {len(gdf_cleaned)} cells. "
            f"Filtered out {dropped_count} outlier cells that drifted outside the sampling footprint."
        )
    else:
        print(f"🧹 Sanitized: Kept {len(gdf_cleaned)} rows, dropped {dropped_count} outliers.")
        
    return gdf_cleaned

import math
import numpy as np
import xarray as xr
import rioxarray
from pyproj import CRS

def create_aligned_raster_template(sample_bbox, grid_name, registry=spatial_engine.GRID_REGISTRY):
    """
    Takes a bounding box and generates an empty xarray DataArray that perfectly 
    aligns with the specified master grid from the registry. 
    
    Bakes in the CRS, grid registry key, resolution, and native spatial units.
    """
    master = registry[grid_name]
    res = master["resolution"]
    master_minx, master_miny, master_maxx, master_maxy = master["bounds"]
    
    # sample_bbox is (minx, miny, maxx, maxy)
    s_minx, s_miny, s_maxx, s_maxy = sample_bbox
    
    # 1. Snap strictly to the Master Grid intervals
    aligned_minx = master_minx + math.floor((s_minx - master_minx) / res) * res
    aligned_miny = master_miny + math.floor((s_miny - master_miny) / res) * res
    aligned_maxx = master_minx + math.ceil((s_maxx - master_minx) / res) * res
    aligned_maxy = master_miny + math.ceil((s_maxy - master_miny) / res) * res
    
    # 2. Calculate integer dimensions safely
    width = int(round((aligned_maxx - aligned_minx) / res))
    height = int(round((aligned_maxy - aligned_miny) / res))
    
    # 3. Generate spatial coordinates (Pixel Centers)
    x_coords = aligned_minx + (np.arange(width) + 0.5) * res
    y_coords = aligned_maxy - (np.arange(height) + 0.5) * res
    
    # 4. Dynamically determine spatial units from the CRS
    crs_obj = CRS.from_string(master["crs"])
    spatial_unit = "degrees" if crs_obj.is_geographic else "meters"
    
    # 5. Create the DataArray template with robust metadata attributes
    template = xr.DataArray(
        data=np.zeros((height, width), dtype=np.int32), 
        coords={"y": y_coords, "x": x_coords},
        dims=("y", "x"),
        attrs={
            "grid_registry_key": grid_name,
            "res": res,
            "spatial_unit": spatial_unit
        }
    )
    
    # 6. Inject CF-compliant spatial topology FIRST (Creates 'spatial_ref')
    template = template.rio.write_crs(master["crs"])
    
    # 7. MANUALLY FORCE IT IN LAST: Assign the text attribute *after* rioxarray finishes its cleanup
    template.attrs["crs"] = str(master["crs"])
    
    return template, (aligned_minx, aligned_miny, aligned_maxx, aligned_maxy)

import numpy as np
import pandas as pd
import geopandas as gpd
from shapely.geometry import box
from scipy.spatial import KDTree

def transform_gdf_to_template(
    source_gdf, 
    target_template, 
    value_column, 
    data_type='discrete', 
    method='kdtree'
):
    """
    Transforms data from a source GeoDataFrame to match an xarray target template.
    Safely preserves all original attribute columns (species, year, etc.).
    
    Parameters:
    -----------
    source_gdf : geopandas.GeoDataFrame
        The input data to transform.
    target_template : xarray.DataArray
        The aligned empty template containing metadata attributes ('crs', 'res') and coordinates.
    value_column : str
        The name of the numeric column to aggregate.
    data_type : str
        'discrete' (sum whole integers) or 'continuous' (areal weighting).
    method : str
        'kdtree' (nearest neighbor snapping, perfect mass conservation, discrete only)
        'intersect' (geometric clipping, strict boundary adherence, discrete or continuous)
    """
    # 1. Extract metadata dynamically from the xarray template
    target_crs = target_template.attrs.get('crs')
    res = target_template.attrs.get('res')
    
    if target_crs is None or res is None:
        raise ValueError("Target template must have 'crs' and 'res' in its attributes.")

    x_centers = target_template.x.values
    y_centers = target_template.y.values

    # 2. Build the target grid mesh (Row-major order matching xarray)
    polygons = []
    grid_ids = []
    idx = 0
    for yi in y_centers:
        for xi in x_centers:
            polygons.append(box(
                xi - (res / 2), 
                yi - (res / 2), 
                xi + (res / 2), 
                yi + (res / 2)
            ))
            grid_ids.append(idx)
            idx += 1
            
    target_grid_gdf = gpd.GeoDataFrame({'grid_id': grid_ids}, geometry=polygons, crs=target_crs)

    # 3. Handle Auto-Reprojection of the Source Data
    if source_gdf.crs != target_crs:
        source_df = source_gdf.to_crs(target_crs)
    else:
        source_df = source_gdf.copy()

    # 4. Prepare Source Data for Aggregation
    source_df['src_uid'] = source_df.index 
    source_df['source_area'] = source_df.geometry.area
    
    # Identify columns to carry over (ignore temp/geometry columns)
    ignore_cols = ['geometry', value_column, 'src_uid', 'source_area']
    preserve_cols = [col for col in source_df.columns if col not in ignore_cols]
    
    # Columns used to group the data without flattening taxonomy or time
    group_cols = ['grid_id'] + preserve_cols

    # ==========================================
    # STRATEGY A: KDTREE (Point Distance Snapping)
    # ==========================================
    if method == 'kdtree':
        if data_type != 'discrete':
            raise ValueError("KDTree routing mathematically requires 'discrete' data_type.")
            
        src_centroids = source_df.geometry.centroid
        src_coords = np.column_stack([src_centroids.x, src_centroids.y])
        tgt_coords = np.array([[xi, yi] for yi in y_centers for xi in x_centers])
        
        # Snap every source cell to the nearest target pixel center
        tree = KDTree(tgt_coords)
        _, matched_grid_ids = tree.query(src_coords)
        source_df['grid_id'] = matched_grid_ids
        
        # Group by target cell AND original attributes, summing the whole integer occurrences
        aggregated = source_df.groupby(group_cols)[value_column].sum().reset_index()

    # ==========================================
    # STRATEGY B: INTERSECT (Geometric Overlay)
    # ==========================================
    elif method == 'intersect':
        intersections = gpd.overlay(source_df, target_grid_gdf, how='intersection')
        
        if intersections.empty:
            return target_grid_gdf.iloc[0:0].copy()
            
        intersections['intersect_area'] = intersections.geometry.area

        if data_type == 'continuous':
            # Areal Weighting
            intersections['weight'] = intersections['intersect_area'] / intersections['source_area']
            intersections['weighted_val'] = intersections[value_column] * intersections['weight']
            
            aggregated = intersections.groupby(group_cols)['weighted_val'].sum().reset_index()
            aggregated.rename(columns={'weighted_val': value_column}, inplace=True)

        elif data_type == 'discrete':
            # Majority Area Rule
            idx_max = intersections.sort_values('intersect_area', ascending=False).groupby('src_uid').head(1)
            aggregated = idx_max.groupby(group_cols)[value_column].sum().reset_index()
        else:
            raise ValueError("Invalid data_type for intersect. Choose 'continuous' or 'discrete'.")
            
    else:
        raise ValueError("Invalid method. Choose 'kdtree' or 'intersect'.")

    # 5. Re-attach the pristine target grid geometries to our aggregated attribute table
    target_geometries = target_grid_gdf[['grid_id', 'geometry']]
    result_grid = target_geometries.merge(aggregated, on='grid_id', how='inner')
    
    return result_grid

import numpy as np

def check_grid_alignment(gdf, raster, resolution=250):
    """
    Checks if a GeoDataFrame's polygons align perfectly with a raster's grid.
    """
    print("--- Alignment Diagnostic Report ---")
    
    # 1. Check Polygon Drift
    # Extract the lower-left coordinates (minx, miny) for all polygons
    bounds = gdf.bounds
    poly_minx = bounds['minx'].values
    poly_miny = bounds['miny'].values
    
    # Calculate how far off the polygons are from a pure 250m interval
    x_drift = np.abs(poly_minx % resolution)
    y_drift = np.abs(poly_miny % resolution)
    
    # Account for modulo wrap-around (e.g., a drift of 249.999 is actually a drift of -0.001)
    x_drift = np.minimum(x_drift, resolution - x_drift)
    y_drift = np.minimum(y_drift, resolution - y_drift)
    
    max_poly_x_drift = np.max(x_drift)
    max_poly_y_drift = np.max(y_drift)
    
    if np.isclose(max_poly_x_drift, 0, atol=1e-6) and np.isclose(max_poly_y_drift, 0, atol=1e-6):
        print("✅ Polygons: Perfectly snapped to the 250m grid (No drift detected).")
    else:
        print(f"❌ Polygons: Drift detected! Max X drift: {max_poly_x_drift}m, Max Y drift: {max_poly_y_drift}m")

    # 2. Check Raster Grid Drift
    # Extract the first x and y coordinates from the raster
    rx = raster.x.values[0]
    ry = raster.y.values[0]
    
    # Determine if raster coords represent edges (remainder 0) or pixel centers (remainder 125)
    rx_rem = rx % resolution
    
    if np.isclose(rx_rem, 0, atol=1e-6):
        print("✅ Raster: Coordinates represent pixel EDGES and are aligned.")
        raster_is_aligned = True
    elif np.isclose(rx_rem, resolution / 2, atol=1e-6):
        print("✅ Raster: Coordinates represent pixel CENTERS and are aligned.")
        raster_is_aligned = True
    else:
        print(f"❌ Raster: Sub-pixel drift detected in the template raster! Offset: {rx_rem}m")
        raster_is_aligned = False
        
    # 3. Final Conclusion
    if np.isclose(max_poly_x_drift, 0, atol=1e-6) and raster_is_aligned:
        print("\n🏆 CONCLUSION: 1-to-1 Match Confirmed. No drift between datasets.")
    else:
        print("\n⚠️ CONCLUSION: Misalignment detected. Do not proceed with rasterization without fixing the grid origins.")


import numpy as np
import pandas as pd
import geopandas as gpd
from shapely.geometry import Point
import ot  # Python Optimal Transport (pip install POT)

def compute_spatial_fidelity(
    gdf_original: gpd.GeoDataFrame, 
    gdf_target: gpd.GeoDataFrame, 
    value_col: str = 'occurrences',
    group_cols: list = ['scientificname', 'year', 'month'],
    shared_crs: str = "EPSG:3035"
) -> pd.DataFrame:
    """
    Computes Center of Mass shift and Wasserstein (Earth Mover's) Distance 
    between two GeoDataFrames, stratified by taxonomy and time.
    """
    # 1. Ensure both dataframes are in a shared metric projection (meters)
    orig_proj = gdf_original.to_crs(shared_crs).copy()
    targ_proj = gdf_target.to_crs(shared_crs).copy()
    
    # Reset index if the group columns are currently trapped in the index
    if orig_proj.index.name in group_cols or orig_proj.index.names[0] in group_cols:
        orig_proj = orig_proj.reset_index()
    if targ_proj.index.name in group_cols or targ_proj.index.names[0] in group_cols:
        targ_proj = targ_proj.reset_index()
        
    # Extract Centroids
    orig_proj['cx'] = orig_proj.geometry.centroid.x
    orig_proj['cy'] = orig_proj.geometry.centroid.y
    targ_proj['cx'] = targ_proj.geometry.centroid.x
    targ_proj['cy'] = targ_proj.geometry.centroid.y
    
    results = []
    
    # 2. Iterate through each unique Stratum (Species + Year + Month)
    groups = orig_proj.groupby(group_cols)
    
    for group_keys, orig_group in groups:
        # Build dictionary for dynamic group keys unpacking
        group_dict = dict(zip(group_cols, group_keys if isinstance(group_keys, tuple) else [group_keys]))
        
        # Find matching group in target dataset
        query_mask = np.ones(len(targ_proj), dtype=bool)
        for col, val in group_dict.items():
            query_mask &= (targ_proj[col] == val)
            
        targ_group = targ_proj[query_mask]
        
        # Skip if target group is missing or empty
        if targ_group.empty or orig_group[value_col].sum() == 0 or targ_group[value_col].sum() == 0:
            continue
            
        # ==========================================
        # METRIC 1: Center of Mass (Weighted Mean)
        # ==========================================
        w_orig = orig_group[value_col].values
        w_targ = targ_group[value_col].values
        
        orig_com_x = np.average(orig_group['cx'].values, weights=w_orig)
        orig_com_y = np.average(orig_group['cy'].values, weights=w_orig)
        
        targ_com_x = np.average(targ_group['cx'].values, weights=w_targ)
        targ_com_y = np.average(targ_group['cy'].values, weights=w_targ)
        
        # Euclidean distance between the two Centers of Mass (in meters)
        com_shift_meters = np.sqrt((targ_com_x - orig_com_x)**2 + (targ_com_y - orig_com_y)**2)
        
        # ==========================================
        # METRIC 2: Wasserstein Metric (EMD)
        # ==========================================
        coords_orig = np.column_stack((orig_group['cx'], orig_group['cy']))
        coords_targ = np.column_stack((targ_group['cx'], targ_group['cy']))
        
        # Normalize weights so they sum to 1 (Required for Optimal Transport probabilities)
        p_orig = w_orig / w_orig.sum()
        p_targ = w_targ / w_targ.sum()
        
        # Calculate the Euclidean distance cost matrix between all points
        cost_matrix = ot.dist(coords_orig, coords_targ, metric='euclidean')
        
        # Compute exact Earth Mover's Distance
        emd_meters = ot.emd2(p_orig, p_targ, cost_matrix)
        
        # Store results
        results.append({
            **group_dict,
            'occurrences_orig': w_orig.sum(),
            'occurrences_targ': w_targ.sum(),
            'com_shift_meters': round(com_shift_meters, 2),
            'wasserstein_meters': round(emd_meters, 2)
        })
        
    return pd.DataFrame(results)
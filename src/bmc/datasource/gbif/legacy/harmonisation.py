import os
import re
import pandas as pd
import geopandas as gpd
import xarray as xr
import regionmask
from geocube.api.core import make_geocube
from geocube.rasterize import rasterize_image
from rasterio.enums import MergeAlg
from functools import partial
import rioxarray
from shapely.geometry import box
import pyproj
from shapely.ops import transform


def parse_to_polygon(code):
    """
    Use a cell code to construct a geospatial polygon. The function dynamically scrapes the E and N coordinates + resolution 
    and constructs the corresponding bounding box
    """
    # ^ matches the start of the string
    # (\d+) a series of digits
    # (km|m) a substring matching either km or m
    match = re.match(r'^(\d+)(km|m)E(\d+)N(\d+)$', code)
    if not match:
        raise ValueError(f"Invalid EEA grid code format: {code}")
      
    res_val, res_unit, easting_code, northing_code = match.groups()
    #Convert resolution to meters
    cell_size_m = int(res_val) * 1000 if res_unit == 'km' else int(res_val)

    # Coordinates are 7 digits and trailing zeros are removed from the cell code to make them more compact
    # Calculate the multiplier based on trailing zeros
    # Convert cell size to string, strip non-zeros, and count difference
    str_size = str(cell_size_m)
    trailing_zeros = len(str_size) - len(str_size.rstrip('0'))
    multiplier = 10 ** trailing_zeros
        
    # 4. Calculate Lower-Left coordinates
    ll_easting = int(easting_code) * multiplier
    ll_northing = int(northing_code) * multiplier
    ur_easting = ll_easting + cell_size_m
    ur_northing = ll_northing + cell_size_m
    (ll_easting, ll_northing, ur_easting, ur_northing)
    
    return box(ll_easting, ll_northing, ur_easting, ur_northing)


def ds_zonal_statistics_table(gbif_df, env_ds, out_name, out_path="", cellcode_colname="eeacellcode"):
    """
    Function that ingests a environmental xarray Dataset and computes the zonal average associated with the
    EEA cell defined within a GBIF dataframe.

    Args:
        param gbif_df (pd.Dataframe): A GBIF data table that was generated using the SQL cubing functionality. The data table contains EEA cells
        param env_ds (xarray.Dataset): A xarray dataset that contains environmental data. The xarray dataset must have a CRS associated with it
        param out_name (str): The output name of the zonal average table for the sampled EEA cells

    Returns:
        env_df (pd.Dataframe): A data frame containing the zonal averages for each of the EEA cells for the different timesteps
    """
    # Built in check to assure that environmental dataset has a CRS to which the polygons can be projected
    assert env_ds.rio.crs!=None, "The environmental Dataset does not have a CRS in its metadata"

    # Apply the parse polygon function to the eeacellcode column to generate a geometry column
    gbif_df['geometry'] = gbif_df['eeacellcode'].apply(parse_to_polygon)

    # Convert dataframe to geopandas dataframe. We explicitly define the CRS as EPSG:3035 (ETRS89-LAEA=EEA grid)
    gbif_gdf = gpd.GeoDataFrame(gbif_df, geometry='geometry', crs="EPSG:3035")

    #Convert geometry coordinates to CRS of the xarray dataset
    gbif_gdf = gbif_gdf.to_crs(env_ds.rio.crs)

    # Retrieve all the unique eeacell codes from the geodataframe and reset the index, i.e. reset the pandas index
    unique_eea = gbif_gdf.drop_duplicates(subset='eeacellcode').reset_index(drop=True)

    # Generate regions that correspond to the EEA cells
    # Region mask requires a geodataframe that contains polygons and is indexed based on the cell code columnest
    regions = regionmask.from_geopandas(unique_eea,names="eeacellcode")

    # Mask out all the values that fall into the defined regions in the region from the original xr Dataset
    mask = regions.mask(env_ds).rename("region")

    # Group values based on region indices and aggregate them as a mean
    env_stats = env_ds.groupby(mask).mean()

    # Convert the region ids to integers
    valid_ids = env_stats['region'].values.astype(int)
    
    # Map the region IDs to the corresponding EEA cell codes
    original_codes = regions[valid_ids].names
    
    # Transform the region coordinate to the EEA cell codes
    env_stats = env_stats.assign_coords(region=original_codes)
    env_stats = env_stats.rename({'region': 'eeacellcode'})
    
    # Export the EO/climate dataset to a pandas data frame (tabular format conversion)
    env_df = env_stats.to_dataframe().reset_index()
    env_df.to_csv(os.path.join(out_path, out_name), sep=",")
    
    return env_df


def rasterize_gbif_full(df, target_ds, tolerance=2.0):
    """
    Converts GBIF occurrences data to a raster aligned with the spatial grid of target_ds,
    but retains the full temporal extent (all years/months) present in the GBIF data.

    Tolerance has been set to 2.0 to take into account that the EEA grid is planar vs the spherical chelsa grid that was used
    """
    # Built in check to assure that environmental dataset has a CRS to which the polygons can be projected
    assert env_ds.rio.crs!=None, "The environmental Dataset does not have a CRS in its metadata"

    # Apply the parse polygon function to the eeacellcode column to generate a geometry column
    gbif_df['geometry'] = df['eeacellcode'].apply(parse_to_polygon)

    # Transform to geopandas dataframe to ease geospatial operations
    gdf = gpd.GeoDataFrame(gbif_df, geometry='geometry')

    # GBIF data is described in EEA cell codes which correspond to CRS 3035
    if gdf.crs is None:
        gdf.set_crs("EPSG:3035", inplace=True)

    # Geocube operates on general x and y dimension for spatial coordinates. Check if spatial dimensions are named differently
    # In case they are named differently add spatial dimensions for x and y
    if {"lat", "lon"}.issubset(target_ds.coords):
        target_ds.rio.set_spatial_dims(x_dim="lon", y_dim="lat", inplace=True)

    # Unify the month and year column present in the GBIF data to a datetime format, required for xarray temporal slicing
    if 'month' in gdf.columns and 'year' in gdf.columns:
        gdf['time'] = pd.to_datetime(gdf[['year', 'month']].assign(day=1))
    
    # Define the Global Equal area CRS system
    equal_area_crs = "EPSG:6933"

    # Transform the geopandas dataframe to the global equal area CRS
    gdf_ea = gdf.to_crs(equal_area_crs)

    # Calculate the median area of the cells through the geometry column
    # Calculation of the median is preferred to minimize the effect of statistical outliers
    gbif_area_sqm = gdf_ea.geometry.area.median()

    # Extract the bounds from the environmental dataset
    minx, miny, maxx, maxy = target_ds.rio.bounds()
    
    # Generate coordinates for a cell in the center of the bbox 
    cx, cy = (minx + maxx) / 2.0, (miny + maxy) / 2.0

    # Retrieve the resolution of the raster 
    target_x_res, target_y_res = map(abs, target_ds.rio.resolution())

    """
    Single pixel area is chose to prevent excessive computation for area statistics. For a raster of size (W x H)
    we would have to compute the area for each of the pixels present in the raster itself. The central pixel thus serves
    as a proxy or representative of all the pixels present in the raster itself. 
    """
    
    # Construct an artificial pixel in the center of the raster
    pixel_geom = box(cx - target_x_res/2, cy - target_y_res/2, cx + target_x_res/2, cy + target_y_res/2)
    # Create a transformer from your target CRS (e.g., EPSG:4326) to EPSG:6933
    # always_xy=True is critical to prevent x/y coordinates from flipping
    project = pyproj.Transformer.from_crs(
        target_ds.rio.crs, 
        equal_area_crs, 
        always_xy=True
    ).transform
    
    # Transform the single Shapely box directly
    pixel_geom_ea = transform(project, pixel_geom)
    
    # Get the area of the projected shape
    target_area_sqm = pixel_geom_ea.area

    # Compare the area of the median GBIF cellcode area to the central pixel 
    if gbif_area_sqm > (target_area_sqm * tolerance):
        raise ValueError(
            f"RESOLUTION MISMATCH ERROR: Vector area (~{gbif_area_sqm:,.0f} sqm) "
            f"exceeds Raster area (~{target_area_sqm:,.0f} sqm)."
        )

    # In order to assure that EEA cells are assigned to a single cell we utilize the cell centroid in order to decide in which raster cell the occurrences happen
    gdf['geometry'] = gdf.geometry.centroid

    # Transform the GBIF CRS to the target_ds CRS
    gdf = gdf.to_crs(target_ds.rio.crs)

    """
    make_geocube rasterizes vector data in which the target_ds is used as a spatial template
        - Measurements designates which column becomes the data variable in the xarray dataset
        - group_by allows us to introduce a temporal dimension
        - functools.partial fixes one of the arguments of a function
        - rasterize_image rasterizes a list of shapes+values for a given GeoBox. The merge_alg is fixed to add up overlapping values
            ! standard rasterization just keeps the last point drawn (overwriting the first), in order to conserve total number of occurrences
              we need to specify addition in case 2 eea cells are written to the same raster cell !
    """
    rasterized_ds = make_geocube(
        vector_data=gdf,
        measurements=['occurrences'], 
        like=target_ds,
        group_by='time',
        fill=0,
        rasterize_function=partial(rasterize_image, merge_alg=MergeAlg.add)
    )
    
    # We deliberately DO NOT reindex to target_ds.time here. reindex interpolates and fills in with NaN
    # We return this as a standalone dataset so the timeline remains untouched.
    return rasterized_ds
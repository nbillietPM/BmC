import geopandas as gpd
import pandas as pd
import numpy as np
from shapely.geometry import Point, box
import rasterio
from rasterio.features import rasterize
from rasterio.transform import from_origin

# --- Load occurrence data ---
dataOcc = "./data/0015395-250515123054153/occurrence.txt"
essential_cols = ["decimalLongitude", "decimalLatitude", "speciesKey", "eventDate"]

df = pd.read_csv(dataOcc, usecols=essential_cols, parse_dates=["eventDate"], sep="\t")
df.dropna(subset=["decimalLongitude", "decimalLatitude", "speciesKey"], inplace=True)

# Create GeoDataFrame
df["geometry"] = [Point(xy) for xy in zip(df.decimalLongitude, df.decimalLatitude)]
gdf = gpd.GeoDataFrame(df, geometry="geometry", crs="EPSG:4326")
gdf = gdf.to_crs("EPSG:3035")  # Use metric projection

# --- Create 10x10 km grid ---
minx, miny, maxx, maxy = gdf.total_bounds
cell_size = 10000
width = int((maxx - minx) / cell_size)
height = int((maxy - miny) / cell_size)
transform = from_origin(minx, maxy, cell_size, cell_size)

grid_cells = [
    box(x0, y0, x0 + cell_size, y0 + cell_size)
    for x0 in np.arange(minx, maxx, cell_size)
    for y0 in np.arange(miny, maxy, cell_size)
]

grid = gpd.GeoDataFrame({"geometry": grid_cells}, crs="EPSG:3035")
grid["grid_id"] = grid.index

# --- Spatial join: assign points to grid cells ---
joined = gpd.sjoin(gdf, grid, how="inner", predicate="within")

# --- Aggregate total counts per grid cell per species ---
agg = (
    joined.groupby(["grid_id", "speciesKey"])
    .size()
    .reset_index(name="occurrence_count")
)

# Merge grid geometry back into aggregated results
agg = pd.merge(agg, grid[["grid_id", "geometry"]], on="grid_id")
agg = gpd.GeoDataFrame(agg, geometry="geometry", crs="EPSG:3035")

# --- Prepare raster ---
species_keys = agg["speciesKey"].unique()
species_keys.sort()  # Optional: consistent band order
num_bands = len(species_keys)
raster_data = np.zeros((num_bands, height, width), dtype=np.uint16)

# --- Rasterize each species band ---
for i, species in enumerate(species_keys):
    species_geom = agg[agg["speciesKey"] == species]
    shapes = zip(species_geom.geometry, species_geom["occurrence_count"])
    band = rasterize(
        shapes=shapes,
        out_shape=(height, width),
        transform=transform,
        fill=0,
        dtype="uint16"
    )
    raster_data[i] = band

# --- Write output to GeoTIFF ---
output_path = "species_occurrences_total.tif"
with rasterio.open(
    output_path,
    "w",
    driver="GTiff",
    height=height,
    width=width,
    count=num_bands,
    dtype="uint16",
    crs=agg.crs,
    transform=transform,
) as dst:
    for i in range(num_bands):
        dst.write(raster_data[i], i + 1)
        dst.set_band_description(i + 1, str(species_keys[i]))

print(f"✅ Raster written with {num_bands} bands to {output_path}")

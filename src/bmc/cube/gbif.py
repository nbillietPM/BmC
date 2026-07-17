from bmc.cube.spatiotemporal import *
import yaml
import os
import pandas as pd
from bmc.datasource.gbif import sql
import re
from shapely.geometry import box

class gbif_cube(spatiotemporal_vector_cube):

    def generate_gbif_query_from_recipe(self, recipe: dict) -> str:
        """
        Parses a configuration recipe dictionary and generates a validated GBIF SQL query.

        Translates bounding box coordinates into WKT polygons, resolves taxonomic 
        column hierarchies, parses spatial resolutions into metric integers, maps 
        taxonomic keys to optimal GBIF/CoL SQL columns, and structures custom YAML 
        parameters into strictly formatted GBIF SQL clauses.
        """
        # 1. Spatial extraction and WKT conversion
        spatial_cfg = recipe.get("spatial", {})
        bbox_cfg = spatial_cfg.get("bbox", {})
        bbox_tuple = (
            bbox_cfg.get("long_min"),
            bbox_cfg.get("lat_min"),
            bbox_cfg.get("long_max"),
            bbox_cfg.get("lat_max")
        )
        wkt_polygon = sql.bbox2polygon_wkt(bbox_tuple)

        # 2. Taxonomic & Standard Column Resolution (RESTORED TO ORIGINAL)
        gbif_cfg = recipe.get("sources", {}).get("gbif", {})
        taxonomy_cfg = gbif_cfg.get("taxonomy", {})
        
        target_level = taxonomy_cfg.get("lowest_level")
        columns = sql.resolve_taxonomic_columns(target_level) if target_level else ["speciesKey"]
        
        # Ensure standard temporal columns are always included
        for std_col in ["year", "month"]:
            if std_col not in columns:
                columns.append(std_col)

        # 3. Read Catalogue of Life (CoL) Identifiers directly from recipe
        col_backbone = taxonomy_cfg.get("col_backbone", False)
        col_uuid = taxonomy_cfg.get("col_uuid", "7ddf754f-d193-4cc9-b351-99906754a03b")

        # 4. Temporal formatting
        temporal_cfg = recipe.get("temporal", {})
        year_range = [temporal_cfg.get("start_year"), temporal_cfg.get("end_year")]
        month_range = [temporal_cfg.get("start_month"), temporal_cfg.get("end_month")]

        # 5. Map record type to GBIF SQL enum ("presence" -> "occurrence")
        yaml_record_type = gbif_cfg.get("query_filters", {}).get("record_type", "presence")
        record_type = "occurrence" if yaml_record_type == "presence" else yaml_record_type

        # 6. Master Grid Resolution Parsing
        target_grid = spatial_cfg.get("target_grid")
        res_str = str(spatial_cfg.get("target_resolution", "")).lower().strip()
        
        if "km" in res_str:
            grid_res_meters = int(float(res_str.replace("km", "")) * 1000)
        elif "m" in res_str:
            grid_res_meters = int(float(res_str.replace("m", "")))
        else:
            if "sec" in res_str or "min" in res_str:
                geo_mapping = {"0_3sec": 10, "3sec": 100, "7_5sec": 250, "15sec": 500, "30sec": 1000, "5min": 10000}
                grid_res_meters = geo_mapping.get(res_str, 1000)
            else:
                try:
                    grid_res_meters = int(res_str)
                except ValueError:
                    grid_res_meters = 1000

        # 7. Query Mode Routing: Cubed vs Raw Points (Solely controlled by aggregate block)
        aggregate_cfg = gbif_cfg.get("aggregate", {})
        aggregate_flag = aggregate_cfg.get("return_count", False)

        if aggregate_flag:
            query_grid = target_grid
            query_res = grid_res_meters
            distinct_obs_flag = aggregate_cfg.get("return_nmbObservers", False)
        else:
            query_grid = False
            query_res = None
            distinct_obs_flag = False

        # 8. Uncertainty handling
        query_filters = gbif_cfg.get("query_filters", {})
        default_uncert = query_filters.get("default_Uncertainty", 1000)
        max_uncertainty = query_filters.get("max_uncertainty", "auto")
        
        if not aggregate_flag and gbif_cfg.get("return_raw", {}).get("coordinateUncertainty"):
            max_uncertainty = gbif_cfg.get("return_raw", {}).get("coordinateUncertainty")

        # 9. Dynamically build issue exclusion flags
        issue_flags = ["hasCoordinate = TRUE"]
        excluded_issues = query_filters.get("exclude_issues", [])
        if excluded_issues:
            for issue in excluded_issues:
                issue_flags.append(f"NOT GBIF_STRINGARRAYCONTAINS(occurrence.issue, '{issue}', TRUE)")

        # 10. Map Taxonomic Keys to Optimized SQL Columns
        raw_taxon_keys = query_filters.get("taxon_keys", [])
        if raw_taxon_keys:
            print(f"Mapping {len(raw_taxon_keys)} taxonomic keys to SQL columns...")
            mapped_taxon_keys = sql.map_taxonkeys_to_columns(
                taxon_keys=raw_taxon_keys,
                col_backbone=col_backbone,
                col_uuid=col_uuid
            )
        else:
            mapped_taxon_keys = []

        # 11. Dispatch to the SQL generator function
        gbif_sql_query = sql.generate_query(
            taxonKeys=mapped_taxon_keys,
            columns=columns,               
            record_type=record_type,
            wkt_polygon=wkt_polygon,
            year_range=year_range,
            month_range=month_range,
            aggregate=aggregate_flag,                 
            include_distinct_observers=distinct_obs_flag, 
            grid=query_grid,                          
            grid_resolution=query_res,     
            coordinateUncertainty=default_uncert, 
            max_uncertainty=max_uncertainty,      
            issue_flags=issue_flags,
            col_backbone=col_backbone,
            col_uuid=col_uuid
        )

        return gbif_sql_query

    def _mine_crs_from_grid(self, df: pd.DataFrame, logger: Optional[logging.Logger] = None) -> str:
        """
        Inspects the DataFrame for a GBIF grid cell code column and mines the native CRS.
        Maps explicit strings (CRS3035, CRS4326) and implicit grid patterns (EEA, EQDG).
        """
        # Find the column dynamically generated by the SQL formatter (e.g., 'eeacellcode', 'dmsgcellcode')
        cell_col = next((col for col in df.columns if 'cellcode' in col.lower()), None)
        
        if not cell_col or df[cell_col].dropna().empty:
            log_execution(logger, "No grid cell codes found. Defaulting to EPSG:4326.", logging.DEBUG)
            return "EPSG:4326"
            
        # Sample the first valid cell code
        sample_code = str(df[cell_col].dropna().iloc[0]).upper()
        log_execution(logger, f"Mining native CRS from grid cell code: {sample_code}", logging.INFO)
        
        # 1. Explicit CRS strings (Eurostat & DMSG Grids)
        if sample_code.startswith("CRS3035"):
            return "EPSG:3035"
        if sample_code.startswith("CRS4326"):
            return "EPSG:4326"
            
        # 2. Implicit EEA Grid (EPSG:3035)
        # Matches formats like '100KME51N29' or '250ME510500N293350'
        if "KME" in sample_code or "ME" in sample_code:
            return "EPSG:3035"
            
        # 3. Implicit Extended Quarter-Degree Grid / EQDG (EPSG:4326)
        # Matches formats like 'W175S21' or 'E010N52BDB'
        if (sample_code.startswith('W') or sample_code.startswith('E')) and ('S' in sample_code or 'N' in sample_code):
            return "EPSG:4326"
            
        # Fallback if ISEA3H or MGRS is used
        log_execution(logger, f"Unrecognized CRS pattern in code {sample_code}. Defaulting to EPSG:4326.", logging.WARNING)
        return "EPSG:4326"

    def _parse_cellcode_to_polygon(self, code: str):
        """
        Dynamically parses GBIF grid cell codes into Shapely Polygons.
        Supports implicit EEA formats and explicit Eurostat/DMSG metric formats.
        Returns None if the code is invalid or unsupported.
        """
        if pd.isna(code):
            return None
            
        code = str(code).upper().strip()
        
        try:
            # 1. EEA Grid (e.g., '100KME51N29' or '250ME510500N293350')
            eea_match = re.match(r'^(\d+)(KM|M)E(\d+)N(\d+)$', code)
            if eea_match:
                res_val, res_unit, easting_code, northing_code = eea_match.groups()
                
                # Convert resolution to meters
                cell_size_m = int(res_val) * 1000 if res_unit == 'KM' else int(res_val)
                
                # Calculate the multiplier based on trailing zeros
                str_size = str(cell_size_m)
                trailing_zeros = len(str_size) - len(str_size.rstrip('0'))
                multiplier = 10 ** trailing_zeros
                
                # Calculate coordinates
                ll_easting = int(easting_code) * multiplier
                ll_northing = int(northing_code) * multiplier
                ur_easting = ll_easting + cell_size_m
                ur_northing = ll_northing + cell_size_m
                
                return box(ll_easting, ll_northing, ur_easting, ur_northing)
                
            # 2. Eurostat / DMSG Metric Grid (e.g., 'CRS3035RES10000MN2480000E4850000')
            euro_match = re.match(r'^CRS(\d+)RES(\d+)MN(\d+)E(\d+)$', code)
            if euro_match:
                epsg, res_m, northing, easting = euro_match.groups()
                
                cell_size_m = int(res_m)
                ll_easting = int(easting)
                ll_northing = int(northing)
                
                return box(ll_easting, ll_northing, ll_easting + cell_size_m, ll_northing + cell_size_m)
                
            # Note: Complex angular grids (like EQDG 'W175S21') are skipped 
            # and will drop out gracefully, as they cannot be perfectly boxed in planar meters.
            return None
            
        except Exception:
            return None

    def resolve_target_grid(self, spatial_cfg: Dict[str, Any], logger: Optional[logging.Logger] = None) -> str:
        """
        Translates user-defined spatial configurations into a validated master grid key
        that explicitly matches the parent class's GRID_REGISTRY format.
        """
        grid_base = spatial_cfg.get("target_grid")
        resolution = spatial_cfg.get("target_resolution")
        
        if not grid_base:
            log_execution(logger, "No 'target_grid' specified in recipe. Defaulting to Global_WGS84_30sec.", logging.WARNING)
            return "Global_WGS84_30sec"
            
        # 1. Check if the user already provided the full exact key (e.g., "EEA_1km")
        if grid_base in self.GRID_REGISTRY:
            log_execution(logger, f"GBIF strictly adhering to recipe master grid: {grid_base}", logging.INFO)
            return grid_base
            
        # 2. If they provided base and resolution separately, construct the key
        if resolution:
            constructed_key = f"{grid_base}_{resolution}"
            if constructed_key in self.GRID_REGISTRY:
                log_execution(logger, f"Constructed and resolved master grid key: {constructed_key}", logging.INFO)
                return constructed_key
                
        # 3. Fail gracefully if the grid doesn't exist in the registry
        valid_keys = list(self.GRID_REGISTRY.keys())
        raise KeyError(
            f"Could not resolve grid '{grid_base}' with resolution '{resolution}'. "
            f"Please ensure your recipe perfectly matches a key in the GRID_REGISTRY. Valid keys include: {valid_keys[:5]}..."
        )

    def fetch_data(self, recipe: dict, logger: Optional[logging.Logger] = None, downloaded_filepath: str = None, **kwargs) -> gpd.GeoDataFrame:
        """
        Loads the raw GBIF data into a GeoDataFrame, mining the native CRS from the grid definitions.
        """
        if not downloaded_filepath or not os.path.exists(downloaded_filepath):
            log_execution(logger, "No pre-downloaded file provided. Initiating synchronous download...", logging.WARNING)
            # Standalone fallback: generate query and download
            query = self.generate_gbif_query_from_recipe(recipe)
            download_key = sql.submit_gbif_query(query)
            downloaded_filepath = sql.fetch_gbif_download(download_key, target_dir=recipe.get('base_dir', './downloads'))

        log_execution(logger, f"Loading GBIF data from {downloaded_filepath}...", logging.INFO)
        
        # Extract and load the CSV payload
        with zipfile.ZipFile(downloaded_filepath) as z:
            csv_filename = [name for name in z.namelist() if name.endswith('.csv')][0]
            with z.open(csv_filename) as f:
                df = pd.read_csv(f, sep='\t') 

        # 1. Mine the native CRS from the grid strings
        native_crs = self._mine_crs_from_grid(df, logger)
        log_execution(logger, f"Dataset mapped to native CRS: {native_crs}", logging.INFO)

        # Build Geometries
        # Check if the dataframe contains the center-point coordinates supplied by the grid aggregation
        lat_col = 'latitude' if 'latitude' in df.columns else 'decimalLatitude'
        lon_col = 'longitude' if 'longitude' in df.columns else 'decimalLongitude'

        # Locate the cell code column generated by SQL
        cell_col = next((col for col in df.columns if 'cellcode' in col.lower()), None)

        if lat_col in df.columns and lon_col in df.columns:
            log_execution(logger, f"Generating representative Point geometries from {lat_col}/{lon_col}...", logging.INFO)
            gdf = gpd.GeoDataFrame(
                df, 
                geometry=gpd.points_from_xy(df[lon_col], df[lat_col]),
                crs="EPSG:4326"
            )
            if native_crs != "EPSG:4326":
                gdf = gdf.to_crs(native_crs)
                
        # Parsing polygons from cell codes
        elif cell_col and not df[cell_col].dropna().empty:
            log_execution(logger, f"No coordinates found. Parsing polygon geometries directly from '{cell_col}'...", logging.INFO)
            
            # Apply the parser to generate shapely boxes
            polygons = df[cell_col].apply(self._parse_cellcode_to_polygon)
            
            gdf = gpd.GeoDataFrame(df, geometry=polygons, crs=native_crs)
            
            # Drop any rows where the cell code was invalid or unsupported (like EQDG strings)
            failed_parse_count = gdf.geometry.isna().sum()
            if failed_parse_count > 0:
                log_execution(logger, f"Dropped {failed_parse_count} rows with unsupported or malformed cell codes.", logging.WARNING)
                gdf = gdf.dropna(subset=['geometry']).copy()
                
        else:
            log_execution(logger, "No coordinates or valid cell codes found. Returning un-spatialized DataFrame.", logging.WARNING)
            gdf = gpd.GeoDataFrame(df)

        """
        # Spatial clipping / filtering

        spatial_cfg = recipe.get('spatial', {})
        
        # Only clip if geometries exist and a bounding box is requested
        if not gdf.empty and gdf.geometry.name is not None and spatial_cfg.get('use_bbox', False) and 'bbox' in spatial_cfg:
            log_execution(logger, "Clipping fetched GBIF data strictly to recipe bounding box...", logging.INFO)
            bbox_cfg = spatial_cfg['bbox']
            
            # The recipe bbox is defined in WGS84 (EPSG:4326) degrees
            wgs84_bbox = box(
                bbox_cfg["long_min"], 
                bbox_cfg["lat_min"], 
                bbox_cfg["long_max"], 
                bbox_cfg["lat_max"]
            )
            
            # Convert the shapely box into a GeoSeries to handle reprojections safely
            bbox_series = gpd.GeoSeries([wgs84_bbox], crs="EPSG:4326")
            
            # Project the bbox mask to the native CRS we just mined
            if gdf.crs is not None and gdf.crs != "EPSG:4326":
                bbox_series = bbox_series.to_crs(gdf.crs)
                
            # Perform the geometric clip
            gdf = gpd.clip(gdf, bbox_series)
            
            log_execution(logger, f"Features remaining after spatial clip: {len(gdf)}", logging.INFO)
        """
        return gdf
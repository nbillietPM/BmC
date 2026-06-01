import logging
import numpy as np
from pyproj import CRS, Transformer
from typing import Optional, Tuple

    def build_safe_fetch_envelope(
        self,
        target_grid_name: str,
        target_bounds: Optional[Tuple[float, float, float, float]] = None,
        source_crs_or_grid: str = "EPSG:4326",
        source_resolution: Optional[float] = None,
        pixel_buffer: int = 5,
        logger: Optional[logging.Logger] = None
    ) -> Tuple[float, float, float, float]:
        """
        Constructs a densified, buffered source envelope guaranteed to fully encapsulate 
        a target grid region without causing edge starvation or NaN boundary artifacts.

        Parameters
        ----------
        target_grid_name : str
            The key of the destination grid defined in `GRID_REGISTRY` (e.g., "EEA_1km").
        target_bounds : tuple of float, optional
            Specific sub-region bounding box in target CRS units: (minx, miny, maxx, maxy). 
            If omitted, defaults to the master grid's full definitive bounds.
        source_crs_or_grid : str, optional
            Either a target key from `GRID_REGISTRY` (e.g., "Global_WGS84_30sec") or a 
            standard CRS string (e.g., "EPSG:4326"). Default is "EPSG:4326".
        source_resolution : float, optional
            The size of a single source pixel in native source CRS units. Automatically 
            inferred if `source_crs_or_grid` exists in the registry.
        pixel_buffer : int, optional
            Number of native source pixels added as an outer safety padding to support 
            multi-pixel GDAL resampling kernels. Default is 5.
        logger : logging.Logger, optional
            Logger instance for recording execution metadata. Default is None.

        Returns
        -------
        tuple of float
            The safe outer envelope in the source coordinate space: 
            (src_minx, src_miny, src_maxx, src_maxy).

        Raises
        -------
        KeyError
            If the requested `target_grid_name` does not exist in the registry.
        ValueError
            If spatial transformation yields entirely non-finite coordinates.
        """
        if logger:
            logger.info(f"Computing safe fetch envelope for target grid '{target_grid_name}'...")

        # 1. Resolve Target Grid Configurations
        if target_grid_name not in self.GRID_REGISTRY:
            raise KeyError(f"Target grid '{target_grid_name}' not found in GRID_REGISTRY.")
            
        target_spec = self.GRID_REGISTRY[target_grid_name]
        target_crs = target_spec["crs"]
        
        if target_bounds is None:
            target_bounds = target_spec["bounds"]
            if logger:
                logger.info("Specific target_bounds omitted. Encapsulating full master grid extent.")

        # 2. Resolve Source Data Configurations
        if source_crs_or_grid in self.GRID_REGISTRY:
            src_spec = self.GRID_REGISTRY[source_crs_or_grid]
            actual_source_crs = src_spec["crs"]
            if source_resolution is None:
                source_resolution = src_spec["resolution"]
        else:
            actual_source_crs = source_crs_or_grid
            if source_resolution is None:
                # Smart fallback: Assume CHELSA/WorldClim ~30 arc-second base spacing
                source_resolution = self.GRID_REGISTRY.get("Global_WGS84_30sec", {}).get(
                    "resolution", 0.008333333333333333
                )
                if logger:
                    logger.warning(
                        f"source_resolution omitted for custom CRS '{actual_source_crs}'. "
                        f"Applying default 30 arc-second fallback: {source_resolution}"
                    )

        # 3. Vectorized Perimeter Densification (Captures projection curvature)
        minx, miny, maxx, maxy = target_bounds
        num_points = 100  # Granularity per edge

        # Linearly interpolate intermediate coordinates along the box boundaries
        bx = np.linspace(minx, maxx, num_points)
        by = np.full(num_points, miny)

        rx = np.full(num_points, maxx)
        ry = np.linspace(miny, maxy, num_points)

        tx = np.linspace(maxx, minx, num_points)
        ty = np.full(num_points, maxy)

        lx = np.full(num_points, minx)
        ly = np.linspace(maxy, miny, num_points)

        # Merge perimeter arrays
        perimeter_x = np.concatenate([bx, rx, tx, lx])
        perimeter_y = np.concatenate([by, ry, ty, ly])

        # 4. Perform Coordinate Transformation
        transformer = Transformer.from_crs(target_crs, actual_source_crs, always_xy=True)
        src_x, src_y = transformer.transform(perimeter_x, perimeter_y)

        # Validate transformation matrix domain limits
        valid_mask = np.isfinite(src_x) & np.isfinite(src_y)
        if not np.any(valid_mask):
            raise ValueError(
                f"Failed to project target bounds from {target_crs} to {actual_source_crs}. "
                "Ensure target coordinates fall within allowable projection definitions."
            )
            
        src_x, src_y = src_x[valid_mask], src_y[valid_mask]

        # Extract precise envelope bounds
        src_minx, src_maxx = float(np.min(src_x)), float(np.max(src_x))
        src_miny, src_maxy = float(np.min(src_y)), float(np.max(src_y))

        # 5. Apply Resampling Safety Buffer
        buffer_padding = source_resolution * pixel_buffer
        
        safe_minx = src_minx - buffer_padding
        safe_maxx = src_maxx + buffer_padding
        safe_miny = src_miny - buffer_padding
        safe_maxy = src_maxy + buffer_padding

        # 6. Apply Geographic Domain Guardrails
        src_crs_obj = CRS.from_string(actual_source_crs)
        if src_crs_obj.is_geographic:
            safe_minx = max(-180.0, safe_minx)
            safe_maxx = min(180.0, safe_maxx)
            safe_miny = max(-90.0, safe_miny)
            safe_maxy = min(90.0, safe_maxy)

        if logger:
            logger.info(
                f"Safe Source Envelope ({actual_source_crs}): "
                f"({safe_minx:.5f}, {safe_miny:.5f}, {safe_maxx:.5f}, {safe_maxy:.5f})"
            )
            
        return (safe_minx, safe_miny, safe_maxx, safe_maxy)

# Assuming 'cube' is your initialized processing class instance

# 1. Define your localized metric target area using the EEA 1km grid framework
target_grid = "EEA_1km"
my_study_area_bounds = (3800000, 2900000, 3900000, 3000000) # Metric EPSG:3035 coordinates

# 2. Compute the safe fetching window matching native CHELSA/WGS84 source files
safe_wgs_bbox = cube.build_safe_fetch_envelope(
    target_grid_name=target_grid,
    target_bounds=my_study_area_bounds,
    source_crs_or_grid="Global_WGS84_30sec", # Automatically inherits CRS and ~1km pixel metrics
    pixel_buffer=5,                          # Adds 5 pixels to feed average/cubic warping kernels
    logger=logger
)

# 3. Batch fetch the source spatial data from S3 using this buffered envelope
raw_chelsa_datacubes = batch_fetch_chelsa_and_stack(
    filtered_catalog=filtered_catalog, 
    bbox=safe_wgs_bbox
)

# 4. Pass the buffered source arrays to your out-of-core affine warping wrapper
# Because the source arrays possess an outer safety collar, GDAL populates the target 
# boundary pixels perfectly wall-to-wall.
aligned_datacubes = process_chelsa_through_engine(
    chelsa_datasets=raw_chelsa_datacubes,
    cube_instance=cube,
    grid_name=target_grid,
    output_base_dir="master_datacubes/chelsa/",
    compress_mode="deflate",
    logger=logger
)

import os
import gc
import logging
import pandas as pd
import numpy as np
import xarray as xr
import rioxarray
import rasterio
from rasterio.enums import Resampling
import dask
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Dict, List, Tuple, Union, Optional, Any

from bmc.cube.spatiotemporal import spatiotemporal_cube
from bmc.utils.logger import log_execution


class chelsa_cube(spatiotemporal_cube):
    """
    Child class dedicated to the out-of-core ingestion, alignment, and master-grid 
    snapping of the high-resolution CHELSA V2.1 climatological archive.

    Inherits from `spatiotemporal_cube`. This class acts as the dedicated processing 
    engine for continuous atmospheric variables, volumetric precipitation fluxes, and 
    multi-decadal CMIP6 bioclimatic scenarios. It intercepts spatial recipes to enforce 
    native observation scales (~1km equivalent target grids), calculates safe outer 
    fetching envelopes to eliminate edge starvation, streams remote Cloud Optimized 
    GeoTIFFs lazily, standardizes internal downscaled arrays, and warps variables out-of-core.

    Attributes
    ----------
    catalog : pd.DataFrame
        The master precompiled inventory catalog detailing all available CHELSA V2.1 
        assets, associated temporal ranges, CMIP6 ensembles, scenarios, and virtual 
        S3 file paths (`vsi_path`).
    resample_rules : dict
        Internal definitive mapping routing physical environmental variables to their 
        mathematically valid GDAL resampling algorithms (e.g., preserving volumes via 
        `average` vs. continuous gradients via `bilinear`).
    """

    def __init__(self, catalog_df: pd.DataFrame, **kwargs):
        """
        Initializes the CHELSA data lake ingestion engine.

        Parameters
        ----------
        catalog_df : pd.DataFrame
            The master CHELSA inventory catalog containing requisite indexing columns: 
            `level`, `variable`, `date`, `time_range`, `ensemble`, `scenario`, and `vsi_path`.
        **kwargs : dict
            Optional keyword arguments passed directly to the `spatiotemporal_cube` parent class.
        """
        super().__init__(**kwargs)
        self.catalog = catalog_df

        # Definitive physical resampling logic mapping
        self.resample_rules = {
            # Continuous atmospheric surfaces
            'tas': 'bilinear',
            'tasmax': 'bilinear',
            'tasmin': 'bilinear',
            'sfcWind': 'bilinear',
            'vpd': 'bilinear',
            # Volumetric mass conservation and fractional percentages
            'pr': 'average',
            'rsds': 'average',
            'clt': 'average',
            'hurs': 'average',
            'pet': 'average'
        }

    def _resolve_chelsa_target_grid(
        self, 
        spatial_cfg: Dict[str, Any], 
        logger: Optional[logging.Logger] = None
    ) -> str:
        """
        Intersects target spatial configurations to override fine resolutions, locking 
        execution strictly to the equivalent native ~1km grid framework.

        Parameters
        ----------
        spatial_cfg : dict
            The `spatial` block extracted from the YAML execution recipe.
        logger : logging.Logger, optional
            The logger instance used to record configuration modifications. Default is None.

        Returns
        -------
        str
            The specific, validated target grid key from `GRID_REGISTRY` (e.g., "EEA_1km").
        """
        log_execution(logger, "Resolving target grid base for CHELSA ingestion...", logging.INFO)
        grid_base = str(spatial_cfg.get('target_grid', 'EEA')).upper()
        
        if grid_base == "EEA":
            resolved_grid = "EEA_1km"
        elif grid_base in ["GEA", "GLOBAL_EQUALAREA"]:
            resolved_grid = "Global_EqualArea_1km"
        elif grid_base in ["WGS84", "GLOBAL_WGS84"]:
            resolved_grid = "Global_WGS84_30sec"
        else:
            # Dynamic lookup for equivalent base reference
            resolved_grid = "EEA_1km"
            for key in self.GRID_REGISTRY.keys():
                if grid_base in key.upper() and ("1KM" in key.upper() or "30SEC" in key.upper()):
                    resolved_grid = key
                    break
            log_execution(
                logger, 
                f"Unrecognized target grid base '{grid_base}'. Applying fallback: '{resolved_grid}'.", 
                logging.WARNING
            )
            return resolved_grid

        log_execution(logger, f"Enforcing native atmospheric scale: mapped to '{resolved_grid}'.", logging.INFO)
        return resolved_grid

    def intersect_config(
        self, 
        recipe: Dict[str, Any], 
        logger: Optional[logging.Logger] = None
    ) -> pd.DataFrame:
        """
        Parses the abstract configuration recipe against the master CHELSA catalog to 
        isolate the precise multi-variable file sets required for retrieval.

        Parameters
        ----------
        recipe : dict
            The complete, loaded YAML configuration recipe.
        logger : logging.Logger, optional
            The logger instance used to document intersection operations. Default is None.

        Returns
        -------
        pd.DataFrame
            A highly filtered subset of the inventory catalog containing only activated assets.
        """
        log_execution(logger, "Intersecting configuration recipe against CHELSA inventory...", logging.INFO)
        chelsa_cfg = recipe.get('sources', {}).get('chelsa', {})
        
        if not chelsa_cfg.get('enabled', False):
            log_execution(logger, "CHELSA processing explicitly disabled in recipe.", logging.INFO)
            return pd.DataFrame()

        # Isolate temporal bounds
        temp_cfg = recipe.get('temporal', {})
        start_date = pd.to_datetime(f"{temp_cfg.get('start_year', 1980)}-{temp_cfg.get('start_month', 1):02d}-01")
        end_date = pd.to_datetime(f"{temp_cfg.get('end_year', 2020)}-{temp_cfg.get('end_month', 12):02d}-28")

        levels_cfg = chelsa_cfg.get('levels', {})
        filtered_chunks = []

        for level, settings in levels_cfg.items():
            if not settings.get('include', False):
                continue

            active_vars = [var for var, is_active in settings.get('variables', {}).items() if is_active]
            if not active_vars:
                continue

            base_mask = (self.catalog['level'] == level) & (self.catalog['variable'].isin(active_vars))

            # Apply temporal streaming logic to continuous historical data
            if level in ['daily', 'monthly', 'annual']:
                time_mask = (self.catalog['date'] >= start_date) & (self.catalog['date'] <= end_date)
                chunk = self.catalog[base_mask & time_mask]

            # Apply static cross-matrix filtering to multi-decadal norms and models
            elif level in ['climatologies', 'bioclim']:
                active_ranges = [tr for tr, state in settings.get('time_ranges', {}).items() if state]
                active_ensembles = [ens for ens, state in settings.get('ensembles', {}).items() if state]
                active_scenarios = [scen for scen, state in settings.get('scenarios', {}).items() if state]

                static_mask = (
                    (self.catalog['time_range'].isin(active_ranges)) &
                    (self.catalog['ensemble'].isin(active_ensembles) | self.catalog['ensemble'].isna()) &
                    (self.catalog['scenario'].isin(active_scenarios) | self.catalog['scenario'].isna())
                )
                chunk = self.catalog[base_mask & static_mask]

            filtered_chunks.append(chunk)

        if filtered_chunks:
            final_df = pd.concat(filtered_chunks, ignore_index=True)
            log_execution(logger, f"Intersection successful: queued {len(final_df)} target assets.", logging.INFO)
            return final_df
            
        log_execution(logger, "No inventory assets matched specified parameters.", logging.WARNING)
        return pd.DataFrame()

    @staticmethod
    def _fetch_worker(
        row: pd.Series, 
        bbox: Tuple[float, float, float, float], 
        gdal_env: Dict[str, str]
    ) -> Tuple[str, Optional[xr.DataArray]]:
        """
        Thread worker execution block: applies optimizations to lazily parse metadata, 
        clip spatial bounds, scale integer buffers, and assign coordinate indices.

        Parameters
        ----------
        row : pd.Series
            A single catalog row item defining the remote asset.
        bbox : tuple
            The exact safe outer fetching bounding box in source CRS coordinates.
        gdal_env : dict
            Optimized GDAL environment configuration flags.

        Returns
        -------
        tuple
            The extracted grouping level string alongside the finalized lazy `xr.DataArray`.
        """
        vsi_path = row['vsi_path']
        level = row['level']
        var_name = row['variable']

        try:
            with rasterio.Env(**gdal_env):
                # Lock Dask single-threaded locally to prevent packet loss over multiplexed HTTP connections
                with dask.config.set(scheduler='single-threaded'):
                    da = rioxarray.open_rasterio(vsi_path, chunks=True, masked=True)
                    clipped = da.rio.clip_box(*bbox).compute()

                clipped.name = var_name

                # Metadata integration via coordinate coordinate promotion
                if pd.notna(row.get('date')):
                    clipped = clipped.assign_coords({"time": row['date']}).expand_dims("time")

                if pd.notna(row.get('time_range')):
                    ens = row.get('ensemble') if pd.notna(row.get('ensemble')) else 'historical'
                    scen = row.get('scenario') if pd.notna(row.get('scenario')) else 'historical'
                    clipped = clipped.assign_coords({
                        "time_range": row['time_range'],
                        "ensemble": ens,
                        "scenario": scen
                    }).expand_dims(["time_range", "ensemble", "scenario"])

                if "band" in clipped.coords:
                    clipped = clipped.squeeze("band").drop_vars("band")

                return level, clipped

        except Exception as e:
            # Suppress console breaking to preserve processing across thread queues
            return level, None

    def _internal_shape_alignment(
        self, 
        loose_arrays: List[xr.DataArray], 
        logger: Optional[logging.Logger] = None
    ) -> List[xr.DataArray]:
        """
        Internally harmonizes downscaled or native low-resolution inputs (e.g., sfcWind) 
        to perfectly align with the core ~1km baseline within RAM.

        Parameters
        ----------
        loose_arrays : list of xr.DataArray
            Chronologically structured time-series variables belonging to a level.
        logger : logging.Logger, optional
            The logger instance used to record alignment steps. Default is None.

        Returns
        -------
        list of xr.DataArray
            Perfectly grid-aligned arrays guaranteed to stack without coordinate collisions.
        """
        if not loose_arrays:
            return []

        # Establish definitive spatial footprint via maximum array area
        sorted_arrays = sorted(loose_arrays, key=lambda d: d.sizes.get('x', 0) * d.sizes.get('y', 0), reverse=True)
        base_template = sorted_arrays[0]
        target_shape = base_template.shape[-2:]

        aligned_list = []
        for da in loose_arrays:
            if da.shape[-2:] != target_shape:
                log_execution(
                    logger, 
                    f"Internally aligning coarse variable '{da.name}' to standard 1km baseline...", 
                    logging.INFO
                )
                if da.rio.crs is None: 
                    da = da.rio.write_crs("EPSG:4326")
                if base_template.rio.crs is None: 
                    base_template = base_template.rio.write_crs("EPSG:4326")
                
                resample_key = self.resample_rules.get(str(da.name), 'bilinear')
                res_enum = getattr(Resampling, resample_key, Resampling.bilinear)
                
                da = da.rio.reproject_match(base_template, resampling=res_enum)
            aligned_list.append(da)
            
        return aligned_list

    def batch_fetch_source_level(
        self, 
        filtered_catalog: pd.DataFrame, 
        source_bbox: Tuple[float, float, float, float], 
        max_workers: int = 10,
        logger: Optional[logging.Logger] = None
    ) -> Dict[str, xr.Dataset]:
        """
        Executes highly parallelized, optimized HTTP extractions across the targeted subset, 
        automatically resolving variable shape discrepancies and generating unified Datasets.

        Parameters
        ----------
        filtered_catalog : pd.DataFrame
            The final subset DataFrame detailing target S3 files.
        source_bbox : tuple
            The exact outer boundary buffer in source CRS coordinates.
        max_workers : int, optional
            Concurrent ThreadPool execution threads allocated to I/O fetching. Default is 10.
        logger : logging.Logger, optional
            The logger instance used to report process progress. Default is None.

        Returns
        -------
        dict
            A mapping of level strings directly to fully merged native `xr.Dataset` objects.
        """
        log_execution(logger, f"Spinning up thread pool ({max_workers} workers) for concurrent extraction...", logging.INFO)
        
        gdal_env = {
            "GDAL_DISABLE_READDIR_ON_OPEN": "EMPTY_DIR",
            "CPL_VSIL_CURL_ALLOWED_EXTENSIONS": ".tif",
            "GDAL_HTTP_MULTIPLE_CONNECTIONS": "NO", 
            "VSI_CACHE": "FALSE",                   
            "GDAL_HTTP_MAX_RETRY": "10", 
            "GDAL_HTTP_RETRY_DELAY": "3",
            "GDAL_HTTP_TIMEOUT": "30",
            "GDAL_HTTP_MERGE_CONSECUTIVE_RANGES": "YES"
        }

        level_bins: Dict[str, List[xr.DataArray]] = {lvl: [] for lvl in filtered_catalog['level'].unique()}
        failed_fetches = 0

        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            futures = {
                executor.submit(self._fetch_worker, row, source_bbox, gdal_env): row['vsi_path']
                for _, row in filtered_catalog.iterrows()
            }
            for future in as_completed(futures):
                level, data = future.result()
                if data is not None:
                    level_bins[level].append(data)
                else:
                    failed_fetches += 1

        if failed_fetches > 0:
            log_execution(logger, f"Network fetching Phase closed with {failed_fetches} failed files.", logging.WARNING)

        level_datasets = {}
        for level, arrays in level_bins.items():
            if not arrays:
                continue

            log_execution(logger, f"Fusing and harmonizing arrays for level '{level}'...", logging.INFO)
            
            # Subgroup arrays by physical variable names
            var_groups = {}
            for da in arrays:
                var_groups.setdefault(str(da.name), []).append(da)

            compiled_vars = []
            for var_name, da_list in var_groups.items():
                # Erase internal coordinate drift across concurrent download slices
                base_x, base_y = da_list[0].coords['x'], da_list[0].coords['y']
                snapped_list = [d.assign_coords(x=base_x, y=base_y) for d in da_list]
                
                combined = xr.combine_by_coords(snapped_list, combine_attrs='drop_conflicts', join='override')
                if isinstance(combined, xr.DataArray):
                    combined.name = var_name
                compiled_vars.append(combined)

            # Eliminate coordinate mismatches caused by coarser model products (sfcWind)
            aligned_vars = self._internal_shape_alignment(compiled_vars, logger=logger)

            # Final global lock to first array matrix origin
            ref_x, ref_y = aligned_vars[0].coords['x'], aligned_vars[0].coords['y']
            final_vars = [d.assign_coords(x=ref_x, y=ref_y) for d in aligned_vars]

            level_ds = xr.merge(final_vars, combine_attrs='drop_conflicts', join='override')
            level_ds.rio.write_crs("EPSG:4326", inplace=True)
            level_datasets[level] = level_ds

        return level_datasets

    def process_cube(
        self, 
        recipe: Dict[str, Any], 
        max_workers: int = 10, 
        logger: Optional[logging.Logger] = None
    ) -> List[str]:
        """
        Orchestrates the overarching computational pipeline: processes configurations, 
        evaluates safe spatial parameters, extracts cloud subsets, executes out-of-core 
        reprojections, and commits data directly to target storage formats.

        Parameters
        ----------
        recipe : dict
            The validated YAML execution blueprint dictating spatiotemporal targets.
        max_workers : int, optional
            Maximum concurrent workers dedicated to network stream layers. Default is 10.
        logger : logging.Logger, optional
            The master logger instance recording operations. Default is None.

        Returns
        -------
        list of str
            A comprehensive index containing absolute paths to all finalized, optimized 
            Cloud Optimized GeoTIFF files permanently saved to disk.
        """
        log_execution(logger, "\n=== Initiating CHELSA Out-of-Core Processing Engine ===", logging.INFO)
        
        base_dir = recipe.get('base_dir', './outputs/')
        output_base_dir = os.path.join(base_dir, 'chelsa/')
        compress_opt = recipe.get('resampling', {}).get('compress_mode', 'deflate')
        generated_cogs = []

        # 1. Pipeline configuration catalog isolation
        filtered_df = self.intersect_config(recipe, logger=logger)
        if filtered_df.empty:
            log_execution(logger, "Terminating pipeline: no candidate asset catalog generated.", logging.WARNING)
            return generated_cogs

        # 2. Extract validated bounding configurations
        spatial_cfg = recipe.get('spatial', {})
        target_grid_key = self._resolve_chelsa_target_grid(spatial_cfg, logger=logger)
        
        bbox_cfg = spatial_cfg.get('bbox', {})
        target_bounds = (
            min(bbox_cfg.get('long_min', 0), bbox_cfg.get('long_max', 0)),
            min(bbox_cfg.get('lat_min', 0), bbox_cfg.get('lat_max', 0)),
            max(bbox_cfg.get('long_min', 0), bbox_cfg.get('long_max', 0)),
            max(bbox_cfg.get('lat_min', 0), bbox_cfg.get('lat_max', 0))
        )

        # 3. Derive safe buffered fetching space via parent class
        source_bbox = self.build_safe_fetch_envelope(
            target_grid_name=target_grid_key,
            target_bounds=target_bounds,
            source_crs_or_grid="Global_WGS84_30sec",
            pixel_buffer=5,
            logger=logger
        )

        # 4. Invoke parallel network extraction engine
        native_datacubes = self.batch_fetch_source_level(
            filtered_df, 
            source_bbox, 
            max_workers=max_workers, 
            logger=logger
        )

        # 5. Out-of-core snapping via parent base warping engine
        for level, ds in native_datacubes.items():
            log_execution(logger, f"\nRouting level '{level}' arrays through high-performance GDAL Warp...", logging.INFO)
            level_dir = os.path.join(output_base_dir, level)
            os.makedirs(level_dir, exist_ok=True)

            time_coords = ds.coords.get('time', None)

            # Warp environmental arrays band-by-band out-of-core
            for var_name, da in ds.data_vars.items():
                rule = self.resample_rules.get(str(var_name), 'bilinear')
                final_cog_path = os.path.join(level_dir, f"chelsa_{level}_{var_name}_{target_grid_key}.tif")

                # Override logic specifically routing composite bioclim precipitation metrics to volume conservation
                if 'bio' in str(var_name).lower():
                    rule = 'average' if var_name.lower() in ['bio12', 'bio13', 'bio14'] else 'bilinear'

                log_execution(logger, f"Snapping variable '{var_name}' -> {final_cog_path} (Resampling: {rule})", logging.INFO)
                
                # Sanitize geometry internally to enforce native default constraints safely
                da = self._sanitize_spatial_geometry(da, default_crs="EPSG:4326", logger=logger)
                
                # Route through base out-of-core transformation method
                warped_da = self.affine_reproject(
                    input_data=da,
                    output_filepath=final_cog_path,
                    grid_name=target_grid_key,
                    resample_keyword=rule,
                    compress_mode=compress_opt,
                    logger=logger
                )

                # Reconstruct structured array axes
                if time_coords is not None and "band" in warped_da.dims:
                    warped_da = warped_da.rename({"band": "time"}).assign_coords(time=time_coords)
                elif "band" in warped_da.dims and warped_da.sizes["band"] == 1:
                    warped_da = warped_da.squeeze("band").drop_vars("band")

                warped_da.name = var_name
                
                # Explicit final physical persistence to output structure using parent base method
                self.export_to_cog(warped_da, final_cog_path, logger=logger)
                generated_cogs.append(final_cog_path)

                # Execute dynamic garbage cleanup to release heavy cache files safely
                warped_da.close()
                del warped_da

            ds.close()
            del ds
            gc.collect()

        log_execution(logger, "\n=== CHELSA Data Lake Ingestion Complete ===", logging.INFO)
        return generated_cogs
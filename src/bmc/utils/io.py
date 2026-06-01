import logging
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Dict, List, Tuple, Optional, Union, Any

import dask
import rasterio
import rioxarray
import xarray as xr

from bmc.utils.logger import log_execution

logger = logging.getLogger(__name__)

# Highly optimized default environment for streaming Cloud Optimized GeoTIFFs (COGs) via HTTP/S3
DEFAULT_GDAL_ENV = {
# 1. Prevents GDAL from scanning the entire remote bucket directory for matching file layouts.
#    Treating the folder as an 'EMPTY_DIR' avoids massive directory parsing latency penalties.
"GDAL_DISABLE_READDIR_ON_OPEN": "EMPTY_DIR",

# 2. Restricts network probes strictly to .tif assets. Prevents GDAL from hunting for 
#    non-existent sidecar files (like .tfw, .aux.xml, or .ovr) which wastes valuable network roundtrips.
"CPL_VSIL_CURL_ALLOWED_EXTENSIONS": ".tif",

# 3. Disables curl's multiplexed multi-connection connection reuse pool. This is critical for 
#    thread-safety inside a Python ThreadPoolExecutor to prevent cross-thread socket pollution.
"GDAL_HTTP_MULTIPLE_CONNECTIONS": "NO", 

# 4. Disables GDAL's internal global RAM caching mechanism for virtual files. Since our thread pool 
#    immediately processes, clips, and frees arrays, turning this off prevents creeping RAM bloat.
"VSI_CACHE": "FALSE",                    

# 5. Sets the maximum connection retry threshold. If the host server experiences a transient spike 
#    or issues a 429/503 rate limit throttle, the thread will attempt to re-connect up to 10 times.
"GDAL_HTTP_MAX_RETRY": "10", 

# 6. The backoff rest interval (in seconds) between retry attempts, giving remote servers space 
#    to recover before the thread aggressively polls the resource endpoint again.
"GDAL_HTTP_RETRY_DELAY": "3",

# 7. Slashes the maximum connection timeout window to 30 seconds. Prevents an un-responsive 
#    network socket or dead server from permanently hanging an execution thread indefinitely.
"GDAL_HTTP_TIMEOUT": "30",

# 8. Instructs GDAL to look ahead at byte-range requests. If the script requests blocks that 
#    are close together, it merges them into one larger sequential stream request, bypassing 
#    individual HTTP header handshake latencies.
"GDAL_HTTP_MERGE_CONSECUTIVE_RANGES": "YES"}


def _fetch_single_raster(
    path: str, 
    geom: Union[Tuple[float, float, float, float], Any], 
    gdal_env: Dict[str, str],
    geom_crs: Optional[str] = None
) -> Tuple[str, Optional[xr.DataArray]]:
    """
    Low-level thread worker to stream and clip a single remote or local raster asset.

    Parameters
    ----------
    path : str
        The absolute path or virtual file system URI (e.g., '/vsicurl/...', '/vsis3/...') 
        pointing to the target raster asset.
    geom : tuple of float or Any (Shapely geometry, GeoJSON dict)
        The spatial geometry used to clip the raster. If a 4-element tuple is provided, 
        it is treated as a bounding box (min_x, min_y, max_x, max_y). Otherwise, it is 
        treated as a polygon mask.
    gdal_env : dict
        A dictionary of GDAL environment variables used to configure 
        the `rasterio.Env` context for optimized streaming.
    geom_crs : str, optional
        The coordinate reference system of the provided polygon geometry 
        (e.g., "EPSG:4326"). Required if passing a polygon that differs from 
        the source raster's CRS. Default is None.

    Returns
    -------
    tuple
        A 2-element tuple containing:
        - path (str): The original file path, passed back for downstream matching.
        - clipped (xarray.DataArray or None): The clipped spatiotemporal array in memory. 
          Returns None if a network or I/O failure occurs.
    """
    # Clean fallback: use the module-level constant if no specific override is passed down
    gdal_env = gdal_env or DEFAULT_GDAL_ENV
    try:
        with rasterio.Env(**gdal_env):
            # Enforce single-threaded Dask computation within this specific worker
            with dask.config.set(scheduler='single-threaded'):
                da = rioxarray.open_rasterio(path, chunks=True, masked=True)
                
                # Route based on geometry type
                if isinstance(geom, tuple) and len(geom) == 4:
                    # High-speed rectangular envelope crop
                    clipped = da.rio.clip_box(*geom).compute()
                else:
                    # Precise polygon masking (masks pixels outside boundary to NaN)
                    # rioxarray expects an iterable of geometries
                    geometries = geom if isinstance(geom, (list, tuple)) else [geom]
                    clipped = da.rio.clip(geometries, crs=geom_crs, drop=True).compute()
                    
                return path, clipped
                
    except Exception as e:
        log_execution(
            logger, 
            f"Failed to fetch asset '{path}'. Reason: {e}", 
            logging.DEBUG
        )
        return path, None


def parallel_fetch_rasters(
    paths: Union[str, List[str]],
    geom: Union[Tuple[float, float, float, float], Any],
    max_workers: int = 10,
    gdal_config: Optional[Dict[str, str]] = None,
    geom_crs: Optional[str] = None
) -> Dict[str, xr.DataArray]:
    """
    Executes highly parallelized, optimized extractions across a generalized list of rasters.

    This utility spins up a concurrent thread pool to fetch, clip, and load raster 
    assets from any list of URIs or local paths. It acts as a universal geospatial 
    I/O engine completely decoupled from specific dataset metadata.

    Parameters
    ----------
    paths : str or list of str
        A single file path/URI or a list of file paths/URIs to be downloaded and clipped.
    geom : tuple of float or Any (Shapely geometry, GeoJSON dict)
        The spatial geometry used to clip the raster. Accepts either a rectangular 
        bounding box tuple (min_x, min_y, max_x, max_y) or a complex polygon geometry.
    max_workers : int, optional
        The maximum number of concurrent threads to spawn for network I/O. 
        Default is 10.
    gdal_config : dict, optional
        Overrides for the GDAL environment configuration. If None, a highly 
        optimized default configuration for Cloud Optimized GeoTIFF (COG) 
        HTTP streaming is applied. Default is None.
    geom_crs : str, optional
        The coordinate reference system of the provided polygon geometry 
        (e.g., "EPSG:4326"). Required if passing a polygon that differs from 
        the source raster's CRS. Default is None.

    Returns
    -------
    dict
        A dictionary mapping the original file path (str) directly to its 
        in-memory `xarray.DataArray`.

    Notes
    -----
    Files that fail to download (due to network timeouts or missing assets) 
    are logged and automatically excluded from the returned dictionary to 
    ensure pipeline continuity.
    """
    # Clean fallback: use the module-level constant if no specific override is passed down
    gdal_env = gdal_config or DEFAULT_GDAL_ENV

    if isinstance(paths, str):
        paths = [paths]

    geom_type = "bounding box" if isinstance(geom, tuple) and len(geom) == 4 else "polygon mask"
    log_execution(
        logger, 
        f"Initiating parallel raster fetch across {len(paths)} assets using {max_workers} workers (Geometry: {geom_type})...", 
        logging.INFO)

    fetched_arrays: Dict[str, xr.DataArray] = {}
    failed_fetches = 0

    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = {
            executor.submit(_fetch_single_raster, path, geom, gdal_env, geom_crs): path
            for path in paths}     
        for future in as_completed(futures):
            path, data = future.result()
            if data is not None:
                fetched_arrays[path] = data
            else:
                failed_fetches += 1

    if failed_fetches > 0:
        log_execution(
            logger, 
            f"Parallel ingest completed with {failed_fetches} network fetch failures.", 
            logging.WARNING
        )
    else:
        log_execution(
            logger, 
            "Parallel ingest completed successfully with zero fetch failures.", 
            logging.INFO
        )
    return fetched_arrays
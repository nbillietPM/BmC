import logging
from typing import Optional, Union, Dict, Any, List, Tuple
from abc import ABC, abstractmethod
from concurrent.futures import ThreadPoolExecutor
from functools import partial
import gc

import yaml
import sys
import os
import shutil
import zipfile
import glob
from pathlib import Path

import numpy as np
import xarray as xr
import pandas as pd
import rioxarray
from osgeo import gdal

from bmc.utils.spatial import build_envelope_from_file
from bmc.utils.io import parallel_fetch_rasters
from bmc.utils.logger import log_execution

from bmc.engine.spatial import spatial_engine


class spatiotemporal_lake(spatial_engine, ABC):

    def export_to_cog(
        self, 
        ds: Union[xr.DataArray, xr.Dataset], 
        output_filepath: str, 
        compress_mode: str = "deflate",
        logger: Optional[logging.Logger] = None
    ) -> str:
        """
        Exports an xarray object to disk as a Cloud Optimized GeoTIFF (COG).

        Parameters
        ----------
        ds : xarray.DataArray or xarray.Dataset
            The spatial data to be exported. If a Dataset is provided, it iterates 
            and saves each variable as a separate COG (or bands, depending on structure).
        output_filepath : str
            The target file path (must end in .tif).
        compress_mode : str, optional
            The compression algorithm. 'deflate' is highly recommended for SDM data.
        logger : logging.Logger, optional
            Logger for recording execution progress.

        Returns
        -------
        output_filepath : str
            The path to the successfully created COG.
        """
        log_execution(logger, f"Exporting to Cloud Optimized GeoTIFF: {output_filepath}", logging.INFO)
        
        # Ensure the directory exists
        os.makedirs(os.path.dirname(os.path.abspath(output_filepath)), exist_ok=True)

        try:
            # rioxarray has native support for the GDAL COG driver
            if isinstance(ds, xr.Dataset):
                # For datasets (like fractional coverages), we can write them out 
                # as multi-band COGs or iteratively. rioxarray handles Datasets by 
                # writing each data_var as a band if they share coordinates.
                ds.rio.to_raster(
                    output_filepath,
                    driver="COG",
                    compress=compress_mode,
                    tiled=True,
                    windowed=True # Crucial for keeping RAM usage low during write
                )
            else:
                ds.rio.to_raster(
                    output_filepath,
                    driver="COG",
                    compress=compress_mode,
                    tiled=True,
                    windowed=True
                )
            
            log_execution(logger, f"COG successfully generated.", logging.INFO)
            return output_filepath

        except Exception as e:
            log_execution(logger, f"Failed to export COG: {e}", logging.ERROR, exc_info=True)
            raise
 
    def gather_tifs_from_zips(
        self, 
        source_directory: str, 
        target_directory: str, 
        logger: Optional[logging.Logger] = None
    ) -> None:
        """
        Iterates through zip archives in a source directory and extracts 
        only the .tif files, flattening them into a single target directory.

        This method acts as a data preparation step, isolating raw spatial 
        arrays from nested archive structures and auxiliary metadata files 
        prior to Virtual Raster (VRT) mosaic construction.

        Parameters
        ----------
        source_directory : str
            The path to the directory containing the downloaded .zip archives.
        target_directory : str
            The destination directory where extracted .tif files will be saved.
            If the directory does not exist, it will be created.
        logger : logging.Logger, optional
            The logger instance for recording execution progress and errors. 
            Defaults to None.

        Returns
        -------
        None
        """
        # Set up the paths
        source_path = Path(source_directory)
        target_path = Path(target_directory)
        
        # Create the target directory if it doesn't exist yet
        target_path.mkdir(parents=True, exist_ok=True)

        # Find all zip files in the source folder
        zip_files = list(source_path.glob("*.zip"))
        log_execution(logger, f"Found {len(zip_files)} zip files. Starting extraction...", logging.INFO)

        # Iterate over each zip file
        for zip_file_path in zip_files:
            try:
                with zipfile.ZipFile(zip_file_path, 'r') as zip_ref:
                    
                    # Look at every file hidden inside the zip archive
                    for file_info in zip_ref.infolist():
                        
                        # Check if the file is a .tif
                        if file_info.filename.endswith('.tif'):
                            
                            # Extract the filename without any internal zip folder structure
                            tif_filename = Path(file_info.filename).name 
                            destination_file = target_path / tif_filename
                            
                            # Copy the .tif file to the shared folder
                            with zip_ref.open(file_info) as source_file:
                                with open(destination_file, 'wb') as target_file:
                                    shutil.copyfileobj(source_file, target_file)
            except zipfile.BadZipFile:
                log_execution(logger, f"Warning: {zip_file_path.name} is corrupted or not a valid zip file.", logging.WARNING)
                continue
            except Exception as e:
                log_execution(logger, f"Error extracting from {zip_file_path.name}: {e}", logging.ERROR)

        log_execution(logger, "All .tif files have been gathered successfully!", logging.INFO)

    def cleanup_raw_storage(self, recipe: Dict, logger: Optional[logging.Logger] = None) -> None:
        """
        Safely purges the raw data directory based on the user configuration.

        This function checks the execution recipe for the `keep_raw` parameter.
        If `keep_raw` is explicitly set to False, it will aggressively remove 
        the raw downloads directory to free up disk space. It uses `ignore_errors=True` 
        during removal to prevent phantom file locks (e.g., [WinError 32] on Windows) 
        from crashing the pipeline. If `keep_raw` is True or missing, the raw 
        data is safely retained.

        Parameters
        ----------
        recipe : dict
            The parsed execution configuration dictionary. It is expected to contain 
            the `keep_raw` boolean under `recipe['raw_config']['keep_raw']` and the 
            target directory path under `recipe['paths']['raw_dir']`.
        logger : logging.Logger, optional
            The logger instance used to record the execution and cleanup status. 
            Default is None.

        Returns
        -------
        None
            This function does not return a value.
        """
        keep_raw = recipe.get('raw_config', {}).get('keep_raw', True)
        raw_dir = recipe.get('paths', {}).get('raw_dir')

        if not keep_raw and raw_dir and os.path.exists(raw_dir):
            if logger:
                logger.info(f"keep_raw is False. Purging raw data directory: {raw_dir}")
            try:
                # Delete an entire directory tree; path must point to a directory (but not a symbolic link to a directory). 
                shutil.rmtree(raw_dir, ignore_errors=True)
                if logger:
                    logger.info("Raw data successfully deleted.")
            except Exception as e:
                if logger:
                    logger.warning(f"Could not completely delete raw directory: {e}")
        elif keep_raw:
            if logger:
                logger.info("keep_raw is True. Retaining raw downloaded data.")

    def build_fractional_cogs(self, ds, grid_name, output_dir, class_values, logger, file_prefix="fractional", class_mapping=None):
            """
            Lake Orchestrator: Iterates classes, computes fractions, and bakes COGs.
            """
            for cls in class_values:
                cls_int = int(cls)
                
                # Map the integer to a human-readable name if available
                if class_mapping and cls_int in class_mapping:
                    cls_name = str(class_mapping[cls_int]).replace(" ", "_").replace("/", "_").replace("-", "_")
                else:
                    cls_name = f"class_{cls_int}"

                # 1. Ask the engine to do the math
                frac_da = self.compute_class_fraction(ds, cls_int, grid_name, logger)
                
                # 2. Handle the I/O
                final_cog_path = os.path.join(output_dir, f"{file_prefix}_{cls_name}.tif")
                self.export_to_cog(frac_da, final_cog_path, logger)
                
                # 3. Clean up RAM
                frac_da.close()
                del frac_da
                gc.collect()

    def build_virtual_mosaic(
        self,
        input_folder: str, 
        output_vrt_path: str, 
        logger: Optional[logging.Logger] = None
    ) -> Optional[str]:
        """
        Creates a lightweight Virtual Raster (VRT) blueprint from multiple GeoTIFF tiles.

        This method discovers all `.tif` files in a folder and creates an XML-based `.vrt` 
        file, mosaicking them together at their native resolution.

        Parameters
        ----------
        input_folder : str
            The directory containing the raw `.tif` tiles.
        output_vrt_path : str
            The destination file path for the blueprint (must end in `.vrt`).
        logger : logging.Logger, optional
            The logger instance to use for recording execution messages. Default is ``None``.

        Returns
        -------
        str or None
            The file path to the generated `.vrt` file. Returns ``None`` if no tiles were found.
        """
        # Enable errors
        gdal.UseExceptions()
        
        tif_files = glob.glob(f"{input_folder}/*.tif")
        if not tif_files:
            log_execution(logger, f"No .tif files found in '{input_folder}'.", logging.WARNING)
            return None

        log_execution(logger, f"Found {len(tif_files)} files. Building VRT blueprint...", logging.INFO)

        try:
            os.makedirs(os.path.dirname(os.path.abspath(output_vrt_path)), exist_ok=True)

            # Build the VRT without any resampling options
            vrt = gdal.BuildVRT(output_vrt_path, tif_files)
            
            # Critical: Flush cache and destroy the python object to force GDAL to write the XML to disk
            vrt.FlushCache()
            vrt = None 

            log_execution(logger, f"Virtual mosaic successfully saved to: {output_vrt_path}", logging.INFO)
            return output_vrt_path

        except Exception as e:
            log_execution(logger, f"Error building virtual mosaic: {e}", logging.ERROR, exc_info=True)
            raise

    def process_virtual_mosaic(
        self, 
        vrt_path: str, 
        strategy: str, 
        grid_name: str,
        output_dir_or_file: str,
        logger: Optional[logging.Logger] = None,
        **kwargs
    ) -> Union[xr.DataArray, xr.Dataset, None]:
        """
        Routes a Virtual Raster (VRT) blueprint to the appropriate spatial processing algorithm.

        This dispatcher function delays heavy pixel-crunching until the exact mathematical 
        strategy is determined. It handles the I/O handoff, routing the lightweight XML 
        blueprint to either a standard GDAL affine reprojection or a categorical 
        fractional coverage calculator.

        Parameters
        ----------
        vrt_path : str
            The file path to the source `.vrt` file.
        strategy : str
            The processing algorithm to apply. Valid options: 'reproject', 'coverage'.
        grid_name : str
            The key of the target grid defined in the class grid registry.
        output_dir_or_file : str
            If strategy is 'reproject', this should be the output file path (.tif).
            If strategy is 'coverage', this should be the output directory.
        logger : logging.Logger, optional
            The logger instance to use for recording execution messages. Default is ``None``.
        **kwargs : dict
            Keyword arguments passed directly to the chosen processing function 
            (e.g., class_values, class_mapping, file_prefix, resample_keyword, compress_mode).

        Returns
        -------
        xarray.DataArray or xarray.Dataset or None
            The lazily loaded result of the processing step if 'reproject'. 
            Returns None if 'coverage' is used, as it bakes directly to disk.
        """
        if not os.path.exists(vrt_path):
            raise FileNotFoundError(f"VRT blueprint not found at: {vrt_path}")

        log_execution(logger, f"Initializing '{strategy}' processing pipeline for VRT...", logging.INFO)

        try:
            if strategy.lower() == 'reproject':
                return self.affine_reproject(
                    input_data=vrt_path, 
                    output_filepath=output_dir_or_file, 
                    grid_name=grid_name, 
                    logger=logger, 
                    **kwargs
                )
                
            elif strategy.lower() == 'coverage':
                # Safely extract kwargs specific to categorical processing
                target_classes = kwargs.get('class_values', [])
                file_prefix = kwargs.get('file_prefix', 'fractional')
                class_mapping = kwargs.get('class_mapping', None)
                
                # FIX: Open the VRT blueprint as a chunked DataArray
                import rioxarray
                opened_ds = rioxarray.open_rasterio(
                    vrt_path, 
                    masked=True, 
                    chunks={'x': 2048, 'y': 2048}
                ).squeeze()
                
                self.build_fractional_cogs(
                    ds=opened_ds, # Pass the opened spatial array, not the string path
                    grid_name=grid_name, 
                    output_dir=output_dir_or_file, 
                    class_values=target_classes, 
                    logger=logger,
                    file_prefix=file_prefix,
                    class_mapping=class_mapping
                )
                return None
                
            else:
                raise ValueError(f"Unknown processing strategy: '{strategy}'. Must be 'reproject' or 'coverage'.")

        except Exception as e:
            log_execution(logger, f"Pipeline failure during {strategy}: {e}", logging.ERROR, exc_info=True)
            raise

    @abstractmethod
    def fetch_raw_data(self, recipe: Dict[str, Any], logger: logging.Logger) -> Any:
        """
        Connects to the vendor API or remote server and downloads the raw source files.
        
        Parameters
        ----------
        recipe : dict
            The configuration dictionary containing API credentials, spatial/temporal 
            bounds, and requested datasets.
        logger : logging.Logger
            Logger for recording API connection status and download progress.
        """
        pass

    @abstractmethod
    def build_datalake(self, recipe: Dict[str, Any], logger: logging.Logger) -> List[str]:
        """
        Translates raw downloaded files into standardized Cloud Optimized GeoTIFFs (COGs).
        
        This method should orchestrate the `spatial_engine` tools to mosaic raw tiles, 
        calculate fractional coverages, and save the final standardized arrays to the 
        local data lake directory.

        Parameters
        ----------
        recipe : dict
            The configuration dictionary dictating resampling strategies and output paths.
        logger : logging.Logger
            Logger for recording spatial transformations and I/O operations.

        Returns
        -------
        list of str
            A list of file paths pointing to the successfully generated COGs.
        """
        pass

    @abstractmethod
    def validate_datalake(self, base_dir: str, logger: logging.Logger) -> bool:
        """
        Performs strict mathematical and ecological QA validations on the generated lake.
        
        Parameters
        ----------
        base_dir : str
            The root directory of the finalized data lake.
        logger : logging.Logger
            Logger for recording QA test results and flagging physics violations.

        Returns
        -------
        bool
            True if the data passes all physics checks without corruption, False otherwise.
        """
        pass

    @abstractmethod
    def generate_catalog(self, target_dir: str, logger: logging.Logger) -> str:
        """
        Builds the final inventory file that downstream runtime cubes will query.
        
        Parameters
        ----------
        target_dir : str
            The root directory where the catalog file should be saved.
        logger : logging.Logger
            Logger for recording catalog generation.

        Returns
        -------
        str
            The file path to the newly generated catalog (e.g., CSV, Parquet).
        """
        pass
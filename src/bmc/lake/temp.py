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
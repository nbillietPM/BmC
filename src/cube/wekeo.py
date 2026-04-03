from .spatiotemporal import *
import yaml
import os
from src.utils.logger import log_execution

class wekeo_cube(spatiotemporal_cube):
    def __init__(self):
        wekeo_logger = self._setup_pipeline_logger("wekeo_logger", "wekeoCube.log")

    def _fetch_unpack_query(self, wekeo_query, hda_client, base_dir="wekeo", logger=self.wekeo_logger):
        """
        Fetches data using the hda_client, logs the progress, and extracts .tif files.
        """
        dataset_id = wekeo_query.get("dataset_id", "UNKNOWN_DATASET")
        log_execution(logger, f"===={dataset_id}====", logging.INFO)
        
        pretty_query = json.dumps(wekeo_query, indent=4)
        indented_block = "\n    " + pretty_query.replace("\n", "\n    ")
        log_execution(logger, f"Executing search query...{indented_block}", logging.INFO)

        response = hda_client.search(wekeo_query)
        
        # --- BUG FIX: Check if response has results or is fundamentally empty ---
        # Many API clients evaluate to 'False' if empty, or contain a .results list
        try:
            is_empty = not response or len(getattr(response, 'results', [])) == 0
        except Exception:
            # Fallback if the object structure is completely unexpected
            is_empty = False 

        if is_empty:
            log_execution(logger, f"EMPTY RESULT FROM QUERY", logging.WARNING)
            return None
            
        download_dir = os.path.join(base_dir, dataset_id.replace(":", "_"))
        
        log_execution(logger, f"Downloading response to {download_dir}...", logging.INFO)
        response.download(download_dir=download_dir)
        
        gather_tifs_from_zips(
            source_directory=download_dir, 
            target_directory=os.path.join(download_dir, "tif_files"),
            logger=logger
        )
        return download_dir
    
    def generate_data_cube(self, config_path, config_file, logger_name=f"{config_file.split(".")[0]}_wekeo_pipeline.log"):
        logger = self._setup_pipeline_logger(logger_name=logger_name, )



from bmc.cube.spatiotemporal import *
import yaml
import os
import json
from bmc.utils.logger import log_execution

class wekeo_cube(spatiotemporal_cube):
    _PRODUCT_TYPE_MAPPINGS = {
    # --- CROPLAND (CRL) CATEGORICAL PRODUCTS ---
    "Crop Types": {
        1110: "Wheat", 1120: "Barley", 1130: "Maize", 1140: "Rice", 
        1150: "Other_Cereals", 1210: "Fresh_Vegetables", 1220: "Dry_Pulses", 
        1310: "Potatoes", 1320: "Sugar_Beet", 1410: "Sunflower", 1420: "Soybeans", 
        1430: "Rapeseed", 1440: "Flax_Cotton_Hemp", 2100: "Grapes", 2200: "Olives", 
        2310: "Fruits", 2320: "Nuts", 3100: "Unclassified_Arable", 
        3200: "Unclassified_Permanent"
    },
    
    "Secondary Crops Type": {
        1110: "Wheat", 1120: "Barley", 1130: "Maize", 1140: "Rice", 
        1150: "Other_Cereals", 1210: "Fresh_Vegetables", 1220: "Dry_Pulses", 
        1310: "Potatoes", 1320: "Sugar_Beet", 1410: "Sunflower", 1420: "Soybeans", 
        1430: "Rapeseed", 1440: "Flax_Cotton_Hemp", 2100: "Grapes", 2200: "Olives", 
        2310: "Fruits", 2320: "Nuts", 3100: "Unclassified_Arable", 
        3200: "Unclassified_Permanent"
    },
    
    "Fallow Land Presence": {
        1: "Fallow_Land"
    },

    # --- TREE COVER / FOREST (TCF) CATEGORICAL PRODUCTS ---
    "Forest Type": {
        1: "Broadleaved_Forest", 
        2: "Coniferous_Forest"
    },
    
    "Dominant Leaf Type": {
        1: "Broadleaved", 
        2: "Coniferous"
    },

    # --- GRASSLAND (GRA) CATEGORICAL PRODUCTS ---
    "Grassland": {
        1: "Grassland"
    },
    
    "Ploughing Indicator": {
        1: "Ploughed"
    },

    # --- IMPERVIOUSNESS (IMP) CATEGORICAL PRODUCTS ---
    "Impervious Built-up": {
        1: "Impervious_Built_up"
    },

    "Corine Land Cover":{
        1: "111_Continuous_Urban_Fabric",
        2: "112_Discontinuous_Urban_Fabric",
        3: "121_Industrial_Commercial",
        4: "122_Road_Rail_Networks",
        5: "123_Port_Areas",
        6: "124_Airports",
        7: "131_Mineral_Extraction",
        8: "132_Dump_Sites",
        9: "133_Construction_Sites",
        10: "141_Green_Urban_Areas",
        11: "142_Sport_Leisure_Facilities",
        12: "211_Non_Irrigated_Arable_Land",
        13: "212_Permanently_Irrigated_Land",
        14: "213_Rice_Fields",
        15: "221_Vineyards",
        16: "222_Fruit_Trees_Berry_Plantations",
        17: "223_Olive_Groves",
        18: "231_Pastures",
        19: "241_Annual_Crops_with_Permanent",
        20: "242_Complex_Cultivation_Patterns",
        21: "243_Agriculture_with_Natural_Vegetation",
        22: "244_Agro_Forestry_Areas",
        23: "311_Broad_Leaved_Forest",
        24: "312_Coniferous_Forest",
        25: "313_Mixed_Forest",
        26: "321_Natural_Grasslands",
        27: "322_Moors_Heathland",
        28: "323_Sclerophyllous_Vegetation",
        29: "324_Transitional_Woodland_Shrub",
        30: "331_Beaches_Dunes_Sands",
        31: "332_Bare_Rocks",
        32: "333_Sparsely_Vegetated_Areas",
        33: "334_Burnt_Areas",
        34: "335_Glaciers_Perpetual_Snow",
        35: "411_Inland_Marshes",
        36: "412_Peat_Bogs",
        37: "421_Salt_Marshes",
        38: "422_Salines",
        39: "423_Intertidal_Flats",
        40: "511_Water_Courses",
        41: "512_Water_Bodies",
        42: "521_Coastal_Lagoons",
        43: "522_Estuaries",
        44: "523_Sea_Ocean",
        48: "NoData", 
        128: "NoData", 
        254: "Unclassifiable",
        255: "Outside_Area",
        65535: "Outside_Area"
    }
}
    def __init__(self):
        super().__init__()
        self.wekeo_logger = self._setup_pipeline_logger("wekeo_logger", "wekeoCube.log")

    def _fetch_unpack_query(self, wekeo_query, hda_client, base_dir="wekeo", logger=None):
        """
        Fetches data using the hda_client, logs the progress, and extracts .tif files.
        """
        if not logger:
            logger = self.wekeo_logger

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
    
    #def generate_data_cube(self, config_path, config_file, logger_name=f"{config_file.split(".")[0]}_wekeo_pipeline.log"):
    #    logger = self._setup_pipeline_logger(logger_name=logger_name, )



import yaml
import os
import logging
from bmc.lake.wekeo import wekeo_lake

lake_recipe = """
paths:
  base_dir: "/storage/niels/bmc"
  raw_dir: "/storage/niels/bmc/raw/"

lake_name: "micro_lake"

raw_config:
  keep_raw: false
  spatial:
    use_bbox: false

spatial:
  target_grid_key: "EEA_100m"
  resampling_strategies:
    continuous: ["average", "max", "min", "rms"]
    discrete: "coverage" 

temporal:
  start_year: 1990
  start_month: 1
  end_year: 2023
  end_month: 12

sources:
  wekeo:
    enabled: true
    query_resolution: "highest" 
    datasets:
      TCF:
        include: true
        productTypes:
          - "Dominant Leaf Type"
          - "Forest Type"
          - "Tree Cover Density"
          - "Broadleaved Cover Density"
          - "Coniferous Cover Density"
      GRA:
        include: true
        productTypes:
          - "Grassland"
          - "Grassland Mowing Dates (4 Dates per Year)"
          - "Grassland Mowing Events"
          - "Herbaceous Cover"
          - "Ploughing Indicator"
      IMP:
        include: true
        productTypes:
          - "Imperviousness Density"
          - "Impervious Built-up"
          - "Share of Built-up"
      SLF:
        include: true
        productTypes:
          - "Small Woody Features"
          - "Woody Vegetation Layer"
      CRL:
        include: true
        productTypes:
          - "Bare Soil After"
          - "Bare Soil Before"
          - "Cropping Seasons Yearly"
          - "Crop Types"
          - "Fallow Land Presence"
          - "Main Crop Duration"
          - "Main Crop Emergence"
          - "Main Crop Harvest"
          - "Secondary Crops Duration"
          - "Secondary Crops Emergence"
          - "Secondary Crops Type"
      CORINE:
        include: true
        productTypes:
          - "Corine Land Cover 1990"
          - "Corine Land Cover 2000"
          - "Corine Land Cover 2006"
          - "Corine Land Cover 2012"
          - "Corine Land Cover 2018"
"""

recipe = yaml.safe_load(lake_recipe)
lake_dir = recipe["paths"]["base_dir"]

# 1. Initialize your engine
lake_engine = wekeo_lake()

# 2. AUTOMATIC INITIALIZATION: Explicitly pass logger=None. 
# Your code will sense this, automatically configure the file-handler logs, 
# and tie the output path directly inside "/storage/niels/bmc/logs/micro_lake_generation.log"
generated_cogs = lake_engine.build_datalake(recipe, logger=None)

# 3. EXTRACTION: Now capture the automatically constructed logger from the engine instance
auto_logger = lake_engine.logger

# 4. ORCHESTRATION: Pass the captured logger to downstream steps so everything writes to the same log file
is_valid = lake_engine.validate_datalake(lake_dir, logger=auto_logger)

if is_valid:
    lake_engine.generate_catalog(lake_dir, logger=auto_logger)
else:
    if auto_logger:
        auto_logger.error("Datalake QA validation failed! Catalog creation aborted.")






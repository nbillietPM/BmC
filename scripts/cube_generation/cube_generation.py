import yaml
import os
import logging
from bmc.cube.bmd import bmd_cube

pan_eu_recipe="""
base_dir: /storage/niels/bmc/cube_generation
cube_name: europe_chelsa_10km_eea

export:
  as_tree: true
  as_nested_dir: true

spatial:
  target_grid: EEA
  target_resolution: 10km
  use_bbox: true
  bbox:
    long_min: -11
    long_max: 41
    lat_min: 34
    lat_max: 72

temporal:
  start_year: 2018
  start_month: 1
  end_year: 2018
  end_month: 12
sources:
  chelsa:
    enabled: true
    catalog_path: ../../../meta/chelsa_gp_stac/chelsa_master.parquet
    levels:
      daily:
        include: false
      annual:
        include: false
      climatologies:
        include: false
      bioclim:
        include: true
        time_ranges:
          1981-2010: true
          2011-2040: true
          2041-2070: true
          2071-2100: true
        ensembles:
          historical: true
          GFDL-ESM4: true
          IPSL-CM6A-LR: true
          MPI-ESM1-2-HR: true
          MRI-ESM2-0: true
          UKESM1-0-LL: true
        scenarios:
          historical: true
          ssp126: true
          ssp370: true
          ssp585: true
        variables:
          bio01: true
          bio02: true
          bio03: true
          bio04: true
          bio05: true
          bio06: true
          bio07: true
          bio08: true
          bio09: true
          bio10: true
          bio11: true
          bio12: true
          bio13: true
          bio14: true
          bio15: true
          bio16: true
          bio17: true
          bio18: true
          bio19: true
"""
recipe = yaml.safe_load(pan_eu_recipe)
try:
    recipe_data = yaml.safe_load(recipe)
    
    with open("panEU_recipe.yaml", "w", encoding="utf-8") as file:
        yaml.dump(recipe_data, file, default_flow_style=False, sort_keys=False)
        
    print("YAML file successfully generated!")
    
except yaml.YAMLError as exc:
    print(exc)


bmd_engine = bmd_cube()
bmd_engine.generate_bmd_data("panEU_recipe.yaml", "")
bmd_engine.construct_datatree("panEU_recipe.yaml", "")
bmd_engine.export_tree("panEU_recipe.yaml", "")
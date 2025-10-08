from datasource.chelsa import s3 
from datasource.chelsa import layer

import os
import unittest
from unittest.mock import patch, MagicMock
import logging
from pathlib import Path

TEST_DIR = os.path.dirname(os.path.abspath(__file__))
#LOG_DIR = os.path.join(Path.parent(TEST_DIR))
#logging.basicConfig(filename="test_s3_log")

class TestS3Functions(unittest.TestCase):
    def test_format_url_monthly_valid(self):
        dir_path = os.path.join(TEST_DIR, "urls", "monthly")
        diff_ts = ["cmi","pet","sfcWind", "tas", "tasmax", "tasmin", "vpd"]
        variable_url_lists = os.listdir(dir_path)
        for filename in variable_url_lists:
            var_name = filename.split(".")[0]
            filepath = os.path.join(dir_path, filename)
            with open(filepath, "r") as f:
                urls = f.readlines()
                urls = [item.strip() for item in urls]
            url_check = []
            failed = []
            if var_name in diff_ts:
                month_year_pairs = layer.generate_month_year_range(2,12,1979,2018)
                generated_urls = [s3.format_url_month_ts(var=var_name, month=pair[0], year=pair[1]) for pair in month_year_pairs]
                for generated_url in generated_urls:
                    if generated_url in urls:
                        url_check.append(True)
                    else:
                        url_check.append(False)
                        failed.append(generated_url)
            else:
                month_year_pairs = layer.generate_month_year_range(1,12,1979,2018)
                generated_urls = [s3.format_url_month_ts(var=var_name, month=pair[0], year=pair[1]) for pair in month_year_pairs]
                for generated_url in generated_urls:
                    if generated_url in urls:
                        url_check.append(True)
                    else:
                        url_check.append(False)
                        failed.append(generated_url)
            print(failed)
            if set(url_check)=={True}:
                continue
            else:
                self.fail(f"URL generation for variable {set(url_check)} failed the test")


    def test_monthly_invalid_args(self):
        dir_path = os.path.join(TEST_DIR, "urls", "monthly")
        var_names = [filename.split(".")[0] for filename in os.listdir(dir_path)]
        diff_ts = ["cmi","pet","sfcWind", "tas", "tasmax", "tasmin", "vpd"]
        with self.assertRaises(ValueError):
            for var in diff_ts:
                s3.format_url_month_ts(var=var, month=1, year=1979)
            for var in var_names:
                random_year = randint(0,1978)
                s3.format_url_month_ts(var=var, month=1, year=random_year)
                s3.format_url_month_ts(var=var, month=1, year=-random_year)
                s3.format_url_month_ts(var=var, month=0, year=1980)
                s3.format_url_month_ts(var=var, month=13, year=1980)
                s3.format_url_month_ts(var="invalid_var", month=1, year=1980)

    def test_clim_ref_period(self):
        dir_path = os.path.join(TEST_DIR, "urls", "ref")
        filepath = os.path.join(dir_path, "ref_period.txt")
        with open(filepath, "r") as f:
            valid_urls = f.readlines()
            valid_urls = [url.strip() for url in valid_urls]
        var_opt = ['ai','bio10','bio11','bio12','bio13','bio14','bio15','bio16','bio17','bio18','bio19','bio1','bio2','bio3',
               'bio4','bio5','bio6','bio7','bio8','bio9','clt_max','clt_mean','clt_min','clt_range','cmi_max','cmi_mean',
               'cmi_min','cmi_range','fcf','fgd','gdd0','gdd10','gdd5','gddlgd0','gddlgd10','gddlgd5','gdgfgd0','gdgfgd10',
               'gdgfgd5','gsl','gsp','gst','hurs_max','hurs_mean','hurs_min','hurs_range','kg0','kg1','kg2','kg3','kg4','kg5',
               'lgd','ngd0','ngd10','ngd5','npp','pet_penman_max','pet_penman_mean','pet_penman_min','pet_penman_range',
               'rsds_max','rsds_min','rsds_mean','rsds_range','scd','sfcWind_max','sfcWind_mean','sfcWind_min','sfcWind_range',
               'swb','swe','vpd_max','vpd_mean','vpd_min','vpd_range']
        urls = [s3.format_url_clim_ref_period(var) for var in var_opt]
        url_check = [url in valid_urls for url in urls]
        if False in url_check:
            failed_vars = []
            for check, var in zip(url_check, var_opt):
                if check == False:
                    failed_vars.append(var)
            self.fail(f"The following variables {failed_vars} did not generate valid")
        if len(urls)!=len(valid_urls):
            self.fail(f"Not all files possible")

    
    def test_clim_ref_monthly(self):
        dir_path = os.path.join(TEST_DIR, "urls", "ref")
        filepath = os.path.join(dir_path, "ref_month.txt")
        with open(filepath, "r") as f:
            valid_urls = f.readlines()
            valid_urls = [url.strip() for url in valid_urls]
        var_opt=["clt","cmi","hurs","pet","pr","rsds","sfcWind","tas","tasmax","tasmin", "vpd"]
        failed_urls = []
        for var in var_opt:
            for month in range(1, 13):
                generated_url = s3.format_url_clim_ref_monthly(var, month)
                if generated_url in valid_urls:
                    continue
                else:
                    failed_urls.append(generated_url)
        if failed_urls!=[]:
            self.fail(f"Invalid URLs have been generated for the reference monthly data")

    def test_clim_sim_period(self):
        dir_path = os.path.join(TEST_DIR, "urls", "sim")
        filepath = os.path.join(dir_path, "sim_period.txt")
        with open(filepath, "r") as f:
            valid_urls = f.readlines()
            valid_urls = [url.strip() for url in valid_urls]
        var_opt=['bio10','bio11','bio12','bio13','bio14','bio15','bio16','bio17','bio18','bio19','bio1','bio2','bio3',
             'bio4','bio5','bio6','bio7','bio8','bio9','fcf','fgd','gdd0','gdd10','gdd5','gddlgd0','gddlgd10',
             'gddlgd5','gdgfgd0','gdgfgd10','gdgfgd5','gsl','gsp','gst','kg0','kg1','kg2','kg3','kg4','kg5',
             'lgd','ngd0','ngd10','ngd5','npp','scd','swe']
        model_names = ['GFDL-ESM4','IPSL-CM6A-LR','MPI-ESM1-2-HR','MRI-ESM2-0','UKESM1-0-LL']
        scenarios = ["ssp126","ssp370","ssp585"]
        year_ranges = ["2011-2040","2041-2070","2071-2100"]
        failed_urls = []
        for year_range in year_ranges:
            for model_name in model_names:
                for scenario in scenarios:
                    for var in var_opt:
                        generated_url = s3.format_url_clim_sim_period(var, year_range, model_name, scenario)
                        if generated_url in valid_urls:
                            continue 
                        else:
                            failed_urls.append(generated_url)
                            #failed_params.append((var,year_range, model_name, scenario))
        if failed_urls!=[]:
            print(failed_urls)
            self.fail(f"The generated URLs for the period simulation contains invalid URLs")

    def test_clim_sim_monthly_invalid_args(self):
        dir_path = os.path.join(TEST_DIR, "urls", "sim")
        filepath = os.path.join(dir_path, "sim_month.txt")
        with open(filepath, "r") as f:
            valid_urls = f.readlines()
            valid_urls = [url.strip() for url in valid_urls]
        var_opt=["pr", "tas", "tasmax", "tasmin"]
        model_names = ['GFDL-ESM4','IPSL-CM6A-LR','MPI-ESM1-2-HR','MRI-ESM2-0','UKESM1-0-LL']
        scenarios = ["ssp126","ssp370","ssp585"]
        year_ranges = ["2011-2040","2041-2070","2071-2100"]
        failed_urls = []
        for year_range in year_ranges:
            for model_name in model_names:
                for scenario in scenarios:
                    for var in var_opt:
                        for month in range(1,13):
                            generated_url = s3.format_url_clim_sim_month(var, year_range,month, model_name, scenario)
                            if generated_url in valid_urls:
                                continue 
                            else:
                                failed_urls.append(generated_url)
        if failed_urls!=[]:
            print(failed_urls)
            self.fail(f"The generated URLs for the monthly simulation contains invalid URLs")
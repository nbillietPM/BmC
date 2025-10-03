from datasource.chelsa import s3 
from datasource.chelsa import layer

import os
import unittest
from unittest.mock import patch, MagicMock

class TestS3Functions(unittest.TestCase):

    def test_format_url_monthly_valid(self):
        test_dir = os.path.dirname(os.path.abspath(__file__))
        dir_path = os.path.join(test_dir, "urls", "monthly")
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
                month_year_pairs = layer.generate_month_year_range(2,12,1979,2019)
                generated_urls = [s3.format_url_month_ts(var=var_name, month=pair[0], year=pair[1]) for pair in month_year_pairs]
                for generated_url in generated_urls:
                    if generated_url in urls:
                        url_check.append(True)
                    else:
                        url_check.append(False)
                        print(generated_url)
            else:
                month_year_pairs = layer.generate_month_year_range(1,12,1979,2019)
                generated_urls = [s3.format_url_month_ts(var=var_name, month=pair[0], year=pair[1]) for pair in month_year_pairs]
                for generated_url in generated_urls:
                    if generated_url in urls:
                        url_check.append(True)
                    else:
                        url_check.append(False)
                        print(generated_url)
            if set(url_check)==True:
                continue
            else:
                #self.fail(f"URL generation for variable {var_name} failed the test")
                false_indices = [i for i, item in enumerate(url_check) if item is False]
                print(false_indices)
                print(len(urls))
                #print(urls[false_indices[0]])

    def test_monthly_invalid_args(self):
        test_dir = os.path.dirname(os.path.abspath(__file__))
        dir_path = os.path.join(test_dir, "urls", "monthly")
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
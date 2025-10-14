import unittest
import random
from datasource.chelsa import sampling
from datasource.chelsa import layer 

class TestLayerFunctions(unittest.TestCase):
    def setUp(self):
        rand_ulx = random.uniform(-180,180)
        rand_uly = random.uniform(-60,60)
        self.bbox = (rand_ulx, rand_uly-1, rand_ulx+1, rand_uly)

    def test_generate_month_year_range(self):
        year_ts = layer.generate_month_year_range(1,12,2000,2000)
        expected_year_ts = [(i,2000) for i in range(1,13)]
        if not (year_ts==expected_year_ts):
            self.fail("Standard year range (begin_month<end_month & start_year==end_year) deviates from what was expected")
        multi_year_ts_1 = layer.generate_month_year_range(2,3,2000,2001)
        expected_multi_year_ts_1 = [(i,2000) for i in range(2,13)] + [(i,2001) for i in range(1,4)]
        if not (multi_year_ts_1==expected_multi_year_ts_1):
            self.fail("Multi year range (begin_month<end_month & start_year<end_year) deviates from what was expected")
        multi_year_ts_2 = layer.generate_month_year_range(3,2,2000,2001)
        expected_multi_year_ts_2 = [(i,2000) for i in range(3,13)] + [(i,2001) for i in range(1,3)]
        if not (multi_year_ts_2==expected_multi_year_ts_2):
            self.fail("Multi year range (begin_month>end_month & start_year<end_year) deviates from what was expected")
        multi_year_ts_3 = layer.generate_month_year_range(3,3,2000,2001)
        expected_multi_year_ts_3 = [(i,2000) for i in range(3,13)] + [(i,2001) for i in range(1,4)]
        if not (multi_year_ts_3==expected_multi_year_ts_3):
            self.fail("Multi year range (begin_month==end_month & start_year<end_year) deviates from what was expected")

    def test_chelsa_month_ts(self):
        rand_month = random.randint(1,10)
        rand_year = random.randint(1980,2010)
        var_opt = ["clt", "cmi", "hurs", "pet", "pr", "rsds", "sfcWind", "tas", "tasmax", "tasmin", "vpd"]
        test_month_ts = layer.chelsa_month_ts(random.choice(var_opt), self.bbox, rand_month, rand_month+1, rand_year, rand_year)

    def test_chelsa_clim_ref_period(self):
        var_opt = ["ai","bio10","bio11","bio12","bio13","bio14","bio15","bio16","bio17","bio18","bio19","bio1","bio2","bio3",
               "bio4","bio5","bio6","bio7","bio8","bio9","clt_max","clt_mean","clt_min","clt_range","cmi_max","cmi_mean",
               "cmi_min","cmi_range","fcf","fgd","gdd0","gdd10","gdd5","gddlgd0","gddlgd10","gddlgd5","gdgfgd0","gdgfgd10",
               "gdgfgd5","gsl","gsp","gst","hurs_max","hurs_mean","hurs_min","hurs_range","kg0","kg1","kg2","kg3","kg4","kg5",
               "lgd","ngd0","ngd10","ngd5","npp","pet_penman_max","pet_penman_mean","pet_penman_min","pet_penman_range",
               "rsds_max","rsds_min","rsds_mean","rsds_range","scd","sfcWind_max","sfcWind_mean","sfcWind_min","sfcWind_range",
               "swb","swe","vpd_max","vpd_mean","vpd_min","vpd_range"]
        test_chelsa_clim_ref_period_data = layer.chelsa_clim_ref_period(random.choice(var_opt), self.bbox)

    
    def test_chelsa_clim_ref_month(self):
        var_opt=["clt","cmi","hurs","pet","pr","rsds","sfcWind","tas","tasmax","tasmin", "vpd"]
        rand_month = random.randint(1,10)
        test_clim_ref_month_ts = layer.chelsa_clim_ref_month(random.choice(var_opt), self.bbox, [rand_month, rand_month+1])

    def test_chelsa_clim_sim_period(self):
        var_opt=["bio10","bio11","bio12","bio13","bio14","bio15","bio16","bio17","bio18","bio19","bio1","bio2","bio3",
             "bio4","bio5","bio6","bio7","bio8","bio9","fcf","fgd","gdd0","gdd10","gdd5","gddlgd0","gddlgd10",
             "gddlgd5","gdgfgd0","gdgfgd10","gdgfgd5","gsl","gsp","gst","kg0","kg1","kg2","kg3","kg4","kg5",
             "lgd","ngd0","ngd10","ngd5","npp","scd","swe"]
        model_names = ["GFDL-ESM4","IPSL-CM6A-LR","MPI-ESM1-2-HR","MRI-ESM2-0","UKESM1-0-LL"]
        scenarios = ["ssp126","ssp370","ssp585"]
        year_ranges = ["2011-2040","2041-2070","2071-2100"]
        test_clim_sim_period_ts = layer.chelsa_clim_sim_period(var=random.choice(var_opt),
                                                               bbox=self.bbox,
                                                               year_ranges=[random.choice(year_ranges)],
                                                               model_names=[random.choice(model_names)],
                                                               ensemble_members=[random.choice(scenarios)])

    def test_chelsa_clim_sim_month(self):
        var_opt=["pr", "tas", "tasmax", "tasmin"]
        rand_month = random.randrange(1,10)
        model_names = ["GFDL-ESM4","IPSL-CM6A-LR","MPI-ESM1-2-HR","MRI-ESM2-0","UKESM1-0-LL"]
        scenarios = ["ssp126","ssp370","ssp585"]
        year_ranges = ["2011-2040","2041-2070","2071-2100"]
        test_clim_sim_period_ts = layer.chelsa_clim_sim_month(var=random.choice(var_opt),
                                                               bbox=self.bbox,
                                                               year_ranges=[random.choice(year_ranges)],
                                                               months = [rand_month, rand_month+1],
                                                               model_names=[random.choice(model_names)],
                                                               ensemble_members=[random.choice(scenarios)])

import unittest
import tempfile
import os
import numpy as np
import rasterio
from rasterio.transform import from_origin
from rasterio.windows import from_bounds
from affine import Affine
from datasource.chelsa import sampling

# import your function from its module, or place the function above in this file
# from your_module import read_bounding_box
# For demo we assume read_bounding_box is in the same file or imported.

def make_dummy_tif(path, width=100, height=80, ulx=1000.0, uly=2000.0, pixel_size=1.0):
    # raster size and pixel size
    width, height = 512, 384
    pixel_size = 1.0  # map units per pixel

    # construct a simple synthetic image (e.g., gradient + a blob)
    arr = np.zeros((height, width), dtype=np.uint16)
    yy, xx = np.indices(arr.shape)
    arr = (xx + yy) % 256
    arr[150:220, 200:300] += 500  # a bright blob

    # georeference: upper-left corner coordinates and transform
    transform = from_origin(ulx, uly, pixel_size, pixel_size)

    profile = {
        "driver": "GTiff",
        "dtype": arr.dtype,
        "count": 1,
        "width": width,
        "height": height,
        "crs": "EPSG:4326",  # pick whatever CRS you need
        "transform": transform,
    }
    with rasterio.open(path, "w", **profile) as dst:
        dst.write(arr, 1)

class TestReadBoundingBoxValues(unittest.TestCase):
    def setUp(self):
        self.dummy_dir = ("dummy/")
        if not os.path.exists("dummy"):
            os.makedirs("dummy")
        self.tifpath = os.path.join(self.dummy_dir, "dummy.tif")
        self.ulx = 1000.0
        self.uly = 2000.0
        self.pixel = 1.0
        make_dummy_tif(self.tifpath, width=100, height=80, ulx=self.ulx, uly=self.uly, pixel_size=self.pixel)

    def _expected_from_rasterio(self, bbox):
        """Helper that computes expected subset and coordinate grids using rasterio primitives."""
        src = rasterio.open(self.tifpath)
        window = rasterio.windows.from_bounds(*bbox, transform=src.transform)
        window = window.intersection(rasterio.windows.Window(0, 0, src.width, src.height))
        window_transform  = src.window_transform(window)
        if window.width <= 0 or window.height <= 0:
            return np.empty((0,0), dtype=src.dtypes[0]), np.empty((0,0)), np.empty((0,0))
        expected = src.read(1, window=window)
        height, width = expected.shape
        rows, cols = np.meshgrid(np.arange(height), np.arange(width), indexing='ij')
        xs, ys = rasterio.transform.xy(window_transform, rows, cols)
        return xs[:width], ys[::width], expected

    def test_expected(self):
        bbox = (self.ulx, self.uly-5, self.ulx+5, self.uly)
        xs, ys, expected = self._expected_from_rasterio(bbox)
        bool_check = [(xs==np.array([1000.5, 1001.5, 1002.5, 1003.5, 1004.5])).all(),
                      (ys==np.array([1999.5, 1998.5, 1997.5, 1996.5, 1995.5])).all(),
                      (expected==np.array([[0, 1, 2, 3, 4],[1, 2, 3, 4, 5],[2, 3, 4, 5, 6],[3, 4, 5, 6, 7],[4, 5, 6, 7, 8]])).all()]
        if all(bool_check) == False:
            self.fail(f"_expected_from_rasterio method deviates from expected behaviour")

    def test_bbox_intersect_values_and_coords(self):
        # choose bbox that maps to a clear rectangular pixel window
        # e.g., inside the raster so we get non-empty subset
        bbox = (self.ulx, self.uly-5, self.ulx+5, self.uly)

        # call the function under test
        longitudes, latitudes, subset = sampling.read_bounding_box(self.tifpath, bbox, generate_coordinates=True)

        # compute expected via rasterio
        expected_lon, expected_lat,  expected_subset = self._expected_from_rasterio(bbox)

        # exact value equality for pixel values
        np.testing.assert_array_equal(subset, expected_subset, err_msg="subset pixel values differ from expected")

        # exact equality for coordinate arrays (float) within a tight tolerance
        np.testing.assert_allclose(longitudes, expected_lon, rtol=0, atol=1e-9, err_msg="longitude grid mismatch")
        np.testing.assert_allclose(latitudes, expected_lat, rtol=0, atol=1e-9, err_msg="latitude grid mismatch")

if __name__ == "__main__":
    unittest.main()
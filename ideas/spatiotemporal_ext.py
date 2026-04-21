"""
Implementation of distribution metrics during the resampling of categorical data. GDAL does not accept 
custom functions so we can investigate a work around where we write custom out of core processing of tiles
by chunking the dataset and passing our custom functions over it
"""

import numpy as np
from scipy.stats import entropy
from skimage.util import view_as_blocks
from skimage.feature import graycomatrix, graycoprops

def calculate_block_entropy(high_res_array: np.ndarray, downsample_factor: int = 10, nodata_val: int = 255) -> np.ndarray:
    """
    Calculates Shannon Entropy for categorical data.
    Takes a high-res array and compresses it by the downsample_factor.
    """
    # Break array into e.g., 10x10 blocks
    blocks = view_as_blocks(high_res_array, block_shape=(downsample_factor, downsample_factor))
    flat_blocks = blocks.reshape(blocks.shape[0], blocks.shape[1], -1)
    
    new_y, new_x, _ = flat_blocks.shape
    entropy_grid = np.zeros((new_y, new_x), dtype=np.float32)
    
    for i in range(new_y):
        for j in range(new_x):
            block_pixels = flat_blocks[i, j, :]
            
            # Ignore NoData
            valid_pixels = block_pixels[block_pixels != nodata_val]
            if len(valid_pixels) == 0:
                entropy_grid[i, j] = np.nan
                continue
                
            # Calculate Entropy
            _, counts = np.unique(valid_pixels, return_counts=True)
            entropy_grid[i, j] = entropy(counts, base=2)
            
    return entropy_grid


def calculate_glcm_homogeneity(high_res_array: np.ndarray, downsample_factor: int = 10, nodata_val: int = 255) -> np.ndarray:
    """
    Calculates GLCM Homogeneity.
    """
    # GLCM strictly requires positive integers (uint8)
    high_res_array = high_res_array.astype(np.uint8) 
    
    blocks = view_as_blocks(high_res_array, block_shape=(downsample_factor, downsample_factor))
    new_y, new_x = blocks.shape[0], blocks.shape[1]
    
    homogeneity_grid = np.zeros((new_y, new_x), dtype=np.float32)
    
    for i in range(new_y):
        for j in range(new_x):
            block = blocks[i, j, :, :]
            
            # If the entire block is NoData, skip calculation
            if np.all(block == nodata_val):
                homogeneity_grid[i, j] = np.nan
                continue
                
            # Calculate Co-occurrence Matrix (1 pixel away, horizontal)
            glcm = graycomatrix(block, distances=[1], angles=[0], levels=256, symmetric=True, normed=True)
            homogeneity_grid[i, j] = graycoprops(glcm, 'homogeneity')[0, 0]
            
    return homogeneity_grid

import rasterio
from rasterio.windows import Window
from rasterio.transform import Affine
import math
import logging

def process_texture_out_of_core(
    input_path: str, 
    output_path: str, 
    texture_func: callable, 
    downsample_factor: int = 10, 
    chunk_size: int = 5000, 
    nodata_val: int = 255,
    logger: logging.Logger = None
):
    """
    Reads a massive raster in chunks, applies a texture metric function to compress it,
    and writes the result sequentially to a new Cloud Optimized GeoTIFF.

    Parameters
    ----------
    input_path : str
        Path to the raw high-resolution raster or VRT.
    output_path : str
        Path to save the new low-resolution texture COG.
    texture_func : callable
        The function to apply (e.g., calculate_block_entropy).
    downsample_factor : int
        The resolution reduction factor (e.g., 10m to 100m = 10).
    chunk_size : int
        How many high-res pixels to process in memory at once (must be a multiple of downsample_factor).
    """
    if chunk_size % downsample_factor != 0:
        raise ValueError("chunk_size must be perfectly divisible by downsample_factor.")

    with rasterio.open(input_path) as src:
        # Calculate new dimensions
        new_width = src.width // downsample_factor
        new_height = src.height // downsample_factor
        
        # Scale the affine transform matrix mathematically
        new_transform = src.transform * Affine.scale(downsample_factor)
        
        # Build the profile for the output Cloud Optimized GeoTIFF
        profile = src.profile.copy()
        profile.update({
            'driver': 'COG',
            'height': new_height,
            'width': new_width,
            'transform': new_transform,
            'dtype': 'float32',
            'nodata': np.nan,
            'compress': 'deflate'
        })
        
        if logger:
            logger.info(f"Starting chunked texture processing. Target: {new_width}x{new_height} pixels.")
        
        with rasterio.open(output_path, 'w', **profile) as dst:
            
            # Loop through the massive file in RAM-safe chunks
            for row in range(0, src.height, chunk_size):
                for col in range(0, src.width, chunk_size):
                    
                    # Prevent chunks from hanging off the edge of the map
                    current_chunk_height = min(chunk_size, src.height - row)
                    current_chunk_width = min(chunk_size, src.width - col)
                    
                    # Trim edges to ensure they are cleanly divisible by the downsample factor
                    current_chunk_height -= current_chunk_height % downsample_factor
                    current_chunk_width -= current_chunk_width % downsample_factor
                    
                    if current_chunk_height == 0 or current_chunk_width == 0:
                        continue
                        
                    # 1. Read the high-res chunk into RAM
                    read_window = Window(col, row, current_chunk_width, current_chunk_height)
                    data_chunk = src.read(1, window=read_window)
                    
                    # 2. Apply the texture math (Compresses the chunk)
                    compacted_chunk = texture_func(data_chunk, downsample_factor=downsample_factor, nodata_val=nodata_val)
                    
                    # 3. Write the compacted chunk out to the hard drive
                    write_window = Window(
                        col // downsample_factor, 
                        row // downsample_factor, 
                        current_chunk_width // downsample_factor, 
                        current_chunk_height // downsample_factor
                    )
                    dst.write(compacted_chunk, 1, window=write_window)
                    
        if logger:
            logger.info(f"Successfully generated texture COG: {output_path}")
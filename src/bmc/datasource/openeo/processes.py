import numpy as np
import openeo
import openeo.processes as proc

def normalized_sobel_kernel(size):
    """
    Edge detection operator that serves as a discrete differentiation operator. The operator is defined in 
    an odd kernel size, i.e. 3x3, 5x5, etc. The Sobel operator is defined as the product of an averaging and differentiation kernel
    
    Args:
        n (int): Kernel size that will be generentated

    Returns 
        Gx, Gy: The kernels for differentiation in the x and y direction  
    """

    if n % 2 == 0:
        raise ValueError("Kernel size must be odd")
    
    # 1. Generate Smoothing Vector (Pascal Row n-1)
    smoothing_vec = np.poly1d([1, 1])**(size-1)
    smoothing_vec = smoothing_vec.c
    
    # 2. Generate Difference Vector
    diff_poly = (np.poly1d([1, 1])**(size-2)) * np.poly1d([1, -1])
    diff_vec = diff_poly.c
    
    # 3. Create Matrices via Outer Product
    smoothing_vec = smoothing_vec.reshape(-1, 1)
    diff_vec = diff_vec.reshape(1, -1)
    
    Gx = np.dot(smoothing_vec, diff_vec)
    Gy = np.dot(diff_vec.T, smoothing_vec.T)
    
    # 4. Normalize
    # We divide by 2^(2N-3)
    scale_factor = 1 / (2**(2*size - 3))
    
    Gx_norm = Gx * scale_factor
    Gy_norm = Gy * scale_factor
    
    return Gx_norm, Gy_norm

def laplacian_of_gaussian(size, sigma=None, normalize_scale=False):
    """
    Generates a normalized discrete Laplacian of Gaussian (LoG) kernel. 
    The Laplacian of Gaussian (LoG) is an image processing operator that combines Gaussian blurring (noise reduction) 
    and Laplacian sharpening (edge detection) to identify areas of rapid intensity change
    
    Args:
        size (int): The size of the kernel (must be odd, e.g., 5, 7, 15).
        sigma (float): The standard deviation of the Gaussian. 
                       If None, it is auto-calculated to fit the size.
        normalize_scale (bool): If True, multiplies by sigma^2 (scale invariance).
                                Use this if comparing blobs across different sizes.
    
    Returns:
        np.array: The NxN LoG kernel.
    """
    if size % 2 == 0:
        raise ValueError("Kernel size must be odd.")
    
    # 1. Determine Sigma if not provided
    # OpenCV standard rule: sigma = 0.3*((ksize-1)*0.5 - 1) + 0.8
    if sigma is None:
        sigma = 0.3 * ((size - 1) * 0.5 - 1) + 0.8
    
    # 2. Create Coordinate Grid
    k = size // 2
    x = np.arange(-k, k + 1)
    y = np.arange(-k, k + 1)
    X, Y = np.meshgrid(x, y)
    
    # 3. Calculate LoG Formula
    # Formula: (-1 / (pi * sigma^4)) * (1 - (x^2+y^2)/(2*sigma^2)) * exp(...)
    r2 = X**2 + Y**2
    exponent = np.exp(-r2 / (2 * sigma**2))
    
    # The term inside the bracket
    bracket = 1 - (r2 / (2 * sigma**2))
    
    # The pre-factor
    # Note: We can simplify the constant because we force sum=0 later,
    # but keeping it makes the math 'real'.
    factor = -1 / (np.pi * sigma**4)
    
    kernel = factor * bracket * exponent
    
    # 4. Enforce Zero-Sum (Crucial for Laplacian)
    # Due to discrete sampling, the mathematical LoG might not sum to exactly 0.
    # We subtract the mean to force the sum to 0.
    kernel = kernel - kernel.mean()
    
    # 5. Optional: Scale Normalization (for Blob Detection)
    # If detecting blobs, larger sigmas produce weaker responses naturally.
    # Multiplying by sigma^2 corrects this.
    if normalize_scale:
        kernel = kernel * (sigma**2)
        
    return kernel

def merge_list(list_cubes):
    cube = list_cubes[0]
    for band in list_cubes[1:]:
        cube.merge_cubes(band)
    return cube

def fractional_class_map(class_dict, cube, band_name, target_resolution, target_crs, method="bilinear"):
    """
    Converts a discrete data layer to a fractional data layer. The original layer is resampled
    to 
    """
    fractional_bands = []
    for name, value in class_dict.items():
        #Project out the pixels that contain the class
        boolean_mask = (cube.band(band_name)==value)

        #Convert boolean mask to float mask to allow fractional values
        float_mask = boolean_mask*1.0

        #Resample the cube to the desired resolution and CRS
        resampled = float_mask.resample_spatial(resolution=target_resolution, 
                                                projection=target_crs, 
                                                method=method)
        
        #Rename resampled band to the class name
        labeled_band = resampled.add_dimension(name="bands",
                                               label=name,
                                               type="bands")
        
        fractional_bands.append(resampled)
    
    #Construct the fractional cube 
    fractional_cube = merge_list(fractional_bands)

    return fractional_cube

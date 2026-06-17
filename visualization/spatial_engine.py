import os
from graphviz import Digraph

# Ensure the output directory exists
os.makedirs('./out/spatial_engine', exist_ok=True)

# =====================================================================
# 1. The Core Reprojection Pipeline (affine_reproject)
# =====================================================================
def diagram_affine_reproject():
    dot = Digraph(comment='affine_reproject Pipeline')
    dot.attr(rankdir='TB', fontname='Helvetica', dpi='300')
    
    dot.node('Start', 'Input: Data & Target Grid\n(affine_reproject)', shape='oval', style='filled', fillcolor='lightblue')
    dot.node('Specs', 'Fetch Target Grid Specs\n(CRS, Resolution, Master Bounds)', shape='box')
    dot.node('Resampler', 'Map Resampler String to\nGDAL Integer', shape='box')
    
    # Decision Branch
    dot.node('CheckInput', 'Is Input a Lazy\nxarray Object?', shape='diamond', style='filled', fillcolor='lightyellow')
    
    # Xarray Path
    dot.node('Sanitize', 'Call _sanitize_spatial_geometry()', shape='box', style='filled', fillcolor='lightgreen')
    dot.node('ToRaster', 'Stream to Temp Disk File\n(rio.to_raster)', shape='box')
    
    # File Path Path
    dot.node('OpenRaster', 'Open File Metadata\n(rioxarray.open_rasterio)', shape='box')
    
    # Merge Path
    dot.node('Transform', 'Transform Bounding Box\n(Target CRS -> Source CRS)', shape='box')
    dot.node('Snap', 'Mathematical Grid Snapping\n(np.floor / np.ceil)', shape='box', style='filled', fillcolor='lightcoral')
    dot.node('Warp', 'Execute Out-of-Core GDAL Warp\n(gdal.Warp)', shape='box', style='filled', fillcolor='lightcoral')
    dot.node('Cleanup', 'Delete Temp Disk Files', shape='box')
    dot.node('Return', 'Return Chunked DataArray\n(rioxarray.open_rasterio)', shape='oval', style='filled', fillcolor='lightblue')

    # Edges
    dot.edge('Start', 'Specs')
    dot.edge('Specs', 'Resampler')
    dot.edge('Resampler', 'CheckInput')
    
    dot.edge('CheckInput', 'Sanitize', label='Yes')
    dot.edge('Sanitize', 'ToRaster')
    dot.edge('ToRaster', 'Transform')
    
    dot.edge('CheckInput', 'OpenRaster', label='No (String Path)')
    dot.edge('OpenRaster', 'Transform')
    
    dot.edge('Transform', 'Snap')
    dot.edge('Snap', 'Warp')
    dot.edge('Warp', 'Cleanup')
    dot.edge('Cleanup', 'Return')

    return dot

# =====================================================================
# 2. The Safe Envelope Builder
# =====================================================================
def diagram_safe_envelope():
    dot = Digraph(comment='build_safe_fetch_envelope Pipeline')
    dot.attr(rankdir='TB', fontname='Helvetica', dpi='300')
    
    dot.node('Start', 'Input: Target Grid & Source CRS\n(build_safe_fetch_envelope)', shape='oval', style='filled', fillcolor='lightblue')
    dot.node('Resolve', 'Resolve Master & Source Grids\nfrom GRID_REGISTRY', shape='box')
    dot.node('Densify', 'Vectorized Perimeter Densification\n(100 points per edge)', shape='box', style='filled', fillcolor='lightcoral')
    dot.node('Project', 'PyProj Coordinate Transformation\n(Target CRS -> Source CRS)', shape='box', style='filled', fillcolor='lightcoral')
    dot.node('CheckNaN', 'Are Coordinates\nFinite & Valid?', shape='diamond', style='filled', fillcolor='lightyellow')
    dot.node('Buffer', 'Apply Resampling Safety Buffer\n(Source Res * Pixel Buffer)', shape='box')
    dot.node('Guardrails', 'Apply WGS84 Geographic Guardrails\n(Clamp to -180/180, -90/90)', shape='box')
    dot.node('Return', 'Return Safe Bounding Box\n(minx, miny, maxx, maxy)', shape='oval', style='filled', fillcolor='lightblue')

    dot.edge('Start', 'Resolve')
    dot.edge('Resolve', 'Densify')
    dot.edge('Densify', 'Project')
    dot.edge('Project', 'CheckNaN')
    dot.edge('CheckNaN', 'Buffer', label='Yes')
    dot.edge('Buffer', 'Guardrails')
    dot.edge('Guardrails', 'Return')

    return dot

# =====================================================================
# 3. The Metadata Sanitizer
# =====================================================================
def diagram_sanitize_geometry():
    dot = Digraph(comment='_sanitize_spatial_geometry Pipeline')
    dot.attr(rankdir='TB', fontname='Helvetica', dpi='300')
    
    dot.node('Start', 'Input: Dirty xarray Object\n(_sanitize_spatial_geometry)', shape='oval', style='filled', fillcolor='lightblue')
    dot.node('Rename', 'Standardize Axis Names\n(lon/lat -> x/y)', shape='box')
    dot.node('SetDims', 'Set Spatial Dimensions\n(rio.set_spatial_dims)', shape='box')
    dot.node('CheckCRS', 'Is CRS Missing?', shape='diamond', style='filled', fillcolor='lightyellow')
    
    dot.node('SetCRS', 'Inject Default CRS\n(EPSG:4326)', shape='box', style='filled', fillcolor='lightgreen')
        
    dot.node('ClearEnc', 'Clear Dimension Disk Encoding\n(encoding.clear())', shape='box')
    dot.node('FloatDrift', 'Erase Floating-Point Drift\n(np.linspace interpolation)', shape='box', style='filled', fillcolor='lightcoral')
    dot.node('Return', 'Return Sanitized Object', shape='oval', style='filled', fillcolor='lightblue')

    dot.edge('Start', 'Rename')
    dot.edge('Rename', 'SetDims')
    dot.edge('SetDims', 'CheckCRS')
    dot.edge('CheckCRS', 'SetCRS', label='Yes')
    dot.edge('CheckCRS', 'ClearEnc', label='No')
    dot.edge('SetCRS', 'ClearEnc')
    dot.edge('ClearEnc', 'FloatDrift')
    dot.edge('FloatDrift', 'Return')

    return dot

# =====================================================================
# 4. Fractional Coverage Chaining
# =====================================================================
def diagram_class_fraction():
    dot = Digraph(comment='compute_class_fraction Pipeline')
    dot.attr(rankdir='TB', fontname='Helvetica', dpi='300')
    
    dot.node('Start', 'Input: Categorical Raster & Target Class\n(compute_class_fraction)', shape='oval', style='filled', fillcolor='lightblue')
    dot.node('Mask', 'Generate Binary Mask Array\n(1 for Target, NaN for Others)', shape='box')
    dot.node('Metadata', 'Write CRS, Transform, & NoData\nto Binary Mask', shape='box')
    dot.node('UUID', 'Generate Unique Virtual Path\n(/vsimem/temp_frac_UUID.tif)', shape='box')
    
    dot.node('Affine', 'Call: affine_reproject()\nRule: "average"', shape='box', style='filled', fillcolor='lightgreen')
    
    dot.node('LoadRAM', 'Force Data into Python RAM\n(.load())', shape='box')
    dot.node('Unlink', 'Clear GDAL C++ Memory\n(gdal.Unlink)', shape='box')
    dot.node('Return', 'Return Continuous\nFractional Coverage', shape='oval', style='filled', fillcolor='lightblue')

    dot.edge('Start', 'Mask')
    dot.edge('Mask', 'Metadata')
    dot.edge('Metadata', 'UUID')
    dot.edge('UUID', 'Affine')
    dot.edge('Affine', 'LoadRAM')
    dot.edge('LoadRAM', 'Unlink')
    dot.edge('Unlink', 'Return')

    return dot

# =====================================================================
# Render and Export Commands
# =====================================================================

print("Generating high-resolution diagrams...")

# 1. Reprojection
dot_reproject = diagram_affine_reproject()
dot_reproject.render('./out/spatial_engine/affine_reproject_pipeline', format='png', cleanup=True)

# 2. Envelope Builder
dot_envelope = diagram_safe_envelope()
dot_envelope.render('./out/spatial_engine/safe_envelope_pipeline', format='png', cleanup=True)

# 3. Metadata Sanitizer
dot_sanitize = diagram_sanitize_geometry()
dot_sanitize.render('./out/spatial_engine/sanitize_geometry_pipeline', format='png', cleanup=True)

# 4. Fractional Coverage
dot_fraction = diagram_class_fraction()
dot_fraction.render('./out/spatial_engine/class_fraction_pipeline', format='png', cleanup=True)

print("Export complete! Check your ./out/spatial_engine/ directory for crisp, high-quality PNGs.")
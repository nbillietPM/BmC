import os
from graphviz import Digraph

# Ensure the output directory exists
os.makedirs('./out/spatiotemporal_cube', exist_ok=True)

def diagram_process_cube():
    dot = Digraph(comment='process_cube Pipeline')
    dot.attr(rankdir='TB', fontname='Helvetica', dpi='300')
    
    # Legend or Input
    dot.node('Start', 'Input: YAML Recipe & Config\n(process_cube)', shape='oval', style='filled', fillcolor='lightblue')
    
    # 1. Plan Generation
    dot.node('Plan', '@abstractmethod\ngenerate_execution_plan()', shape='box', style='filled', fillcolor='lightpink', penwidth='2')
    
    # 2. Grid Resolution
    dot.node('Grid', '@abstractmethod\nresolve_target_grid()', shape='box', style='filled', fillcolor='lightpink', penwidth='2')
    dot.node('Bounds', 'Calculate Rectangular Bounds\n(transform_bounds)', shape='box')
    
    # 3. Data Fetching
    dot.node('Fetch', 'Parallel Raw Data Fetch\n(parallel_fetch_rasters)', shape='box', style='filled', fillcolor='lightgray')
    
    # 4. Metadata Parsing Loop
    dot.node('MetaLoop', 'Loop Fetched Assets', shape='diamond', style='filled', fillcolor='lightyellow')
    dot.node('MetaParse', '@abstractmethod\nparse_metadata()', shape='box', style='filled', fillcolor='lightpink', penwidth='2')
    dot.node('Binning', 'Bin by Level & Variable', shape='box')
    
    # 5. Harmonization Loop
    dot.node('HarmonizeLoop', 'Loop Level & Variables', shape='diamond', style='filled', fillcolor='lightyellow')
    dot.node('Concat', 'Z-Axis Stacking\n(xr.concat)', shape='box')
    dot.node('ResampleRule', '@abstractmethod\nget_resample_rule()', shape='box', style='filled', fillcolor='lightpink', penwidth='2')
    
    dot.node('Warp', 'Affine Reprojection\n(self.affine_reproject)', shape='box', style='filled', fillcolor='lightgreen')
    dot.node('Clip', 'Mathematical Rectangular Clip\n(rio.clip_box)', shape='box')
    
    # 6. Final Formatting
    dot.node('Merge', 'Merge Variables into Level Cube\n(xr.merge)', shape='box')
    dot.node('MultiIndex', '@abstractmethod\napply_multi_index()', shape='box', style='filled', fillcolor='lightpink', penwidth='2')
    
    dot.node('Return', 'Output: Dict[str, xr.Dataset]\nFinal Data Cubes', shape='oval', style='filled', fillcolor='lightblue')

    # Define the Edges (Flow)
    dot.edge('Start', 'Plan', label=' 1. Parse Config')
    dot.edge('Plan', 'Grid', label=' 2. Execution Queue Ready')
    dot.edge('Grid', 'Bounds', label=' 3. Target CRS Locked')
    dot.edge('Bounds', 'Fetch', label=' 4. Build Envelope')
    dot.edge('Fetch', 'MetaLoop', label=' 5. Raw Rasters')
    
    dot.edge('MetaLoop', 'MetaParse', label=' For each raster')
    dot.edge('MetaParse', 'Binning', label=' Inject Z-Coordinates')
    dot.edge('Binning', 'MetaLoop', label=' Next raster')
    dot.edge('Binning', 'HarmonizeLoop', label=' All parsed')
    
    dot.edge('HarmonizeLoop', 'Concat', label=' For each variable list')
    dot.edge('Concat', 'ResampleRule', label=' 3D Array Created')
    dot.edge('ResampleRule', 'Warp', label=' Resampler String')
    dot.edge('Warp', 'Clip', label=' Out-of-Core Processing')
    dot.edge('Clip', 'HarmonizeLoop', label=' Next variable')
    
    dot.edge('HarmonizeLoop', 'Merge', label=' All variables warped')
    dot.edge('Merge', 'MultiIndex', label=' Single Dataset Created')
    dot.edge('MultiIndex', 'Return', label=' Finalized')

    return dot

# Render and Export Commands
print("Generating spatiotemporal process_cube diagram...")

dot_process = diagram_process_cube()
dot_process.render('./out/spatiotemporal_cube/process_cube_pipeline', format='png', cleanup=True)

print("Export complete! Check your ./out/spatiotemporal_cube/ directory.")
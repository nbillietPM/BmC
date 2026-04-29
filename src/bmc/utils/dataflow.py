from graphviz import Digraph

def plot_wekeoLake_data_flow():
    dot = Digraph(comment='Data Lake Ingestion Flow')
    dot.attr(rankdir='LR') # Left to Right layout
    
    # Add nodes (your data files)
    dot.node('A', 'Raw WEkEO Zip')
    dot.node('B', 'Extracted TIFs')
    dot.node('C', 'Virtual Mosaic (.vrt)')
    dot.node('D', 'Binary Mask (RAM)')
    dot.node('E', 'Fractional COG (Disk)')

    # Add edges (the functions that transform the data)
    dot.edge('A', 'B', label='_fetch_unpack_query')
    dot.edge('B', 'C', label='build_virtual_mosaic')
    dot.edge('C', 'D', label='xarray mask')
    dot.edge('D', 'E', label='affine_reproject')

    dot.render('wekeoLake_data_flow_map.gv', view=True)

plot_wekeoLake_data_flow()
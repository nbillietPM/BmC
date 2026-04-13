import matplotlib.pyplot as plt
import matplotlib.colors as mcolors
import matplotlib.patches as mpatches
import xarray as xr
from typing import Optional

def visualize_class(
    ds: xr.DataArray, 
    target_class: int, 
    output_filepath: Optional[str] = None
):
    """
    Visualizes a binary mask for a specific class. 
    Shows the plot if no output_filepath is provided, otherwise saves it to disk.
    """
    # Create a Boolean Mask (True where target_class, False everywhere else)
    # .squeeze() ensures we drop the band dimension if it exists
    binary_mask = (ds.squeeze() == target_class)

    # Create a high-contrast 2-color colormap
    # 0 (False/Absent) -> Light Gray
    # 1 (True/Present) -> Bright Red
    binary_cmap = mcolors.ListedColormap(['#e0e0e0', '#ff0000'])

    # Setup and plot
    fig, ax = plt.subplots(figsize=(10, 8))

    binary_mask.plot.imshow(
        ax=ax,
        cmap=binary_cmap,
        add_colorbar=False,     # Disable the default colorbar
        interpolation='nearest' # Keep the pixel edges razor sharp
    )

    ax.set_title(f"Presence of Class {target_class}", fontsize=16)
    ax.set_aspect('equal')
    ax.axis('off')

    # Add a legend
    legend_elements = [
        mpatches.Patch(color='#e0e0e0', label='Absent (Other Classes)'),
        mpatches.Patch(color='#ff0000', label=f'Present (Class {target_class})')
    ]
    ax.legend(handles=legend_elements, loc='upper right', framealpha=0.9)

    plt.tight_layout()

    # The Logic for Saving vs Displaying
    if output_filepath:
        # Save the figure with high resolution and tight borders
        plt.savefig(output_filepath, dpi=300, bbox_inches='tight')
        
        # Close the figure to free up memory and prevent it from rendering in the console
        plt.close(fig) 
        print(f"Plot successfully saved to: {output_filepath}")
    else:
        # Display in the runtime
        plt.show()
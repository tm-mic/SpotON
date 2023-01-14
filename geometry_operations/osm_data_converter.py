# Responsible for Module: Max W.; written with support by Christian, Thomas
# osm_data_converter deletes unnecessary data from the original osm data.
# Prepares the data for geometry operations.


import geopandas as gpd
import numpy as np


def slim_osm_data(Path: str):
    """Deletes unnecessary columns from the original osm GeoDataFrames."""
    source = gpd.read_file(Path,encoding='utf-8')
    source = source[['id', 'geometry']]
    source.insert(1, "value", np.nan, True)
    source = source.fillna(0)
    source = source.to_crs(crs='EPSG:3035')
    return source

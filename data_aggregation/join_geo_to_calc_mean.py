import pandas as pd

from geometry_operations import polygon_from_crs_creator, coord_to_polygon


def get_crs_df_for_gemeinde(crs_dataframe: pd.DataFrame, path_to_gemeinde_shp: str):
    """Creates geodataframe of """
    grid_geometry = polygon_from_crs_creator.polygon_creator_by_crs_values(crs_dataframe)
    gemeinde_shapes = coord_to_polygon.load_gemeinde_polygon_to_gdf(path_to_gemeinde_shp)
    return coord_to_polygon.intersection_of_base_polygon_and_grid(grid_geometry, gemeinde_shapes)

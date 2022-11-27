import geopandas

from geometry_operations import coord_to_polygon


def polygon_creator_by_crs_values(df_to_transform_to_gdf_by_crs_column, crs_column="Gitter_ID_100m_neu", crs='EPSG:3035'):
    polygon_df = coord_to_polygon.create_coordinate_tuple_dataframe(
        df_to_transform_to_gdf_by_crs_column[crs_column].unique())
    polygon_df_new = coord_to_polygon.add_coordinate_tuple_plus100_to_dataframe(polygon_df)
    polygon_df_new = coord_to_polygon.create_polygon_tuples_from_dataframe(polygon_df_new)
    polygon_df_new = coord_to_polygon.create_polygon_point_list(polygon_df_new)
    polygon_df_new = coord_to_polygon.create_shapely_polygons(polygon_df_new)
    polygon_df_new = coord_to_polygon.create_geodataframe(polygon_df, polygon_df_new)
    polygon_df_new.rename(columns={0: crs_column}, inplace=True)
    df_to_transform_to_gdf_by_crs_column = df_to_transform_to_gdf_by_crs_column.merge(polygon_df_new,
                                                                                      on=crs_column,
                                                                                      how="left")
    return geopandas.GeoDataFrame(df_to_transform_to_gdf_by_crs_column, crs=crs)

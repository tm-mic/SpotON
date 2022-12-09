from geometry_operations.coord_to_polygon import *

def get_crs_by_bundesland(gitter_path: str, shapefile_path: str):

    """Creates geodataframe out of geogitter"""
    gitter_gdf = gpd.GeoDataFrame.from_file(gitter_path)
    gitter_gdf['Breitengrad'] = gitter_gdf['y_mp'].str.replace(',', '.').astype(float)
    gitter_gdf['Laengengrad'] = gitter_gdf['x_mp'].str.replace(',', '.').astype(float)
    gitter_gdf['geometry'] = gitter_gdf.apply(lambda x: Point((float(x.Laengengrad), float(x.Breitengrad))),
                                              axis=1)

    gitter_gdf = gitter_gdf.set_crs(crs='EPSG:3035', epsg='EPSG:3857')
    gitter_gdf = gitter_gdf.to_crs(crs='EPSG:3035')

    gitter_gdf = gitter_gdf.loc[:, gitter_gdf.columns.intersection(['id', 'geometry'])]

    buffer_centroid_points_gdf = gitter_gdf.copy()
    buffer_centroid_points_gdf['geometry'] = gitter_gdf['geometry'].buffer(50000, cap_style=3)

    """Imports shapefile of Bundeslaender borders"""
    bundesland_polygon_gdf = gpd.read_file(shapefile_path,
                                           encoding = 'utf-8')

    bundesland_polygon_gdf = bundesland_polygon_gdf[["GEN", "ARS", "BEZ", "geometry"]]
    bundesland_polygon_gdf['ARS'] = bundesland_polygon_gdf['ARS'].str.slice(0, 2)
    bundesland_polygon_gdf.rename(columns={'GEN': 'NAME'}, inplace=True)
    bundesland_polygon_gdf = bundesland_polygon_gdf.to_crs(crs='EPSG:3035')

    """sjoin buffer_centroid_points_gdf and bundesland_polygon_gdf"""

    geogitter_bundeslaender_gdf = buffer_centroid_points_gdf.sjoin(bundesland_polygon_gdf)

    return geogitter_bundeslaender_gdf
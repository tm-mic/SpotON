if __name__ == '__main__':

    import pandas as pd
    #pd.set_option('display.max_rows', None)
    pd.set_option('display.max_columns', None)
    pd.set_option('display.width', 1000)
    pd.set_option('display.colheader_justify', 'center')
    pd.set_option('display.precision', 2)
    import geopandas as gpd
    import coord_to_polygon
    import IandO
    import matplotlib
    import matplotlib.pyplot as plt
    import numpy as np
    import shapely
    from shapely.ops import nearest_points
    from shapely.geometry import Point
    from shapely.geometry import Polygon
    import geopy
    import geopy.distance as distance
    import folium
    from pyproj import Proj, transform, Transformer
    import time



"""start = time.time()"""

keys = ["Haushalte", "Bevoelkerung", "Ladestationen"]

"""1step"""
crs_column = IandO.import_all_config_specified_csv(["Haushalte"],r"C:\Users\maxwa\PycharmProjects\pythonProject_geopandas\spoton\fileconfig.txt",nrows=5000)
#print(crs_column)

"""2step"""
crs_unique = coord_to_polygon.get_unique_crs(crs_column, keys)
#print(crs_unique)

"""3step"""
original_coordinate_tuple = coord_to_polygon.create_coordinate_tuple_dataframe(crs_unique)
#print(original_coordinate_tuple)

"""4step"""
original_coordinate_tuple_plus_100 = coord_to_polygon.add_coordinate_tuple_plus100_to_dataframe(original_coordinate_tuple)
#print(original_coordinate_tuple_plus_100)

"""5step"""
coordinate_dataframe_with_polygon_tuples = coord_to_polygon.create_polygon_tuples_from_dataframe(original_coordinate_tuple_plus_100)
#print(coordinate_dataframe_with_polygon_tuples)

"""6step"""
coordinate_dataframe_with_polygon_tuples_list = coord_to_polygon.create_polygon_point_list(coordinate_dataframe_with_polygon_tuples)
#print(coordinate_dataframe_with_polygon_tuples_list)

"""7step"""
shapely_polygon_list = coord_to_polygon.create_shapely_polygons(coordinate_dataframe_with_polygon_tuples)
#print(shapely_polygon_list)

"""8step"""
polygon_grid_gdf = coord_to_polygon.create_geodataframe(original_coordinate_tuple,shapely_polygon_list)
#print(polygon_grid_gdf)

"""8.1step"""
centroid_points_gdf = coord_to_polygon.centroids_of_polygon_grid(polygon_grid_gdf)
#print(centroid_points_gdf)

"""8.2step"""
buffer_centroid_points_gdf = coord_to_polygon.buffer_centroid_points(centroid_points_gdf)
#print(buffer_centroid_points_gdf)

"""9step"""
base_polygon_gdf = coord_to_polygon.load_base_polygon_to_gdf(r"C:\Users\maxwa\Documents\Universität\Master\Wintersemester 2022\KINF Projekt\Shapefiles\zulassungskreise\kfz250.utm32s.shape\kfz250\KFZ250.shp")
#print(base_polygon_gdf)

"""10step"""
zulassungskreis = coord_to_polygon.choose_zulassungskreis_from_gdf_by_name(base_polygon_gdf, "Oberallgäu")
#print(zulassungskreis)

"""11step"""
bundesland = coord_to_polygon.choose_all_zulassungskreise_by_bundesland(base_polygon_gdf, "09")
#print(bundesland)

"""12step"""
insec_base_grid_gdf = coord_to_polygon.intersection_of_base_polygon_and_grid(bundesland,polygon_grid_gdf)
#print(insec_base_grid_gdf)

"""13step"""
ladestationen_gdf = coord_to_polygon.ladestationen_to_gdf(r"C:\Users\maxwa\Documents\Universität\Master\Wintersemester 2022\KINF Projekt\Ladesaeulenregister_CSV_überarbeitung.csv")
#print(ladestationen_gdf)

"""14step"""
zubu_ladestationen = coord_to_polygon.insec_ladestationen_gdf_with_zu_or_bu(bundesland, ladestationen_gdf)
#print(zubu_ladestationen)

"""15step"""
polygon_grid_ladestationen_gdf = coord_to_polygon.insec_polygon_grid_with_ladestationen(zubu_ladestationen,polygon_grid_gdf)
#print(polygon_grid_ladestationen_gdf)


"""xstep - plotting the gdf"""
#plot_polygon_grid_gdf = coord_to_polygon.plot_geodataframe(insec_base_grid_gdf)


#nearest = centroid_points_gdf.copy()
#print(nearest)






"""csv import which did not work cuz i dont know"""
#ladesaeulen_csv = IandO.import_all_config_specified_csv(["Ladestationen"],r"C:\Users\maxwa\PycharmProjects\pythonProject_geopandas\spoton\fileconfig.txt",nrows=1)
#ladesaeulen_df = pd.DataFrame.from_dict(ladesaeulen_csv, orient='index', columns=['Breitengrad', 'Laengengrad', 'Normalladeeinrichtung', 'Anzahl Ladepunkte'])
#ladesaeulen_df = pd.DataFrame.from_dict(ladesaeulen_csv)
#print(ladesaeulen_csv)



"""end = time.time()
elapsed_time = end - start
print("Duration:", elapsed_time, "in seconds")"""

if __name__ == '__main__':

    import pandas as pd
    import geopandas as gpd
    import coord_to_polygon
    import IandO
    import matplotlib
    import matplotlib.pyplot as plt
    import shapely
    from shapely.geometry import Point
    from shapely.geometry import Polygon
    import geopy
    import geopy.distance as distance
    import folium
    import time



"""start = time.time()"""

keys = ["Haushalte", "Bevoelkerung"]

"""1step"""
crs_column = IandO.import_all_config_specified_csv(["Haushalte"],r"C:\Users\maxwa\PycharmProjects\pythonProject_geopandas\spoton\fileconfig.txt",nrows=100000)
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

"""9step"""
base_polygon_gdf = coord_to_polygon.load_base_polygon_to_gdf(r"C:\Users\maxwa\Documents\Universität\Master\Wintersemester 2022\KINF Projekt\Shapefiles\einzelne landkreise\DEBKGID_DEBKGDL20000DZCX.shp")
#print(base_polygon_gdf)

"""10step"""
insec_base_grid_gdf = coord_to_polygon.intersection_of_base_polygon_and_grid(base_polygon_gdf,polygon_grid_gdf)
print(insec_base_grid_gdf)


"""xstep - plotting the gdf"""
plot_polygon_grid_gdf = coord_to_polygon.plot_geodataframe(insec_base_grid_gdf)


"""end = time.time()
elapsed_time = end - start
print("Duration:", elapsed_time, "in seconds")"""

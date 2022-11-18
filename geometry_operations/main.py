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
    from pyproj import Proj, transform, Transformer
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

"""xstep - plotting the gdf"""
#plot_polygon_grid_gdf = coord_to_polygon.plot_geodataframe(insec_base_grid_gdf)


"""csv import which did not work cuz i dont know"""
#ladesaeulen_csv = IandO.import_all_config_specified_csv(["Ladestationen"],r"C:\Users\maxwa\PycharmProjects\pythonProject_geopandas\spoton\fileconfig.txt",nrows=1)
#ladesaeulen_df = pd.DataFrame.from_dict(ladesaeulen_csv, orient='index', columns=['Breitengrad', 'Laengengrad', 'Normalladeeinrichtung', 'Anzahl Ladepunkte'])
#ladesaeulen_df = pd.DataFrame.from_dict(ladesaeulen_csv)
#print(ladesaeulen_df)


"""Changed original csv because config did not work.
Creates gdf with all Ladestationen in Germany. Transfers it in EPSG:3035."""
ladestationen = gpd.GeoDataFrame.from_file(r"C:\Users\maxwa\Documents\Universität\Master\Wintersemester 2022\KINF Projekt\Ladesaeulenregister_CSV_überarbeitung.csv")
ladestationen['Breitengrad'] = ladestationen['Breitengrad'].str.replace(',','.').astype(float)
ladestationen['Laengengrad'] = ladestationen['Laengengrad'].str.replace(',','.').astype(float)
ladestationen['geometry'] = ladestationen.apply(lambda x: Point((float(x.Laengengrad), float(x.Breitengrad))), axis=1)
del ladestationen['Breitengrad']
del ladestationen['Laengengrad']
ladestationen = ladestationen.set_crs(crs='WGS 84', epsg='EPSG:3857')
ladestationen = ladestationen.to_crs(crs='EPSG:3035')
#print(ladestationen)


"""Intersections of bundesland/zulassungskreis polygons with ladestationen gdf."""
#ladestationenzulassungskreis = ladestationen.sjoin(zulassungskreis)
#ladestationenbayern = ladestationen.sjoin(bundesland)
#print(ladestationenzulassungskreis)

"""Intersection of polygon grid with ladestationen of one Zulassungskreis.
Does not work. I think needs changes in the order of columns, maybe."""
#test = polygon_grid_gdf.sjoin(ladestationenzulassungskreis)
#print(test)
#coord_to_polygon.plot_geodataframe(test)

"""Intersection of polygon grid with ladestationen gdf.
Dont know how useful this is, because a ladestation not in a square 
is not mapped."""
test2 = ladestationen.sjoin(polygon_grid_gdf)
print(test2)
coord_to_polygon.plot_geodataframe(test2)



"""end = time.time()
elapsed_time = end - start
print("Duration:", elapsed_time, "in seconds")"""

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

keys = ["Haushalte", "Bevoelkerung"]

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
print(shapely_polygon_list)


#gitterlistevalues = pd.DataFrame(shapely_polygon_list)
#print(gitterlistevalues)
#geoframe = gpd.GeoDataFrame(gitterlistevalues, crs='EPSG:3035',geometry=gitterlistevalues)
#print(geoframe)
#geodata['Polygons'] = gitterlistevalues.values
#geoframe.plot('geometry')
#x,y = t.exterior.xy
#plt.plot(x,y)


#for row in coordinate_dataframe.iterrows():
#    plot_zellen(row.POLYGON)

#print(geodata)

#points = coord_to_polygon.create_points_from_tuple_dataframe(coordinate_tuple)
#xs = [point.x for point in points]
#ys = [point.y for point in points]
#plt.scatter(xs, ys)
#plt.show()















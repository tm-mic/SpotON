# Responsible for Module: Max W.; written with support by Christian, Thomas
# Coord_to_polygon is used for the creation of polygons from the coordinates given
# in the csv's imported by the I/O module.


import pandas as pd
import geopandas as gpd
from shapely.geometry import Polygon
import matplotlib.pyplot as plt
from shapely.geometry import Point

keys = ["Haushalte", "Bevoelkerung"]


def get_unique_crs(crs_dict: dict, keys: list):
    """
    Takes one csv of dict by key. Transfers it to numpy.ndarray
    deletes rows until every row unique.

    :param crs_dict: csv from dict
    :param keys: takes key of a list for the csv dict
    :return: np.ndarray with unique crs
    """
    crs_dict = crs_dict.get(keys[0])
    crs_series = crs_dict.squeeze()
    crs_unique = crs_series.unique()
    return crs_unique


def create_coordinate_tuple_dataframe(crs_unique):
    """
    Creates DataFrame with 3 columns. Slices Key in two parts,
    one longitude and one latitude. Changes DataType of these columns to int64.

    :param crs_unique: np.ndarray with unique crs
    :return: dataframe with longitude and latitude column, from slicing csv
    """
    original_coordinate_tuple = pd.DataFrame(crs_unique)
    original_coordinate_tuple.columns = ['Key']
    original_coordinate_tuple['longitude'] = original_coordinate_tuple['Key'].str.slice(23, 30)
    original_coordinate_tuple['latitude'] = original_coordinate_tuple['Key'].str.slice(15, 22)
    original_coordinate_tuple.longitude = \
        original_coordinate_tuple.longitude.astype('int64')
    original_coordinate_tuple.latitude = \
        original_coordinate_tuple.latitude.astype('int64')
    return original_coordinate_tuple


def add_coordinate_tuple_plus100_to_dataframe(original_coordinate_tuple):
    """
    Ads 2 columns to the original_coordinate_tuple Dataframe, modified by 100
    meters. Important for the creation of the polygons later on.

    :param original_coordinate_tuple: dataframe with longitude and latitude column
    :return: dataframe with two extra columns, one more longitude and latitude column, values higher by 100
    """
    longitudelist = original_coordinate_tuple.iloc[:, 1].tolist()
    longitudelistvalues = pd.Series(longitudelist)
    longitudelistvalues = longitudelistvalues.add(100)
    original_coordinate_tuple['longitude100'] = longitudelistvalues.values
    latitudelist = original_coordinate_tuple.iloc[:, 2].tolist()
    latitudelistvalues = pd.Series(latitudelist)
    latitudelistvalues = latitudelistvalues.add(100)
    original_coordinate_tuple['latitude100'] = latitudelistvalues.values
    original_coordinate_tuple_plus_100 = original_coordinate_tuple
    return original_coordinate_tuple_plus_100


def create_polygon_tuples_from_dataframe(original_coordinate_tuple_plus_100):
    """
    Combines the 2 longitude and 2 latitude values to tuples. Saves them in new
    POLYGON column. Results are tuples of tuples with 4 tuples which match the
    coordinates of the corners of the polygons.

    :param original_coordinate_tuple_plus_100: dataframe with two longitude and latitude columns
    :return: dataframe with pointtuples for corners of the polygon, saves them in one column
    """
    tuple0 = list(zip(original_coordinate_tuple_plus_100.longitude,
                      original_coordinate_tuple_plus_100.latitude))
    tuple1 = list(zip(original_coordinate_tuple_plus_100.longitude100,
                      original_coordinate_tuple_plus_100.latitude))
    tuple2 = list(zip(original_coordinate_tuple_plus_100.longitude100,
                      original_coordinate_tuple_plus_100.latitude100))
    tuple3 = list(zip(original_coordinate_tuple_plus_100.longitude,
                      original_coordinate_tuple_plus_100.latitude100))
    point_list = list(zip(tuple0, tuple1, tuple2, tuple3))
    point_list_polygon = pd.Series(point_list)
    original_coordinate_tuple_plus_100['POLYGON'] = point_list_polygon.values
    coordinate_dataframe_with_polygon_tuples = original_coordinate_tuple_plus_100
    return coordinate_dataframe_with_polygon_tuples


def create_polygon_point_list(coordinate_dataframe_with_polygon_tuples):
    """
    Transforms the dataframe column POLYGON in a list of tuples for further tasks.

    :param coordinate_dataframe_with_polygon_tuples: dataframe with polygon column with 4 pointtuples
    :return: dataframe with list of tuples in polygon column
    """
    for index, row in coordinate_dataframe_with_polygon_tuples.iterrows():
        polygon = row.POLYGON
        polygon_point_list = []
        for x in polygon:
            polygon_point_list.append(x)
        coordinate_dataframe_with_polygon_tuples.at[index, 'POLYGON'] = polygon_point_list
    return coordinate_dataframe_with_polygon_tuples


def create_shapely_polygons(coordinate_dataframe_with_polygon_tuples):
    """
    Transforms column POLYGON in a list of tuples. for-loop creates shapely-polygons
    of every element in the list.

    :param coordinate_dataframe_with_polygon_tuples: dataframe with list of tuples in polygon column
    :return: dataframe with shapely polygons in a list
    """
    polygon_column_list = coordinate_dataframe_with_polygon_tuples.iloc[:, 5].tolist()
    shapely_polygon_list = []
    for x in polygon_column_list:
        x = Polygon(x)
        shapely_polygon_list.append(x)
    return shapely_polygon_list


def create_geodataframe(original_coordinate_tuple, shapely_polygon_list):
    """
    Creates gdf with the first column as key and second column as geometry
    filled with polygons. Also transfers the geometry in EPSG:3035.

    :param original_coordinate_tuple: dataframe with longitude and latitude column
    :param shapely_polygon_list: dataframe with shapely polygons in a list
    :return: geodataframe with key from the polygon grid and geometry column filled with
            the 100x100m squares
    """
    polygon_grid_gdf = gpd.GeoDataFrame()
    keylist = original_coordinate_tuple.iloc[:, 0].tolist()
    keylistvalues = pd.Series(keylist)
    polygon_grid_gdf['Key'] = keylistvalues.values
    shapely_polygon_list_values = pd.Series(shapely_polygon_list)
    polygon_grid_gdf['geometry'] = shapely_polygon_list_values.values
    polygon_grid_gdf = gpd.GeoDataFrame(data=keylistvalues, crs='EPSG:3035',
                                        geometry=shapely_polygon_list_values)
    return polygon_grid_gdf


def centroids_of_polygon_grid(polygon_grid_gdf):
    """
    Creates the centroid of polygons in grid gdf.

    :param polygon_grid_gdf: geodataframe with the grid squares
    :return: geodataframe with the centroids of the squares
    """
    centroid_points_gdf = polygon_grid_gdf.copy()
    centroid_points_gdf['geometry'] = centroid_points_gdf['geometry'].centroid
    return centroid_points_gdf


def buffer_centroid_points(centroid_points_gdf, buffer=1000):
    """
    Buffers the centroids. Creates circle with 1000meters radius.

    :param centroid_points_gdf: geodataframe with centroids of the grid squares
    :param buffer: variable for buffering the centroid
    :return: geodataframe with circles, radius=buffer
    """
    buffer_centroid_points_gdf = centroid_points_gdf.copy()
    buffer_centroid_points_gdf['geometry'] = buffer_centroid_points_gdf['geometry'].buffer(buffer)
    return buffer_centroid_points_gdf


def load_base_polygon_to_gdf(basepolygon_path: str):
    """
    Loads the shapefile of all Zulassungskreise and transforms the
    EPSG and writes it into a gdf with a Name, ARS and geometry column.
    Slices ARS. Only first Numbers are needed for the Bundesland.

    :param basepolygon_path: path string of a shp of germanys Zulassungskreise
    :return: geodataframe with all Zulassungskreise in Germany (EPSG:3035)
    """
    base_polygon_gdf = gpd.read_file(basepolygon_path, encoding='utf-8')
    base_polygon_gdf = base_polygon_gdf[["NAME", "ARS", "geometry"]]
    base_polygon_gdf['ARS'] = base_polygon_gdf['ARS'].str.slice(0, 2)
    base_polygon_gdf = base_polygon_gdf.to_crs(crs='EPSG:3035')
    return base_polygon_gdf


def load_gemeinde_polygon_to_gdf(gemeindeshp: str):
    """
    Loads the shapefile of all Gemeinden and transforms the
    EPSG and writes it into a gdf with a Name, ARS, BEZ and geometry column.
    Slices ARS. Only first Numbers are needed for the Bundesland.

    :param gemeindeshp: path string of a shp of germanys gemeinden
    :return: geodataframe with all Gemeinden in germany (EPSG:3035)
    """
    gemeinde_polygon_gdf = gpd.read_file(gemeindeshp, encoding='utf-8')
    gemeinde_polygon_gdf = gemeinde_polygon_gdf[["GEN", "ARS", "BEZ", "geometry"]]
    gemeinde_polygon_gdf['ARS'] = gemeinde_polygon_gdf['ARS'].str.slice(0, 2)
    gemeinde_polygon_gdf.rename(columns={'GEN': 'NAME'}, inplace=True)
    gemeinde_polygon_gdf = gemeinde_polygon_gdf.to_crs(crs='EPSG:3035')
    return gemeinde_polygon_gdf


def choose_zulassungskreis_from_gdf_by_name(base_polygon_gdf, Name: str):
    """
    Chooses a single Zulassungskreis by Name from the base_polygon_gdf.

    :param base_polygon_gdf: geodataframe with all Zulassungskreise in germany
    :return: geodataframe with single Zulasungskreis choosen by name
    """
    zulassungskreis = base_polygon_gdf.loc[base_polygon_gdf["NAME"] == Name]
    return zulassungskreis


def choose_all_zulassungskreise_by_bundesland(base_polygon_gdf, ARS: str):
    """
    Chooses all Zulassungskreise from a Bundesland by ARS.

    :param base_polygon_gdf: geodataframe with all Zulassungskreise in germany
    :return: geodataframe with all Zulassungskreise of a Bundesland
    choosen by ARS (Regionalschlüssel)
    """
    bundesland = base_polygon_gdf.loc[base_polygon_gdf["ARS"] == ARS]
    return bundesland


def intersection_of_base_polygon_and_grid(GeoDataFrame, polygon_grid_gdf):
    """
    Intersects a GeoDataFrame with the grid_gdf.

    :param GeoDataFrame: geodataframe with borderpolygons of Gemeinde or Zulassungskreis
    :param polygon_grid_gdf: geodataframe with 100x100 squares
    :return: geodataframe with all squares in the GeoDataFrame polygon
    """
    insec_base_grid_gdf = polygon_grid_gdf.sjoin(GeoDataFrame)
    return insec_base_grid_gdf


def ladestationen_to_gdf(path: str):
    """
    Creates gdf with all Ladestationen in Germany from csv.
    Geometries in column are points. Transfers it in EPSG:3035.

    :param path: path string for a csv with all the Ladestationen in germany
    :return: geodataframe with all Ladestationen in germany (EPSG:3035)
    """
    ladestationen_gdf = gpd.GeoDataFrame.from_file(path)
    ladestationen_gdf = ladestationen_gdf.rename(columns=ladestationen_gdf.iloc[9]).loc[10:]
    ladestationen_gdf['Breitengrad'] = ladestationen_gdf['Breitengrad'].str.replace(',', '.').astype(float)
    ladestationen_gdf['Laengengrad'] = ladestationen_gdf['Längengrad'].str.replace(',', '.').astype(float)
    ladestationen_gdf['geometry'] = ladestationen_gdf.apply(
        lambda x: Point((float(x.Laengengrad), float(x.Breitengrad))),
        axis=1)
    del ladestationen_gdf['Breitengrad']
    del ladestationen_gdf['Laengengrad']
    ladestationen_gdf['Anzahl Ladepunkte'] = ladestationen_gdf['Anzahl Ladepunkte'].astype('int64')
    ladestationen_gdf = ladestationen_gdf.set_crs(crs='WGS 84', epsg='EPSG:3857')
    ladestationen_gdf = ladestationen_gdf.to_crs(crs='EPSG:3035')
    return ladestationen_gdf


def insec_ladestationen_gdf_with_zu_or_bu(GeoDataFrame, ladestationen_gdf):
    """
    Intersects Ladestation with a Zulassungskreis or a Bundesland.

    :param GeoDataFrame: geodataframe with the polygon of a zulassungsbezirk or bundesland
    :param ladestationen_gdf: geodataframe with all ladesaeulen in germany
    :return: geodataframe with ladesauelen in area of interest
    """
    zubu_ladestationen = ladestationen_gdf.sjoin(GeoDataFrame)
    del zubu_ladestationen['index_right']
    zubu_ladestationen = zubu_ladestationen.iloc[:, [3, 4, 0, 1, 2]]
    return zubu_ladestationen


def insec_polygon_grid_with_ladestationen(GeoDataFrame, polygon_grid_gdf):
    """
    Intersection of polygon grid with ladestationen of one Zulassungskreis or Bundesland.

    :param GeoDataFrame: geodataframe with ladesauelen in area of interest
    :param polygon_grid_gdf: grid of the zensus data
    :return: geodataframe
    """
    polygon_grid_ladestationen = GeoDataFrame.sjoin(polygon_grid_gdf)
    return polygon_grid_ladestationen


def ladestationen_in_range_of_centroid(zubu_ladestationen, buffer_centroid_points_gdf):
    """
    Ladestation in range of buffered centroid.

    :param zubu_ladestationen: geodataframe with ladesauelen in area of interest
    :param buffer_centroid_points_gdf: geodataframe with centroids of zensus grid buffered
    :return: geodataframe
    """
    ladestationen_in_range = zubu_ladestationen.sjoin(buffer_centroid_points_gdf)
    return ladestationen_in_range


def neighbors_of_grid_polygon(centroid_points_gdf, buffer_centroid_points_gdf):
    """
    Centroids in range of buffered centroid.

    :param centroid_points_gdf: centroids of zensus grid
    :param buffer_centroid_points_gdf:  geodataframe with centroids of zensus grid buffered
    :return: geodataframe
    """
    neighbors = centroid_points_gdf.sjoin(buffer_centroid_points_gdf)
    return neighbors


def ladestationen_in_gemeinde_with_grid_centroid(Name: str, gemeinde_polygon_gdf, ladestationen_gdf,
                                                 centroid_points_gdf, buffer=500):
    """
    Takes all Ladestationen in one Gemeinde (which can specified by name), buffers them and
    intersects them with the centroid of the grid polygons in range of the buffer. The result is
    a gdf in which the information is given, how many ladestationen are in reach of one grid.
    Column names are: KEY, geometry, ARS, NAME, BEZ, Normalladeeinrichtung, Anzahl Ladepunkte.

    :param Name: gemeinde name in gemeinde_polygon_gdf
    :param gemeinde_polygon_gdf: gdf with the polygons of all gemeinden
    :param ladestationen_gdf: gdf with all ladestationen in germany
    :param centroid_points_gdf: gdf with point geometries (center of grid_polygons)
    :param buffer: buffering the centroids by amount x
    :return: ladestationen in reach grid centroids
    """
    gemeinde = choose_zulassungskreis_from_gdf_by_name(gemeinde_polygon_gdf, Name)
    gemeinde_ladestationen = ladestationen_gdf.sjoin(gemeinde)
    gemeinde_ladestationen = buffer_centroid_points(gemeinde_ladestationen, buffer)
    del gemeinde_ladestationen['index_right']
    gemeinde_ladestationen = gemeinde_ladestationen.iloc[:, [4, 3, 5, 0, 1, 2]]
    gemeinde_ladestationen = centroid_points_gdf.sjoin(gemeinde_ladestationen)
    del gemeinde_ladestationen['index_right']
    gemeinde_ladestationen.rename(columns={0: 'KEY'}, inplace=True)
    return gemeinde_ladestationen


def plot_geodataframe(GeoDataFrame):
    """
    Plots a gdf and shows it.

    :param GeoDataFrame: any GeoDataFrame
    :return: Plot of the gdf
    """
    GeoDataFrame.plot()
    plt.show()
    return GeoDataFrame

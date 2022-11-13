# Responsible for Module: Max W.; written with support by Christian, Thomas
# Coord_to_polygon is used for the creation of polygons from the coordinates given
# in the csv's imported by the I/O module.


import pandas as pd
from shapely.geometry import Polygon


keys = ["Haushalte", "Bevoelkerung"]

def get_unique_crs(crs_dict: dict, keys: list):
    """Takes one csv of dict by key. Transfers it to numpy.ndarray
    deletes rows until every row unique."""
    crs_dict = crs_dict.get(keys[0])
    crs_series = crs_dict.squeeze()
    crs_unique = crs_series.unique()
    return crs_unique

def create_coordinate_tuple_dataframe(crs_unique):
    """Creates DataFrame with 3 columns. Slices Key in two parts,
     one longitude and one latitude. Changes DataType of these columns to int64."""
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
    """Ads 2 columns to the original_coordinate_tuple Dataframe, modified by 100
    meters. Important for the creation of the polygons later on."""
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
    """Combines the 2 longitude and 2 latitude values to tuples. Saves them in new
    POLYGON column. Results are tuples of tuples with 4 tuples which match the
    coordinates of the corners of the polygons."""
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
    """Transforms the dataframe column POLYGON in a list of tuples for further tasks."""
    for index, row in coordinate_dataframe_with_polygon_tuples.iterrows():
        polygon = row.POLYGON
        polygon_point_list = []
        for x in polygon:
            polygon_point_list.append(x)
        coordinate_dataframe_with_polygon_tuples.at[index, 'POLYGON'] = polygon_point_list
    return coordinate_dataframe_with_polygon_tuples

def create_shapely_polygons(coordinate_dataframe_with_polygon_tuples):
    """Transforms column POLYGON in a list of tuples. for-loop creates shapely-polygons
    of every element in the list."""
    polygon_column_list = coordinate_dataframe_with_polygon_tuples.iloc[:, 5].tolist()
    shapely_polygon_list = []
    for x in polygon_column_list:
        x = Polygon(x)
        shapely_polygon_list.append(x)
    return shapely_polygon_list

## TODO: Create GeoDataFrame with polygons in EPSG:3035.

## TODO: Plot the GeoDataFrame.

## TODO: Intersect GeoDataFrame with polygon of a Landkreis based
## TODO: on the centroid of the polygons.
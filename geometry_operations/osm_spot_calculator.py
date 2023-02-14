# Responsible for Module: Max W.; written with support by Christian, Thomas
# Joins the different layers of interest with the base layer, which are the
# parking spots.

import pandas as pd
import geopandas as gpd
import osm_calc_parameters
import json
import numpy as np
import shapely

def read_slim_parking_layer(Path:str):
    """
    Reads slim_parking_layer.json into GeoDataFrame.

    :param Path: all OSM parking data with unimportend colums dropped (fee: no and access: yes or customer)
    :return: GeoDataFrame
    """
    parking_layer = gpd.read_file(Path, encoding='utf-8')
    return parking_layer

def delete_parking_areas_with_oels(parking_layer, ladestationen_gdf):
    """
    Intersects parking areas with ladestationen_gdf. Purpose is the deletion of parking areas
    which already have ladestationen.

    :param parking_layer: GeoDataFrame with all parking areas in germany
    :param ladestationen_gdf: GeoDataFrame with all ladestationen in germany, which are openly accessible
    :return: GeoDataFrame of parking areas without ladestationen
    """
    ladestationen_gdf['geometry'] = ladestationen_gdf['geometry'].buffer(5)
    lost_areas = parking_layer.sjoin(ladestationen_gdf)
    clean_parking_layer = parking_layer.merge(lost_areas, how='left', left_on='id', right_on='id')
    clean_parking_layer = clean_parking_layer[clean_parking_layer.isna().any(axis=1)]
    clean_parking_layer = clean_parking_layer[['id','value_x','geometry_x']]
    clean_parking_layer.rename(columns={'value_x':'value','geometry_x':'geometry'},inplace=True)
    clean_parking_layer = gpd.GeoDataFrame(clean_parking_layer, geometry='geometry')
    clean_parking_layer = clean_parking_layer.reset_index(drop=True)
    return clean_parking_layer

def calculate_parking_area(GeoDataFrame):
    """
    Calculates the area of the parking spots.

    :param GeoDataFrame: GeoDataFrame of parking areas without ladestationen
    :return: GeoDataFrame of parking areas with their area
    """
    parking_area = GeoDataFrame
    parking_area.insert(2, "area", parking_area['geometry'].area, True)
    return parking_area

def parking_area_mask(GeoDataFrame):
    """
    Downsizes the parking_area area because driveways are included in the geometries.
    Threshold is 1000sqm. There getting downsized by the factor set in the config_mce_weights_and_buffer.json.

    :param GeoDataFrame: GeoDataFrame of parking areas with their area
    :return: GeoDataFrame of parking areas with their potentially downsized area
    """
    file = open('config_mce_weights_and_buffer.json')
    factor = json.loads(file.read())
    GeoDataFrame['area'] = np.where(GeoDataFrame['area'] > 1000, GeoDataFrame['area'] * factor['downsize_factor'],
                                    GeoDataFrame['area'])
    return GeoDataFrame

def parking_spots_per_parking_area(GeoDataFrame):
    """
    Calculates parking spots per area by width and length set in the
    config_mce_weights_and_buffer.json.

    :param GeoDataFrame: GeoDataFrame of parking areas with their area
    :return: GeoDataFrame of parking areas with their area and parking spots per area
    """
    file = open('config_mce_weights_and_buffer.json')
    metrics = json.loads(file.read())
    GeoDataFrame.insert(3, "parking_spots", GeoDataFrame['area']/
                        (metrics['parking_spot_width']*metrics['parking_spot_length']), True)
    return GeoDataFrame

def ladesaulen_per_parking_area(GeoDataFrame):
    """
    Maps potential ladesaeulen to the parking areas by the amount of parking spots.

    :param GeoDataFrame: GeoDataFrame of parking areas with their area and parking spots per area
    :return: GeoDataFrame of parking areas with their potential
    """
    GeoDataFrame.insert(4, 'ladesaeulen', np.nan, True)
    spots_list = GeoDataFrame['parking_spots'].tolist()
    ladesaeulen_amount = []
    for i in spots_list:
        if i < 10:
            x = 0
        elif i < 100:
            x = 1
        elif i < 200:
            x = 2
        elif i < 300:
            x = 3
        elif i < 400:
            x = 4
        elif i < 500:
            x = 5
        elif i < 600:
            x = 6
        elif i < 700:
            x = 7
        elif i < 800:
            x = 8
        elif i < 900:
            x = 9
        elif i < 1000:
            x = 10
        else:
            x = 15
        ladesaeulen_amount.append(x)
    ladesaeulen_amount_values = pd.Series(ladesaeulen_amount)
    GeoDataFrame['ladesaeulen'] = ladesaeulen_amount_values.values
    return GeoDataFrame

def buffer_parking_layer(GeoDataFrame):
    """
    Buffers the parking layer by the amount set in the config_mce_weights_and_buffer.json.

    :param GeoDataFrame: GeoDataFrame of parking areas with their potential
    :return: GeoDataFrame buffered by amount x
    """
    file = open('config_mce_weights_and_buffer.json')
    buffer = json.loads(file.read())
    buffer = buffer['buffer']
    GeoDataFrame['geometry'] = GeoDataFrame['geometry'].buffer(buffer)
    return GeoDataFrame

def calculate_parking_spot_weight_by_poi(GeoDataFrame):
    """
    Calculates the weight of all parking spots. The function reads a dict with the paths of the
    point of interest layers and their values determined by MCE.

    :param GeoDataFrame: GeoDataFrame buffered by amount x
    :return: GeoDataFrame of parking areas with their values calculated by spatial joins with the poi layers
    """
    for path, value in osm_calc_parameters.poi.items():
        path = gpd.read_file(path, encoding='utf-8')
        path['value'] = value
        GeoDataFrame = GeoDataFrame.sjoin(path, how='left')
        value_sum = GeoDataFrame.groupby('id_left')['value_right'].sum().reset_index()
        GeoDataFrame = pd.merge(GeoDataFrame, value_sum, on='id_left')
        GeoDataFrame['value_sum'] = GeoDataFrame['value_left'] + GeoDataFrame['value_right_y']
        GeoDataFrame = GeoDataFrame[['id_left', 'value_sum', 'area', 'parking_spots', 'ladesaeulen', 'geometry']]
        GeoDataFrame = GeoDataFrame.drop_duplicates().reset_index(drop=True)
        GeoDataFrame.rename(columns={'id_left': 'id', 'value_sum': 'value'}, inplace=True)
    return GeoDataFrame

def calculate_parking_spot_weight_by_streets1(GeoDataFrame):
    """
    Calculates the weight of all parking spots. The function reads a dict with the paths of the
    street layers and their values determined by MCE. In this case the first set of values(street1).
    Large streets get higher values.

    :param GeoDataFrame: GeoDataFrame of parking areas with their values calculated by spatial joins with the poi layers
    :return: GeoDataFrame of parking areas with their values calculated by spatial joins with the poi layers with
                added values for the streets
    """
    for path, value in osm_calc_parameters.streets1.items():
        path = gpd.read_file(path, encoding='utf-8')
        path['value'] = value
        GeoDataFrame = GeoDataFrame.sjoin(path, how='left')
        value_sum = GeoDataFrame.groupby('id_left')['value_right'].sum().reset_index()
        GeoDataFrame = pd.merge(GeoDataFrame, value_sum, on='id_left')
        GeoDataFrame['value_sum'] = GeoDataFrame['value_left'] + GeoDataFrame['value_right_y']
        GeoDataFrame = GeoDataFrame[['id_left', 'value_sum', 'area', 'parking_spots', 'ladesaeulen', 'geometry']]
        GeoDataFrame = GeoDataFrame.drop_duplicates().reset_index(drop=True)
        GeoDataFrame.rename(columns={'id_left': 'id', 'value_sum': 'value'}, inplace=True)
    return GeoDataFrame

def calculate_parking_spot_weight_by_streets2(GeoDataFrame):
    """
    Calculates the weight of all parking spots. The function reads a dict with the paths of the
    street layers and their values determined by MCE. In this case the first set of values(street2).
    Streets in more inhabited areas get higher values.

    :param GeoDataFrame: GeoDataFrame of parking areas with their values calculated by spatial joins with the poi layers
    :return: GeoDataFrame of parking areas with their values calculated by spatial joins with the poi layers with
                added values for the streets
    """
    for path, value in osm_calc_parameters.streets2.items():
        path = gpd.read_file(path, encoding='utf-8')
        path['value'] = value
        GeoDataFrame = GeoDataFrame.sjoin(path, how='left')
        value_sum = GeoDataFrame.groupby('id_left')['value_right'].sum().reset_index()
        GeoDataFrame = pd.merge(GeoDataFrame, value_sum, on='id_left')
        GeoDataFrame['value_sum'] = GeoDataFrame['value_left'] + GeoDataFrame['value_right_y']
        GeoDataFrame = GeoDataFrame[['id_left', 'value_sum', 'area', 'parking_spots', 'ladesaeulen', 'geometry']]
        GeoDataFrame = GeoDataFrame.drop_duplicates().reset_index(drop=True)
        GeoDataFrame.rename(columns={'id_left': 'id', 'value_sum': 'value'}, inplace=True)
    return GeoDataFrame

def reset_geometry_to_original(GeoDataFrame, clean_parking_layer):
    """
    Because the parking areas were buffered, they need to be reset to their original geometries

    :param GeoDataFrame: GeoDataFrame with the values for each parking spot
    :param clean_parking_layer: GeoDataFrame with the original parking area geometries
    :return: GeoDataFrame with the values for each parking spot and the original geometry
    """
    GeoDataFrame['geometry'] = clean_parking_layer['geometry']
    return GeoDataFrame

def delete_point_linestring_geometries(GeoDataFrame):
    """
    Parking areas where the geometry are points or linestrings are deleted

    :param GeoDataFrame: GeoDataFrame with the values for each parking spot and the original geometry
    :return: GeoDataFrame with polygon and multipolygon geometries
    """
    GeoDataFrame = GeoDataFrame[GeoDataFrame.geom_type != 'Point']
    GeoDataFrame = GeoDataFrame[GeoDataFrame.geom_type != 'LineString']
    return GeoDataFrame

def delete_doubled_geometries(GeoDataFrame):
    """
    Deletes doubled geometries in the GeoDataFrame by comparing their wkb format.

    :param GeoDataFrame: GeoDataFrame with parking areas and their values
    :return: GeoDataFrame without doubled geometries
    """
    GeoDataFrame['geometry'] = GeoDataFrame['geometry'].apply(lambda geom: geom.wkb)
    GeoDataFrame = GeoDataFrame.drop_duplicates(['geometry'])
    GeoDataFrame['geometry'] = GeoDataFrame['geometry'].apply(lambda geom: shapely.wkb.loads(geom))
    GeoDataFrame = gpd.GeoDataFrame(GeoDataFrame, geometry='geometry', crs='EPSG:3035')
    GeoDataFrame = GeoDataFrame.reset_index(drop=True)
    return GeoDataFrame
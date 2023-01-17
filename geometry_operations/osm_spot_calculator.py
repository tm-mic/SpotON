# Responsible for Module: Max W.; written with support by Christian, Thomas
# Joins the different layers of interest with the base layer, which are the
# parking spots.

import pandas as pd
import geopandas as gpd
import osm_calc_parameters
import json


def buffer_parking_layer(Path: str):
    """Buffers the parking layer by the amount set in the config_mce_weights_and_buffer.json."""
    file = open('config_mce_weights_and_buffer.json')
    buffer = json.loads(file.read())
    buffer = buffer['buffer']
    parking_layer = gpd.read_file(Path, encoding='utf-8')
    parking_layer['geometry'] = parking_layer['geometry'].buffer(buffer)
    return parking_layer

def calculate_parking_spot_weight_by_poi(GeoDataFrame):
    """Calculates the weight of all parking spots. The function reads a dict with the paths of the
    point of interest layers and their values determined by MCE. """
    for path, value in osm_calc_parameters.poi.items():
        path = gpd.read_file(path, encoding='utf-8')
        path['value'] = value
        GeoDataFrame = GeoDataFrame.sjoin(path, how='left')
        value_sum = GeoDataFrame.groupby('id_left')['value_right'].sum().reset_index()
        GeoDataFrame = pd.merge(GeoDataFrame, value_sum, on='id_left')
        GeoDataFrame['value_sum'] = GeoDataFrame['value_left'] + GeoDataFrame['value_right_y']
        GeoDataFrame = GeoDataFrame[['id_left', 'value_sum', 'geometry']]
        GeoDataFrame = GeoDataFrame.drop_duplicates().reset_index(drop=True)
        GeoDataFrame.rename(columns={'id_left': 'id', 'value_sum': 'value'}, inplace=True)
    return GeoDataFrame

def calculate_parking_spot_weight_by_streets1(GeoDataFrame):
    """Calculates the weight of all parking spots. The function reads a dict with the paths of the
    street layers and their values determined by MCE. In this case the first set of values(street1)."""
    for path, value in osm_calc_parameters.streets1.items():
        path = gpd.read_file(path, encoding='utf-8')
        path['value'] = value
        GeoDataFrame = GeoDataFrame.sjoin(path, how='left')
        value_sum = GeoDataFrame.groupby('id_left')['value_right'].sum().reset_index()
        GeoDataFrame = pd.merge(GeoDataFrame, value_sum, on='id_left')
        GeoDataFrame['value_sum'] = GeoDataFrame['value_left'] + GeoDataFrame['value_right_y']
        GeoDataFrame = GeoDataFrame[['id_left', 'value_sum', 'geometry']]
        GeoDataFrame = GeoDataFrame.drop_duplicates().reset_index(drop=True)
        GeoDataFrame.rename(columns={'id_left': 'id', 'value_sum': 'value'}, inplace=True)
    return GeoDataFrame

def calculate_parking_spot_weight_by_streets2(GeoDataFrame):
    """Calculates the weight of all parking spots. The function reads a dict with the paths of the
    street layers and their values determined by MCE. In this case the first set of values(street2)."""
    for path, value in osm_calc_parameters.streets2.items():
        path = gpd.read_file(path, encoding='utf-8')
        path['value'] = value
        GeoDataFrame = GeoDataFrame.sjoin(path, how='left')
        value_sum = GeoDataFrame.groupby('id_left')['value_right'].sum().reset_index()
        GeoDataFrame = pd.merge(GeoDataFrame, value_sum, on='id_left')
        GeoDataFrame['value_sum'] = GeoDataFrame['value_left'] + GeoDataFrame['value_right_y']
        GeoDataFrame = GeoDataFrame[['id_left', 'value_sum', 'geometry']]
        GeoDataFrame = GeoDataFrame.drop_duplicates().reset_index(drop=True)
        GeoDataFrame.rename(columns={'id_left': 'id', 'value_sum': 'value'}, inplace=True)
    return GeoDataFrame
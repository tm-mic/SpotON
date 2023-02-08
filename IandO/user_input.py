from import_funcs import obtain_aoi_polygon as op
from import_funcs import read_shp
from IandO import json_utility as ju
import geopandas as gpd


# TODO: add redo possibility through listener

def choose_aoi_shp(aoi_path_dict: dict):
    """
    Requests user selection from dict of shapefile paths.

    :param aoi_path_dict: Dictionary of file paths.
    :return: Dict key entry representing a file path.
    """

    print(f"please choose one of the following aoi: {aoi_path_dict.keys()}")
    while True:
        try:
            # TODO: Extract user input to interest_area_ladestationen_poly
            global user_input_aoi_type
            user_input_aoi_type = str(input(f"Which aoi is interesting for you?"))
            shp_selec = aoi_path_dict.get(user_input_aoi_type)
            if shp_selec is None:
                print("The selection you have made is not valid.")
                continue
            else:
                return shp_selec
        except:
            print(
                "Please enter a String representing a valid Area of Interest (AOI). The valid area of interests are depicted in the list above.")
            continue


def select_aoi(aoi_shp_path):
    """
    From shapefile select an entry from the "GEN" column.
    # TODO: Implement handling of "NAME" instead of "GEN" column in KFZ250.shp

    :param aoi_shp_path: Shapefile path.
    :return: Name of selected aoi. Geometry of selected aoi.
    """
    aoi_df = gpd.read_file(aoi_shp_path, encoding='utf-8')
    if "GEN" in aoi_df:
        aoi_list = aoi_df["GEN"].unique()
    elif "NAME" in aoi_df:
        aoi_list = aoi_df["NAME"].unique()
    print(aoi_list)
    while True:
        try:
            user_input = str(
                input("Please enter a valid Area Of Interest (AOI). You can chose AOI as shown to you by this list."))
            user_aoi = op(aoi_shp_path, user_input)
            if user_aoi.empty:
                print("The selection you have made is not valid. Please chose another valid Area of Interest.")
                continue
            else:
                aoi_poly = user_aoi.dissolve().explode(index_parts=False).iloc[0]['geometry']
                return user_input, aoi_poly, user_input_aoi_type
        except ValueError:
            print(
                "Please enter a String representing a valid Area of Interest (AOI). The valid area of interests are depicted in the list.")
            continue


def ui_aoi(shp_dict):
    """
    Combines choose_aoi_shp and selct_aoi to form ui dialogue.

    :param shp_dict: Dictionary containing shapefiles as specified in config.json.
    :return: Name of selected aoi. Geometry of selected aoi.
    """

    user_aoi_path = choose_aoi_shp(shp_dict)
    user_selec = select_aoi(user_aoi_path)
    return user_selec[0], user_selec[1], user_selec[2]

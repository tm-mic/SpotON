from import_funcs import obtain_bl_polygon as op
from import_funcs import read_shp
from IandO import json_utility as ju


# TODO: add redo possibility through listener

def choose_aoi_shp(aoi_path_dict: dict):
    print(f"please choose one of the following aoi: {aoi_path_dict.keys()}")
    while True:
        try:
            user_input = str(input(f"Which aoi is interesting for you?"
                                   f"Please note: in this step it is most sensical to choose"
                                   f"a Bundesland as the point reference will be created"
                                   f"for the whole Bundesland and can be used for future runs"
                                   f"comparing different areas of interest in the same bundesland."))
            shp_selec = aoi_path_dict.get(user_input)
            if shp_selec is None:
                print("The selection you have made is not valid.")
                continue
            else:
                return shp_selec
        except:
            print(
                "Please enter a String representing a valid Area of Interest (AOI). The valid area of interests are depicted in the list.")
            continue


def select_aoi(aoi_shp_path):
    aoi_list = read_shp(aoi_shp_path, "GEN").unique()
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
                return user_input, aoi_poly
        except ValueError:
            print(
                "Please enter a String representing a valid Area of Interest (AOI). The valid area of interests are depicted in the list.")
            continue


def ui_aoi(shp_dict):
    user_aoi_path = choose_aoi_shp(shp_dict)
    user_selec = select_aoi(user_aoi_path)
    return user_selec[0], user_selec[1]

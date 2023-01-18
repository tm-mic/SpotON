# Responsible for Module: Christian L.; written with support by Max, Thomas
# Json Utility is the basic module to import csv files into a dataframe dictionary

import json
import csv

import pandas as pd


def read_json(file_path='config.json') -> object:
    """
    Casts json file from file to python object.
    Note: Object_hook is necessary due to json format. Casts digit str to int.
    :param: file_path: Path to json file.
    :return: Python object. Most likely a dict.
    """
    try:
        file = open(file_path)
        return json.load(file, object_hook=lambda d: {int(k)
                         if k.isdigit() else k: v for k, v in d.items()})
    except FileNotFoundError:
        print(f"The file {file_path} you are trying to read does not exists at the path given."
              f"Please provide a different path to an existing config file.")
        return 0


def write_dict_to_json(dictionary: dict, filepath: str, append=False):
    """Writes dict type to json file. If file does not exist new files is created."""
    if append == 'a':
        write = 'a'
    else:
        write = 'w'
    with open(filepath, write) as outfile:
        json.dump(dictionary, outfile)
    return None


def read_json_elements(json_obj, file_ident, attr=None):
    """
    Reads json elements. Types are returned like specified in desealization table. Ex. specified obj returns dict.
    :param json_obj: pass python native json object.
    :param file_ident: File identifier - First level element.
    :param attr: Attribute identifier - Second level element.
    :return: Specified Json Entry.
    """
    if attr is None:
        return json_obj[file_ident]
    else:
        return json_obj[file_ident][attr]


def write_df_to_csv(df: pd.DataFrame, folderpath: str, aoi, sep=','):
    df.to_csv(concat_filepath(aoi, folderpath), sep, index=False)
    return


def concat_filepath(aoi: str, folderpath: str, ending='.csv'):
    return folderpath+aoi+ending

# Responsible for Module: Christian L.; written with support by Max, Thomas
# Json Utility is the basic module to import csv files into a dataframe dictionary

import json
import pandas
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


def read_json_elements(json_obj, file_ident, attr):
    """
    Reads json elements. Types are returned like specified in desealization table. Ex. specified obj returns dict.
    :param json_obj: pass python native json object.
    :param file_ident: File identifier - First level element.
    :param attr: Attribute identifier - Second level element.
    :return: Specified Json Entry.
    """
    return json_obj[file_ident][attr]


def import_csv_to_df(filepath: str, header_list: object, nrows=1) -> pd.DataFrame:
    """
    Read csv to Dataframe from passed file.
    :param filepath: Path to csv file.
    :param header_list: List of headers to be read.
    :param nrows: Count of rows to import. Default "1" will import all rows of csv.
    :return: Dataframe with specified columns.
    """
    try:
        if nrows == 1:
            return pandas.read_csv(filepath, sep=None, header=0, usecols=header_list,
                                   engine='python', encoding_errors='replace', on_bad_lines="skip")
        else:
            return pandas.read_csv(filepath, sep=None, header=0, usecols=header_list,
                                   engine='python', encoding_errors='replace', on_bad_lines="skip", nrows=nrows)
    except ValueError:
        return pandas.read_csv(filepath, sep=None, header=0, engine='python', encoding_errors='replace',
                               on_bad_lines="skip", nrows=nrows)



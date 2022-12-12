# Responsible for Module: Christian L.; written by Christian L.
# I/O Metadata module handles import of validation data
import IandO
import pandas as pd

# TODO: Remove tramp assignments


def read_attribute_value_list_excel(path: str, headerrow=10, sheetname="Merkmale"):
    """Read attributes and values from file. File format needs to be standard to work.
    Headerrow is by default 10. Change depending on fileformating. Sheetname by default ""Merkmale."""
    try:
        merkmalsauspraegungen_codierung = pd.read_excel(path, sheet_name=sheetname, skiprows=headerrow, header=1)
        return merkmalsauspraegungen_codierung
    except FileNotFoundError:
        print(
            f"The file with the path {path} you are trying to read cannot be found. Please change path for the next run."
            f"The file will be skipped during this run.")
        pass


def get_attribute_list(path: str, attribute_column = 0):
    """Use to extract attributes as pandas series."""
    dataframe = read_attribute_value_list_excel(path)
    attributes = dataframe[dataframe.columns[attribute_column]]
    return attributes


def get_attribute_value_list(path: str, value_column = 1):
    """Use to extract attribute values as pandas series"""
    dataframe = read_attribute_value_list_excel(path)
    value_codes = dataframe[dataframe.columns[value_column]]
    return value_codes


def remove_nan_values(path: str):
    with_nan = get_attribute_list(path)
    without_nan = with_nan.dropna()
    return without_nan


# TODO: drop all rows with zeros in created dataframe
def create_single_attribute_value_dataframe(path: str, attribute: str) -> list:
    attribute_column = get_attribute_list(path)
    value_column = get_attribute_value_list(path).fillna(
        0)  # '0' fills are used to avoid attributes to take values of attributes laying above as excel is formatted to have an empty cell in first line of attribute definition.
    joined_dataframe = pd.concat([attribute_column, value_column], axis=1).fillna(method='ffill').dropna()
    extract_value_list = joined_dataframe[joined_dataframe['Merkmal'] == attribute]
    extract_value_list = extract_value_list.drop_duplicates(subset='Code')
    # TODO: remove 0 values from attr-val other than insgesamt
    return extract_value_list


def create_attribute_value_dict(path: str) -> dict :
    """Main function of this module. Used to create a dictionary of all the valid attribute values.
    Input: Valid absolute path to attribute description."""
    attr_value_dict = {}
    try:
        for attr in remove_nan_values(path):
            attr_val_df = create_single_attribute_value_dataframe(path, attr)
            val_list = attr_val_df['Code'].to_list()
            attr_value_dict.__setitem__(attr, val_list)
        return attr_value_dict
    except ValueError:
        print("The file or column you are looking for does not exist or is not listed in your path mapping or adapt helper functions to reflect your file structure.")
        pass

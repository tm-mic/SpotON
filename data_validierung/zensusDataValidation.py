# Responsible for Module: Christian L.; written by Christian L.
# Module handles data validation of data Merkmale and code values for Zensus data

from typing import Tuple
import pandas as pd


def validate_attr_val_combination(testframe: pd.DataFrame, attr_val_dict: dict) -> Tuple[list, float]:
    """Input dataframe to be tested and attr_val_dict to Bedarfe_Main.py against.
    [1] Dataframe can be created through *import_all_config_specified_csv* and config file.
    [2] attr_val_dict can be created through *create_attribute_value_dict*.
    [4] Function will return list of bad values and ratio of valid/raw entries."""
    bad_values = []
    correct_counter = 0
    total_counter = 0
    for index, row in testframe.iterrows():
        total_counter = total_counter + 1
        # TODO: add row.merkmal & row.Ausprägungs_code as optional variables in func signature
        raw_tuple = (row.Merkmal, row.Auspraegung_Code)
        test_attr = raw_tuple[0] in attr_val_dict
        if test_attr is False:
            bad_values.append(row)
            testframe.drop(index, inplace=True)
        else:
            if any(raw_tuple[1] in attr_val_dict.get(raw_tuple[0]) for item in attr_val_dict.get(raw_tuple[0])) is True:
                correct_counter = correct_counter + 1
                pass
            else:
                bad_values.append(row)
                testframe.drop(index, inplace=True)
    return bad_values, correct_counter/total_counter


# function written by Christian L.

import pandas as pd
from pandas import DataFrame
import random as rnd


def calc_distro_sum(val_sum: int, splitter: int) -> tuple:
    """
    Calcs two values with val1 + val2 = 1, if given splitter ranges from 0-1.

    :param val_sum: Original value to be splitted.
    :param splitter: Variable to split val. by.
    :return: Tuple of two floats, summing to approx 1.
    :exception Throws Attribute Error if splitter not between 0-1.
    """
    if splitter > 1 or splitter < 0:
        print("The splitter value you have passed in the age disaggregation is not valid.\n"
              "The value has to range between 0-1 to return sensical results for the index calc."
              f"Splitter value given {splitter}\n"
              f"Splitter value required float: 0-1")
        raise AttributeError

    group_one = val_sum * splitter
    group_two = val_sum - group_one
    return group_one, group_two


def value_distro(orig_group: int, distro_val: tuple, row: tuple, new_attr: str, group_mapping=None) -> list:
    """
    Distributes values given in pairs into one or two groups based on mapping provided.

    :param orig_group: Group to be split.
    :param distro_val: Original group values as distribution tuple to be split.
    :param row: Dataframe row passed containing original data.
    :param new_attr: Attribute Name to be inserted for calc values to be identified later.
    :return: List of max. two elements, with disaggregated dataframe row values
    """

    if group_mapping is None:
        group_mapping = {1: [1, 2], 2: [3, -1], 3: [4, 5], 4: [6, 7], 5: [8, 9]}

    keys = group_mapping.get(orig_group)
    res_list = []

    # deal with groups only mapping into one other group instead of two groups
    if len(keys) == 1:
        if distro_val[0] > 0:
            group = keys[0]
            res_list.append([row[1], new_attr, group, distro_val[0]+distro_val[1]])

    # main case mapping into two groups
    else:
        if distro_val[0] > 0:
            group = keys[0]
            res_list.append([row[1], new_attr, group, distro_val[0]])
        if distro_val[1] > 0:
            if keys[1] > 0:
                group = keys[1]
                res_list.append([row[1], new_attr, group, distro_val[1]])

    return res_list


def age_aggregation(
        df: pd.DataFrame,
        dis_uneven_low=0.45,
        dis_uneven_high=0.55,
        alter_kurz='ALTER_KURZ',
        attr_ident='ALTER_10JG',
        cols_to_keep= None
) -> DataFrame:
    """
    Runs through dataframe and disaggregates attr val data into provided groups by given splitter value.

    :param df: raw data to pass.
    :param dis_uneven_low: lower bound of rnd value
    :param dis_uneven_high: higher bound of rnd value
    :param alter_kurz: name of age attr to be disaggregated
    :param attr_ident: new attr identifier to be written back to dataframe
    :param cols_to_keep: columns to be returned with final dataframe
    :return: Dataframe containing only disaggregated age groups.
    """
    if cols_to_keep is None:
        cols_to_keep = ['NAME', 'Merkmal', 'Auspraegung_Code', 'Anzahl']

    val_sum = 1
    splitter = 1
    result_list = []
    for row in df.itertuples():
        if str(row.Merkmal).__contains__(alter_kurz):
            if row.Anzahl > 0:
                val_sum: float = row.Anzahl
                splitter = rnd.uniform(dis_uneven_low, dis_uneven_high)
            val_tuple = calc_distro_sum(val_sum, splitter)
            res_list = value_distro(row.Auspraegung_Code, val_tuple, row, attr_ident)
            result_list.append(res_list)
    result_list = [x for l in result_list for x in l]
    result_df = pd.DataFrame(result_list, columns=cols_to_keep)
    return result_df


def concat_unique(df_orig, df_appended, duplicate_list=None):
    """
    Concat two df only keeping the original values if duplicates are found.

    :param df_orig:
    :param df_appended:
    :param duplicate_list:
    :return: Concated df without duplicates from df_appended.
    """
    if duplicate_list is None:
        duplicate_list = ['Gitter_ID_100m_neu', 'Merkmal', 'Auspraegung_Code']

    drop_kurz = drop_df_values(df_orig)
    return pd.concat([df_appended, drop_kurz]).drop_duplicates(duplicate_list, keep='last').reset_index().sort_values(by=duplicate_list)


def drop_df_values(df, drop_val='ALTER_KURZ'):
    """
    Drop attr. values from dataframe.

    :param df: input
    :param drop_val: Attr to drop
    :return: Df without attr values matching drop_val
    """
    return df.drop(df[df.Merkmal == drop_val].index)


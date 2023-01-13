# function written by Christian L.

import pandas as pd
from pandas import DataFrame
import random as rnd


def calc_distro_sum(val_sum: int, splitter: float) -> tuple:
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

    group = keys[0]
    res_list.append([row[1], new_attr, group, distro_val[0], row[5], row[5], row[7]])
    group = keys[1]
    res_list.append([row[1], new_attr, group, distro_val[1], row[5], row[6], row[7]])

    return res_list


def disaggregate_age_attr(
        df: pd.DataFrame,
        dis_uneven_low=0.45,
        dis_uneven_high=0.55,
        alter_kurz='ALTER_KURZ',
        attr_ident='ALTER_10JG'
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
    alter_kurz = df.loc[df['Merkmal'] == alter_kurz]
    result_list = []
    cols = df.columns
    # TODO: drop itertuples for faster vector implementation
    for row in alter_kurz.itertuples():
        splitter = rnd.uniform(dis_uneven_low, dis_uneven_high)
        val_tuple = calc_distro_sum(row.Anzahl, splitter)
        res_list = value_distro(row.Auspraegung_Code, val_tuple, row, attr_ident)
        group_one = res_list[0]
        group_two = res_list[1]
        result_list.append(group_one)
        result_list.append(group_two)
    result_df = pd.DataFrame(result_list, columns=cols)
    return result_df.loc[result_df['Auspraegung_Code'] != -1]


def rem_by_mask(df: pd.DataFrame, mask: pd.Series, val=True):
    """
    Drops values provided in by boolean mask.

    :param df: Dataframe to check against.
    :param mask: boolean mask
    :param val: Drop True values is default. If False values are supposed to be dropped pass False.
    :return: Dataframe with dropped indizes as specified in boolean mask. No reindexing.
    """
    if val:
        return df.drop(df[mask].index)
    else:
        return df.drop(df[~mask].index)


def remap_groups(df, mapping):
    """
    Based on a dict passed the Auspraegung Code of an Attr is changed.
    #TODO: Add Error Handling - keyError might occur when key is requested that does not exist.
    :param df: Dataframe to regroup
    :param mapping: dictionary providing a mapping to regroup. All groups that exist in df
    should be specified here.
    :return: Dataframe with new Attribute Codes based on mapping provided in mapping dict.
    """
    mapping_keys = list(mapping.keys())
    df = df.loc[df['Merkmal'].isin(mapping_keys)]

    for key in mapping_keys:
        group_map = mapping.get(key)
        df['Auspraegung_Code'] = df.apply(lambda x: group_map[x['Auspraegung_Code']] if x['Merkmal'] == key else x['Auspraegung_Code'], axis=1)
    return df


def calc_group_max(df, col_to_group='Gitter_ID_100m', col_to_max='Anzahl', cols=['Gitter_ID_100m', 'TotalObservations']):
    """
    Groups based on given column and searches for max value in groups.

    :param df: Dataframe to group.
    :param col_to_group: Column to group by. List possible.
    :param col_to_max: Column to find max from.
    :param cols: Column names to be returned.
    :return: Dataframe where max in a specific column has been found for each grouped.
    """
    cell_max = []
    cell_group = df.groupby(col_to_group)
    for name, cell in cell_group:
        max_count = max(cell[col_to_max])
        cell_max.append([name, max_count])
    return pd.DataFrame(cell_max, columns=cols)


def mult_col_dict(df, mapping, new_col, prdne, prdtwo, cond):
    """
    If condition in specified row[col] is met calcs the product of a cell value and a corresponding entry in a dictionary.
    # TODO: Error Handling KeyError - weights[x[prdtwo]] might throw a key error if passed x is not a key in dict.
    :param df: Dataframe to do the multiplication in.
    :param mapping: Dictionary providing the second operand for multiplication.
    :param new_col: Result column to attach to the dataframe.
    :param prdne: First operand of multiplication.
    :param prdtwo: Second operand of multiplication.
    :param cond: Condition on which to trigger multiplication.
    :return: Df with result of multiplication between row member and passed dict.
    """

    for key in mapping:
        weights = mapping.get(key)
        df[new_col] = df.apply(lambda x: (x[prdne]*weights[x[prdtwo]]) if x[cond] == key else 0, axis=1)
    return df




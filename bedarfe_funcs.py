import pandas as pd

from IandO import json_utility as ju
from data_aggregation import data_aggregation as da, ratio_writer as rw, communal_mean as cm
from data_aggregation.age_disaggr import age_aggregation as caa, concat_unique as cu


def import_raw(import_list: list, config_obj: object, nrows) -> list:
    """
    Imports files as specified in config python object.

    :param import_list:
    :param config_obj:
    :return: List of dataframes.
    :exception Throws Key Error if file is not specified in config. Throws FileNotFoundError if specified file cannot be found.
    """
    all_df = []
    haushalte_df = []
    for file_identifier in import_list:

        try:
            filepath = ju.read_json_elements(config_obj, file_identifier, "filepath")
            imp_cols = ju.read_json_elements(config_obj, file_identifier, "columns")
            df = ju.import_csv_to_df(filepath, imp_cols, nrows)
            if file_identifier == "Bevoelkerung":
                df = disaggregate_age(df)
            elif file_identifier == "Haushalte":
                haushalte_df = df_slicer(df, ['Gitter_ID_100m_neu', 'Merkmal', 'Anzahl'], 'Merkmal', 'INSGESAMT',
                                         ['Gitter_ID_100m_neu', 'Anzahl'])
            all_df.append(df)

        except KeyError:
            print(f"The csv {file_identifier} you are trying to import is not specified in the config file."
                  f"No Dataframe has been generated for this identifier. Please specify the identifier in"
                  f" your config file.")

        except FileNotFoundError:
            print(
                f"The file you are trying to load {filepath} does not exist. Please change the filepath in your config json.")

    return all_df, haushalte_df


def concat_df(df_list: list, del_list=None) -> pd.DataFrame:
    """
    Concatenates dataframes from list to one dataframe.

    :param del_list:
    :param df_list: List of dfs
    :return: DF concatentated on rows.
    """
    if del_list is None:
        del_list = ["Auspraegung_Text"]

    df = pd.concat(df_list, axis=0, ignore_index=True)
    for col in del_list:
        del_df_columns(df, col)
    return df


def del_df_columns(df, column: str):
    """
    Deletes column from dataframe

    :param df: Input df.
    :param column: Column as string.
    :return: Dataframe
    """
    del df[column]
    return None


def disaggregate_age(df, renaming=None, duplicate_list=None):
    """
    Disaggregates Age groups into one or two groups.

    :param duplicate_list:
    :param renaming: Provides possibility to rename columns for age disaggregation. Necessary if matching column for concat is not "NAME"
    :param df:
    :return: Concatenated DF with only disaggregated agegroups.
    """
    if renaming is None:
        renaming = {"NAME": "Gitter_ID_100m_neu"}

    dis_aggr = caa(df)
    dis_aggr.rename(columns=renaming, inplace=True)
    return cu(df, dis_aggr, duplicate_list)


def calc_df_ratio(df: pd.DataFrame, attr_grouping: dict, weigths: dict) -> :
    """
    Calculate ratios in df based on provided attr. grouping and weight mapping.

    :param df:
    :param attr_grouping:
    :param weigths:
    :return: Df with ratios written in new column.
    """
    attr_aggregated_df = da.data_aggregation(df, attr_grouping)
    attr_ratio = rw.calculate_weight_cell_ratio_for_attr_val(attr_aggregated_df, weigths)
    return attr_ratio


def calc_sum_ratio(mean_df):
    """
    Calculate mean value of attr_val / sum of this attr_val in Gemeinde

    :param mean_df:
    :return: DF with ratio of summed attr_val
    """
    sum_attr = cm.group_and_sum(mean_df)
    sum_ratio = cm.calc_ratio_to_sum(mean_df, sum_attr)
    return sum_ratio


def calc_count_ratio(value_df):
    """
    Calculate count of attr_val / sum of count of attr_val in Gemeinde

    :param value_df:
    :return:
    """
    distribution = cm.count_grouped(value_df)
    count_ratio = cm.calc_count_countsum_ratio(distribution)
    return count_ratio


def weigh_attr_mean(sum_df, count_df, merge_cols=None):
    """
    Multiplication of two columns in a dataframe after merge

    :return:
    """
    if merge_cols is None:
        merge_cols = ['NAME', 'Merkmal', 'Auspraegung_Code']

    df = sum_df.merge(count_df, on=merge_cols, how='left')
    attr_mean_weight = cm.mean_attr_val(df)
    return attr_mean_weight


def df_slicer(df: pd.DataFrame, cols: list, attr_index: str, search_index: str, search_col: list) -> pd.DataFrame:
    """
    Slice dataframe based on row value in specific column.

    :param df: Input df.
    :param cols: Columns to keep.
    :param attr_index: Index to search through.
    :param search_index: Index value to search for.
    :param search_col: Columns to include in search result.
    :return: Dataframe Slice.
    """
    df = df.loc[:, cols]
    df.index = df.loc[:, attr_index]
    result = df.loc[search_index, search_col]
    return result

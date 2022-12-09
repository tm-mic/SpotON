import pandas as pd


def group_and_mean(df: pd.DataFrame,
                   groupby_list=None, rename_list=None, mean_header='Anzahl') -> pd.DataFrame:
    """ Groups and calculates mean based on given list. Index is reset.
    :param: rename_list: List of rename headers after index reset.
    :param: groupby_list: Control grouping of attributes.
    :return: Dataframe with calculated mean based on list grouping.
    """
    if groupby_list is None:
        groupby_list = ['NAME', 'Merkmal', 'Auspraegung_Code']
    if rename_list is None:
        rename_list = ['NAME', 'Merkmal', 'Auspraegung_Code', 'Anzahl']

    mean_attr_val_gemeinde = df.groupby(by=groupby_list, axis=0).mean(mean_header)
    mean_attr_val_gemeinde = mean_attr_val_gemeinde.reset_index()
    return mean_attr_val_gemeinde[rename_list]


def group_and_sum(df, grouping_list=None, group_col=0, sum_col=1) -> pd.DataFrame:
    """ Groups and sums dataframe based on given list.
    :param: grouping_list: List of columns to be grouped and summed on.
    :param: sum_col: Col to sum over.
    :param: group_col: Col to group by.
    :param: df_to_normalize:
    :return: Dataframe with sum over count of attributes.
    """
    if grouping_list is None:
        grouping_list = ['Merkmal', 'Anzahl']

    df = df.loc[:, grouping_list]
    return df.groupby(grouping_list[group_col]).sum(grouping_list[sum_col])


def calc_ratio_to_sum(dividend_df, divisor_df, quotient_column='Average_ratio', merge_on_col='Merkmal',
                      dividend_col=3, divisor_col=4) -> pd.DataFrame:
    """
    Calculate quotient of two dataframe columns based on inner merge of two dataframes.
    Used for ratio calculation.
    :param: divisor_col: Divisor column passed as int.
    :param: dividend_col: Dividend column passed as int.
    :param: merge_on_col: Specifies the columns to merge on.
    :param: quotient_column: specifies column header of quotient.
    :param: dividend_df: Dataframe containing the dividend values.
    :param: divisor_df: Dataframe containing the divisor values.
    :return: Quotient of two dataframe cols.
    """
    merged = dividend_df.merge(divisor_df, on=merge_on_col, how='inner')
    merged[quotient_column] = merged.iloc[:, dividend_col] / merged.iloc[:, divisor_col]
    return merged


def count_grouped(df: pd.DataFrame,
                  groupby_list=None) -> pd.DataFrame:
    """
    Group and count elements by list.
    :param df:
    :param groupby_list:
    :param rename_list:
    :return: Dataframe with count of grouped elements.
    """
    if groupby_list is None:
        groupby_list = ['NAME', 'Merkmal', 'Auspraegung_Code']

    ratio_attr_val_to_total_gemeinde = df.groupby(by=groupby_list, axis=0).count()
    ratio_attr_val_to_total_gemeinde.reset_index(inplace=True)
    ratio_attr_val_to_total_gemeinde = ratio_attr_val_to_total_gemeinde.rename(columns={'Anzahl': 'Count'})

    return ratio_attr_val_to_total_gemeinde[['NAME', 'Merkmal', 'Auspraegung_Code', 'Count']]


def calc_count_countsum_ratio(df: pd.DataFrame, groupby_list=None, rename_list=None, sum_col='Count',
                              index_reset_list=None, merge_on_cols=None) -> pd.DataFrame:
    """
    Groups and sums over count column. Divides with given count column.
    Sum over grouped elements and create ratio by list.
    :param: sum_col: Col to sum over.
    :param: merge_on_cols: Cols to merge on.
    :param: index_reset_list: Reset multiindex of grouped and summed input dataframe.
    :param: df: input dataframe - should in default include Name, Merkmal, Count as headers
    :param: rename_list: List to of renamed columns.
    :return: Dataframe with quotient of sum over grouped elements divided by merged input dataframe.
    """
    if groupby_list is None:
        groupby_list = ['NAME', 'Merkmal']
    if index_reset_list is None:
        index_reset_list = ['NAME', 'Merkmal', 'Count']
    if rename_list is None:
        rename_list = ['NAME', 'Merkmal', 'Auspraegung_Code', 'Count_Ratio']
    if merge_on_cols is None:
        merge_on_cols = ['NAME', 'Merkmal']

    countsum = df.groupby(groupby_list).sum(sum_col).reset_index()[index_reset_list]
    result_df = df.merge(countsum, how='left', on=merge_on_cols)
    result_df['Count_Ratio'] = result_df['Count_x'] / result_df['Count_y']
    return result_df[rename_list]


def mean_attr_val(df: pd.DataFrame, mul_col='Anzahl', cols_to_mul=None):
    """Multiply two dataframe columns based on column index.
    :param: mul_col: name of result col.
    :param: cols_to_mul: Tuple of df columns to multiply with each other.
    :return: Dataframe with two multiplied columns.
    """
    if cols_to_mul is None:
        cols_to_mul = (5, 6)

    df[mul_col] = df.iloc[:, cols_to_mul[0]] * df.iloc[:, cols_to_mul[1]]
    return df

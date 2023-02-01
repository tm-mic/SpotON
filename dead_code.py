from typing import Tuple

import geopandas
import geopandas as gpd
import pandas
import pandas as pd

from IandO import json_utility as ju
from bedarfe import disaggregate_age_attr as caa

from geometry_operations import coord_to_polygon
from import_funcs import id_to_lat_lon


def import_raw(import_list: list, config_obj: object, nrows=1) -> Tuple[pd.DataFrame]:
    """
    Imports files as specified in config python object.

    :param import_list:
    :param config_obj:
    :return: Tuple of dataframes.
    :exception Throws Key Error if file is not specified in config. Throws FileNotFoundError if specified file cannot be found.
    """
    all_df = []
    haushalte_df = []

    for file_identifier in import_list:

        try:
            filepath = ju.read_json_elements(config_obj, file_identifier, "filepath")
            imp_cols = ju.read_json_elements(config_obj, file_identifier, "columns")
            df = import_csv_chunks(filepath, imp_cols, nrows)

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


def calc_df_ratio(df: pd.DataFrame, attr_grouping: dict, weigths: dict) -> pd.DataFrame:
    """
    Calculate ratios in df based on provided attr. grouping and weight mapping.

    :param df:
    :param attr_grouping:
    :param weigths:
    :return: Df with ratios written in new column.
    """
    attr_aggregated_df = data_aggregation(df, attr_grouping)
    attr_ratio = calculate_weight_cell_ratio_for_attr_val(attr_aggregated_df, weigths)
    return attr_ratio


def calc_sum_ratio(mean_df):
    """
    Calculate mean value of attr_val / sum of this attr_val in Gemeinde

    :param mean_df:
    :return: DF with ratio of summed attr_val
    """
    sum_attr = group_and_sum(mean_df)
    sum_ratio = calc_ratio_to_sum(mean_df, sum_attr)
    return sum_ratio


def calc_count_ratio(value_df):
    """
    Calculate count of attr_val / sum of count of attr_val in Gemeinde

    :param value_df:
    :return:
    """
    distribution = count_grouped(value_df)
    count_ratio = calc_count_countsum_ratio(distribution)
    return count_ratio


def weigh_attr_mean(sum_df, count_df, merge_cols=None):
    """
    Multiplication of two columns in a dataframe after merge

    :return:
    """
    if merge_cols is None:
        merge_cols = ['NAME', 'Merkmal', 'Auspraegung_Code']

    df = sum_df.merge(count_df, on=merge_cols, how='left')
    attr_mean_weight = mean_attr_val(df)
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
    :param grouping_list: List of columns to be grouped and summed on.
    :param sum_col: Col to sum over.
    :param group_col: Col to group by.
    :param df_to_normalize:
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
    :param divisor_col: Divisor column passed as int.
    :param dividend_col: Dividend column passed as int.
    :param merge_on_col: Specifies the columns to merge on.
    :param quotient_column: specifies column header of quotient.
    :param dividend_df: Dataframe containing the dividend values.
    :param divisor_df: Dataframe containing the divisor values.
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


def data_aggregation(
        imported_df,
        mapping_dict: dict,
        return_dataframe: bool = True,
) -> pd.DataFrame:
    """data aggregation function is used to aggregate attributes over predefined groups by unique crs.
    Input:
    [1] imported dataframe containing at least following columns ["Gitter_ID_100m_neu", "Merkmal", "Auspraegung_Code", "Anzahl"]
    [2] path to metadata
    [3] dictionary mapping existing attribute values to desired grouped attribute values

    Return:
    The function returns a dataframe with the 4 rows specified above. CRS cells are unique, Merkmale are grouped(attr value) and aggregated (count)."""
    # for single_attr in dict_of_valid_attr.keys():
    #     check_if_attr_exists(single_attr, dict_of_valid_attr)

    nec_columns = ["Gitter_ID_100m_neu", "Merkmal", "Auspraegung_Code", "Anzahl"]
    try:
        imported_df = imported_df[nec_columns]
    except KeyError:
        print(
            f'The keys necessary for this function do not exist in your dataframe. You need to include {nec_columns} when importing for this function to work.')

    # pass mapping of valid attr-values and data to be aggregated to
    result_list = []
    # TODO: avoid running through imported df as often attributes are requested --> possibly using zip() function
    for single_attr in mapping_dict.keys():
        for index, row in imported_df.iterrows():
            if row.Merkmal == single_attr:
                current_code = row.Auspraegung_Code
                aggr_code = mapping_dict[single_attr].get(current_code)
                row_to_append = [row.Gitter_ID_100m_neu, single_attr, aggr_code, row.Anzahl]
                result_list.append(row_to_append)

    result_df = pd.DataFrame(result_list, columns=nec_columns)

    attr_values = result_df.groupby(['Gitter_ID_100m_neu', 'Merkmal', 'Auspraegung_Code'], group_keys=True)[
        'Anzahl'].sum()

    if return_dataframe:
        attr_values = attr_values.reset_index(name='Anzahl')
    return attr_values


def calculate_weight_cell_ratio_for_attr_val(aggregated_data, weight_mapping, default_weight=0, print_attr_for_cell=False,
                                             included_columns=None):
    """The function to calculat averaged and weigthed ratios for each cell and attribute value combination.


    Returns:
        This function returns a weighted average for each attr_value combination in each cell.


    Parameters:
        [1] Dataframe with following necessary columns: ["Gitter_ID_100m_neu", "Merkmal", "Auspraegung_Code", "Anzahl"] <--> to be sure not to throw errors import your
        data via the IandO module and aggregate the data via general_data_aggregation function

        [2] A weight mapping dict of dicts specifying a weight between 0 - 1 (or any other pre-determined scale) for each attr_value combination
        f.e.


    If no weight is specified or a key error is encountered the default weight of 0 is assumed and your results will not be valid.


    Example Input with csv import specified:

        meta_data_path = IandO.pull_path_column_list_in_config('/Users/msccomputinginthehumanities/PycharmProjects/SpotON/fileconfig.txt',
                                              'Gebaeude')[2]

        imported_df = IandO.import_single_file_as_UTF(
            '/Users/msccomputinginthehumanities/PycharmProjects/SpotON/fileconfig.txt', 'Gebaeude', nrows=5000)

        mapping_dict = {
            'INSGESAMT' : {0:0},
            'GEBTYPGROESSE': {1: 1, 2: 1, 3: 1, 4: 1, 5: 1, 6: 1, 7: 2, 8: 2, 9: 2, 10: 2},
            'EIGENTUM': {1: 1, 2: 1, 3: 1, 4: 2, 5: 2, 6: 3, 7: 3, 8: 3},
            'WOHNEIGENTUM': {1: 1, 2: 2, 3: 2, 4: 2, 99: 0},
            'HEIZTYP': {1: 3, 2: 3, 3: 15, 4: 15, 5: 15, 6: 15}

        weight_mapping = {
            'GEBTYPGROESSE': {1:1, 2:1},
            'EIGENTUM':{1:1, 2:1,3:1},
            'Heiztyp':{3:0, 15:0.9}}
        aggregated_data = general_data_aggregation.data_aggregation(imported_df, meta_data_path, mapping_dict)


    Note: the arithmetic middle of two ratios [1. ratio of ratio_value=(attr_values / attr_count) and ratio_to_total=(attr_values / total observation in the cell)]
    are calculated to adjust for the fact that the total of observations is often unequal to the amount specified for certain
    attr. Without this middeling of values certain attributes would be overstated in their importance.
    If total_count of observations and sum of attribute observations is equal this operation does not have an effect."""

    if included_columns is None:
        included_columns = ['Gitter_ID_100m_neu', 'Merkmal', 'Auspraegung_Code', 'Weighted_Ratio']

# ToDo:use keys to identify groups not unique statement
    crs_list = get_unique_crs(aggregated_data, 0)
    result_df = pd.DataFrame(columns=included_columns)
    for unique_cell_crs in crs_list:
        queried_group = aggregated_data.query('Gitter_ID_100m_neu == @unique_cell_crs')

# ToDo: seperate max function
        max_attr_count = max(queried_group['Anzahl'])

# ToDo: seperate attr.sum() function
        attr_count_sum = queried_group.groupby(['Gitter_ID_100m_neu', 'Merkmal'], group_keys=True)['Anzahl'].sum()
        attr_count_sum = attr_count_sum.reset_index(name='Anzahl')

# ToDo: seperate ratio calc into function
        ratio_df = queried_group.merge(attr_count_sum, on=['Gitter_ID_100m_neu', 'Merkmal'], how='left')
        ratio_df[['Ratio']] = ratio_df[['Anzahl_x']].div(ratio_df['Anzahl_y'], axis=0)

        ratio_df['Max Total'] = max_attr_count
        ratio_df[['Ratio to Total']] = ratio_df[['Anzahl_x']].div(ratio_df['Max Total'], axis=0)

        attr_spec_weighted_ratios = []

# ToDo: [1] replace itertuples with column operations f.e. apply [2] push into function
        for row in ratio_df.itertuples(index=False):
            attr_code = row[2]

            try:
                attr_weight = weight_mapping[row[1]].get(attr_code)
                if attr_weight is False:
                    attr_weight = default_weight

            except KeyError:
                attr_weight = 0

            ratio_value = row[5]
            ratio_to_total = row[7]
            arithmetic_middle_ratio = (ratio_value+ratio_to_total)/2

            attr_spec_weighted_ratios.append(arithmetic_middle_ratio * attr_weight)
        ratio_df['Weighted_Ratio'] = attr_spec_weighted_ratios

        if print_attr_for_cell:
            print(ratio_df)

        result_df = pd.concat([result_df, ratio_df], ignore_index=True, sort=False)[result_df.columns]

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
    return pd.concat([df_appended, drop_kurz]).drop_duplicates(duplicate_list, keep='last').reset_index().sort_values(
        by=duplicate_list)


def drop_df_values(df, drop_val='ALTER_KURZ'):
    """
    Drop attr. values from dataframe.

    :param df: input
    :param drop_val: Attr to drop
    :return: Df without attr values matching drop_val
    """
    return df.drop(df[df.Merkmal == drop_val].index)


def calc_convex_hull(polygon):
    """Creates convex hull from Polygon

    :param: shapely point polygon
    :return: polygon with the smallest convex hull which contains all points of the input polygon
    """
    return polygon.convex_hull


def get_unique_crs(dataframe, crs_column_int):
    """Function to return unique values from any dataframe column."""
    return pd.unique(dataframe.iloc[:, crs_column_int])


def find_extrema_from_id(df, bl, find_x=True):
    """
    Calulates the extrema of a Polygon by the CRS-ID passed.
    Basically slices ID into X and Y component and extracts maximum entry by index.

    :param df: Data Containing CRS_ID as in "Gitter_ID_100m"
    :param bl: Bundesland polygon to get extrema from
    :param find_x: return x extrema if true (default); otherwise return y extrema
    :return: Tuple of extrema values as ndarrays as returned by pandas .loc
    """
    if find_x:
        x = df.loc[df['Bundesland'] == bl, 'Gitter_ID_100m'].apply(id_to_lat_lon).apply(lambda val: val[1])
        return x, df.loc[x.idxmax(), :], df.loc[x.idxmin(), :]
    else:
        y = df.loc[df['Bundesland'] == bl, 'Gitter_ID_100m'].apply(id_to_lat_lon).apply(lambda val: val[0])
        return y, df.loc[y.idxmax(), :], df.loc[y.idxmin(), :]


def create_head_tail_geoseries(df, id_column = 'Gitter_ID_100m'):
    """
    Creates a 2-Row geoseries with the head(1) and tail(1) of the passed dataframe.
    :param df: Any dataframe with
    :return:
    """
    first_point = geo_points_from_id(df.head(1).loc[:, id_column].values[0])
    last_point = geo_points_from_id(df.tail(1).loc[:, id_column].values[0])
    return gpd.GeoSeries([first_point[0], last_point[0]]).set_crs(crs='EPSG:3035')


def max_val(df, col_name: str):
    """
    Gets Max Value from specific column in df. Column must be int or float.

    :param df: Input df.
    :param col_name: Column name as str. to search max()
    :return: max() of column.
    """
    return df[col_name].max()


def get_series_extrema(series):
    """
    Calculates series minimum and series maximum.

    :param series: Data int or float series
    :return: Tuple (min,max)
    """
    return series.min(), series.max()


def test_imported_chunk_in_hull(geoSeries, convex_hull):
    """
    Check if first or last value are in the convex hull of a polygon.

    :param df: Dataframe to check
    :param convex_hull: convex hull of a polygon
    :return: If first or last value is in the polygon true is returned. Else false is returned.
    """
    convex_hull = convex_hull.values[0]
    truth_table = geoSeries.within(convex_hull)
    return truth_table.any()


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


def polygon_creator_by_crs_values(df_to_transform_to_gdf_by_crs_column, crs_column="Gitter_ID_100m_neu", crs='EPSG:3035'):
    polygon_df = coord_to_polygon.create_coordinate_tuple_dataframe(
        df_to_transform_to_gdf_by_crs_column[crs_column].unique())

    polygon_df_new = coord_to_polygon.add_coordinate_tuple_plus100_to_dataframe(polygon_df)
    polygon_df_new = coord_to_polygon.create_polygon_tuples_from_dataframe(polygon_df_new)
    polygon_df_new = coord_to_polygon.create_polygon_point_list(polygon_df_new)
    polygon_df_new = coord_to_polygon.create_shapely_polygons(polygon_df_new)
    polygon_df_new = coord_to_polygon.create_geodataframe(polygon_df, polygon_df_new)
    polygon_df_new.rename(columns={0: crs_column}, inplace=True)
    df_to_transform_to_gdf_by_crs_column = df_to_transform_to_gdf_by_crs_column.merge(polygon_df_new,
                                                                                      on=crs_column,
                                                                                      how="left")
    return geopandas.GeoDataFrame(df_to_transform_to_gdf_by_crs_column, crs=crs)


def calc_cluster(gdf: gpd.GeoDataFrame, EPSG="EPSG:4326"):
    """
    Chooses random polygon in gdf and finds cluster of touching geometries (at least one point is touching - no overlaps).

    :param EPSG: Output EPSG.
    :param gdf: Geodataframe to sample from.
    :return: Geodataframe with only touching polygons.
    """
    # TODO: change identifier_col to Gitter_ID_100m_NEU
    col_head = gdf.columns
    cluster_df = pd.DataFrame(columns=col_head)
    cell_df = pd.DataFrame(columns=col_head)
    try:
        cell = gdf.sample()
    except:
        return 0
    polygon = cell['geometry'].unary_union

    while True:
        touching_poly = find_touching_geo(gdf, polygon)
        cluster_df = pd.concat([cluster_df, touching_poly])
        cell_df = pd.concat([cell_df, cell])

        todo_df = select_rows_not_in_other_df(cluster_df, cell_df)
        if todo_df.empty:
            break

        cell = gpd.GeoDataFrame(todo_df.sample(), geometry='geometry')
        polygon = cell['geometry'].unary_union

    return gpd.GeoDataFrame(cluster_df, geometry='geometry', crs=EPSG)


def find_touching_geo(gdf: gpd.GeoDataFrame, polygon):
    """
    Checks is a geometry has any touching geometries in passed Geodataframe.

    :param gdf: Input Geodataframe
    :param polygon: Geometrie to check for touches
    :return: Geodataframe of touching geometries. Does not include passed polygon.
    """
    gdf['match'] = gdf.touches(polygon)
    return gdf.loc[gdf['match']==True]


def select_rows_not_in_other_df(gdf1, gdf2):
    """
    Checks for geometries in gdf1 that are not in gdf2

    :param gdf1: Geometries to keep.
    :param gdf2: Geometrie filter.
    :return: Returns (Geo)dataframe with Rows not in gdf2.
    """
    return gdf1[~gdf1.OBJID.isin(gdf2.OBJID).dropna(how='all')]


def get_quantile_df(df: pd.DataFrame, col, quant = 0.75, numeric_only=True):
    """
    Calculates top quantile of dataframe in given column.

    :param col: Column to calc the quantile over. Has to be numeric.
    :param df: Input dataframe.
    :return: Df only containing
    """
    try:
        quantile = df.loc[:, col].quantile(quant)
    except TypeError:
        print(f"The column '{col}' you have entered to calculate the quantile over is not numeric // date time. "
              "Please chose a different column."
              " You can find more information in the official docs: https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.quantile.html")

    return df.where(df[col] >= quantile).dropna(how='all')


def import_csv_chunks(filepath: str, header_list: object, type_conversion_dict=None, seperator=None, nrows=1,
                      chunks=None) -> pd.DataFrame:
    """
    Read csv to DataFrame from passed file.
    :param chunksize:
    :param type_conversion_dict: Conversion of types to reduce memory use.
    :param filepath: Path to csv file.
    :param header_list: List of headers to be read.
    :param nrows: Count of rows to import. Default "1" will import all rows of csv.
    :return: DataFrame with specified columns.
    """
    try:
        if nrows == 1:
            return pandas.read_csv(filepath, sep=seperator, header=0, usecols=header_list,
                                   engine='python', encoding_errors='replace', on_bad_lines="skip",
                                   dtype=type_conversion_dict, chunksize=chunks)
        else:
            return pandas.read_csv(filepath, sep=seperator, header=0, usecols=header_list,
                                   engine='python', encoding_errors='replace', on_bad_lines="skip", nrows=nrows,
                                   dtype=type_conversion_dict, chunksize=chunks)
    except ValueError:
        return pandas.read_csv(filepath, sep=seperator, header=0, engine='python', encoding_errors='replace',
                               on_bad_lines="skip", nrows=nrows, dtype=type_conversion_dict, chunksize=chunks)


def geo_points_from_id(id: str):
    """
    Creates points from crs ID by slicing N/E info from ID.

    :return:
    """
    north = id_to_lat_lon(id)[0]
    east = id_to_lat_lon(id)[1]
    df = pd.DataFrame(data={'x': [east], 'y': [north]})
    return gpd.points_from_xy(df['x'], df['y'], crs='EPSG:3035')


def calc_poly_extrema(polygon):
    """
    Calculates the extrema values of a polygon on one axis.
    :param polygon: Polygon or any other geometry.
    :return:
    """
    return polygon.total_bounds.tolist()


def data_poly_import(filepath: str, data_columns: list, poly_extrema: list, chunks: int, conversion: dict):
    result: pd.DataFrame = pd.DataFrame()
    c = 0
    while True:
        imported_data = pd.read_csv(filepath, usecols=data_columns, engine='python', encoding_errors='replace',
                                    on_bad_lines="skip", chunksize=chunks, sep=r'(,|;)', dtype=conversion)

        for data in imported_data:
            in_poly = ID_in_poly(poly_extrema, data)

            if in_poly[0]:
                result = result.append(in_poly[1])
                c += 1
                print(
                    f'\n{c} chunks of {filepath} have been added to the dataframe so far. \nThe current df has a size of {result.shape}')

            north_min_mask = calc_coord_mask(poly_extrema[1], data, xoy='y')
            north_max_mask = calc_coord_mask(poly_extrema[3], data, xoy='y')

            if all_smaller(north_min_mask):
                print("None of imported data in range of Polygon.")
                continue

            if all_bigger(north_max_mask):
                print("This part of the dataset is north of the given Polygon. Data import is stopped.")
                break

            else:
                continue

        break
    return result


def converter(df, col='Anzahl'):
    return pd.to_numeric(df.loc[:, col], errors='coerce', downcast='integer')


def ID_in_poly(poly_extrema: list, data):
    """
    Gives back data which fall into the rectangle bounded by polygon extrema values [xmin, ymin, xmax, ymax].
    X values and Y values are calculated based the Gitter_ID_100m. Only Data that do fall into the range
    xmin - xmax and ymin - ymax are returned in the result dataframe.

    :param df: df to append to
    :param data: data that are being imported
    :param poly_extrema: extrema points of a polygon
    :param data_columns: columns to return
    :return: DataFrame of data inside a max polygon bounding box.
    """
    north_max = poly_extrema[3]
    north_min = poly_extrema[1]
    east_min = poly_extrema[0]
    east_max = poly_extrema[2]

    north_mask = calc_coord_mask(north_min, data, unary=False, xoy="y", cond_val_two=north_max)
    if north_mask.any():
        data.drop(data[~north_mask].index, inplace=True)
        east_mask = calc_coord_mask(east_min, data, unary=False, xoy="x", cond_val_two=east_max)
        if east_mask.any():
            data.drop(data[~east_mask].index, inplace=True)
            return True, data
        else:
            return False, None
    else:
        return False, None


def calc_coord_mask(cond_val_one: int, data: pd.DataFrame, xoy='y', unary=True, cond_val_two=None) -> pd.Series:
    """
    Creates a conditional mask(bools) based on unary x < val_one operation if unary == True. If unary == False
    creates a conditional mask (bools) based on val_one < x < val_two condition.

    :param cond_val_one: val_one for x < val_one or val_one < x < val_two check
    :param data: df
    :param xoy: to check with x or y value
    :param cond_val_two: second value for val_one < x < val_two operation
    :return: pd.Series of bool values
    """
    if unary:
        if xoy == 'y':
            mask_vals = get_lat_lon_series(data, north=True)
        else:
            mask_vals = get_lat_lon_series(data, north=False)
        return mask_vals.apply(check_against_cond_val, args=(cond_val_one,))
    else:
        if xoy == 'y':
            mask_vals = get_lat_lon_series(data, north=True)
        else:
            mask_vals = get_lat_lon_series(data, north=False)
        return mask_vals.apply(check_value_in_range, args=(cond_val_one, cond_val_two,))


def all_bigger(mask: pd.Series) -> bool:
    """
    Returns False if condition True applies to all rows in a pd.Series.
    Used to create mask for data import check.

    :param mask:
    :return:
    """
    if mask.all():
        return False
    else:
        return True


def all_smaller(mask: pd.Series) -> bool:
    """
    Returns true if condition True applies to all rows in a pd.Series.
    Used to create mask for data import check.

    :param mask:
    :return:
    """

    if mask.all():
        return True
    else:
        return False


def check_against_cond_val(input_val: int, cond_val: int) -> bool:
    """
    Checks if a given input value is smaller than the condition.

    :param input_val:
    :param cond_val:
    :return:
    """
    if cond_val > input_val:
        return True
    else:
        return False


def check_value_in_range(input_val: int, minimum: int, maximum: int) -> bool:
    """
    Checks if an input value lies in between two given values min and max.

    :param input_val:
    :param minimum:
    :param maximum:
    :return:
    """
    if minimum < input_val < maximum:
        return True
    else:
        return False


def get_lat_lon_series(df: pd.DataFrame, north=True) -> pd.Series:
    """
    Extracts y(north) and x(east) values from coordinate IDs as specified by DE_GRID ETRS89 UTM32 100m

    :param df:
    :param north:
    :return: pd.Series of ints with
    """
    if north:
        return df.loc[:, 'Gitter_ID_100m'].apply(id_to_lat_lon).apply(lambda val: val[0])
    else:
        return df.loc[:, 'Gitter_ID_100m'].apply(id_to_lat_lon).apply(lambda val: val[1])

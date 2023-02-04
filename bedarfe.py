# function written by Christian L.
import numpy as np
import pandas
import pandas as pd
from pandas import DataFrame
import random as rnd
import geopandas as gpd


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

    group_one = int(val_sum) * splitter
    group_two = int(val_sum) - group_one
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

    keys = group_mapping.get(int(orig_group))
    res_list = []

    group = keys[0]
    res_list.append([row[1], new_attr, group, distro_val[0], row[5], row[6], row[7]])
    group = keys[1]
    res_list.append([row[1], new_attr, group, distro_val[1], row[5], row[6], row[7]])

    return res_list


def disaggregate_age_attr(
        df: pd.DataFrame,
        dis_low=0.45,
        dis_high=0.55,
        alter_kurz='ALTER_KURZ',
        attr_ident='ALTER_10JG'
) -> DataFrame:
    """
    Runs through dataframe and disaggregates attr val data into provided groups by given splitter value.

    :param df: raw data to pass.
    :param dis_low: lower bound of rnd value
    :param dis_high: higher bound of rnd value
    :param alter_kurz: name of age attr to be disaggregated
    :param attr_ident: new attr identifier to be written back to dataframe
    :return: Dataframe containing only disaggregated age groups.
    """
    alter_kurz = df.loc[df['Merkmal'] == alter_kurz]
    result_list = []
    cols = df.columns
    # TODO: drop itertuples for faster vector implementation
    for row in alter_kurz.itertuples():
        splitter = rnd.uniform(dis_low, dis_high)
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


def remap_groups(df: pd.DataFrame, mapping: dict):
    """
    Based on a dict passed the Auspraegung Code of an Attr is changed.


    :param df: Dataframe to regroup
    :param mapping: dictionary providing a mapping to regroup. All groups that exist in df
    should be specified here.
    :return: Dataframe with new Attribute Codes based on mapping provided in mapping dict.
    """
    mapping_keys = list(mapping.keys())
    df = df.loc[df['Merkmal'].isin(mapping_keys)]
    for key in mapping_keys:
        group_map = mapping.get(key)
        df['Auspraegung_Code'] = df.apply(
            lambda x: group_map[x['Auspraegung_Code']] if x['Merkmal'] == key else x['Auspraegung_Code'], axis=1)
    return df


def calc_group_max(df: pd.DataFrame, col_to_group='Gitter_ID_100m', col_to_max='Anzahl',
                   cols=['Gitter_ID_100m', 'TotalObservations']):
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


def mult_col_dict(df: pd.DataFrame, mapping: dict, new_col: str, prdne, prdtwo, cond):
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
        df[new_col] = df.apply(lambda x: (x[prdne] * weights[x[prdtwo]]) if x[cond] == key else 0, axis=1)
    return df


def calc_cell_index(gemeinde_group, weight_map, index_columns, interest_area):
    """
    Calculates index for each cell in a gemeinde group. Infered gemeinde values are added to the cell index value.

    :param gemeinde_group: df grouped by gemeinde
    :param weight_map: map of attribute weights passed from the config.json
    :param index_columns: columns to include in the result
    :param interest_area: area of interest is added as column to the result list
    :return: list of lists containinig cell indices
    """
    # TODO: seperate gemeinde fill and index calc in calc cell index func
    # TODO: extract functions
    index_list = []
    for name, gem in gemeinde_group:
        codes_count = get_code_counts(gem)
        sum_codes = group_and_sum_code_counts(gem)
        attr_median = inner_merge_df(sum_codes, codes_count)
        attr_median['Calc Distro Attr/Cell'] = calc_attr_median(attr_median)
        attr_ratio = count_attr_in_gem(gem)
        gem_vals = infer_gem_vals(attr_median, attr_ratio, weight_map)
        cell_gem_group = gem.groupby('Gitter_ID_100m')
        for id, cell in cell_gem_group:
            geo_point, index = sum_cell_index(cell, gem_vals, index_columns)
            index_list.append([id, interest_area, name, index, geo_point])
    return index_list


def count_attr_in_gem(gem):
    """
    Counts the number of occurences of an attribute in a gemeinde group object. Shows the share of an attribute
    to the total number of attributes in a group.

    :param gem: Group Object.
    :return: Returns a df with a share for each attribute in the gemeinde. Index is reset.
    """
    return gem.value_counts(subset=['Merkmal'], normalize=True).reset_index()


def sum_cell_index(cell, gem_vals, cols):
    cell, geo_point = purge_duplicates(cell, gem_vals, cols)
    index = cell['Attr Index'].sum()
    return geo_point, index


def purge_duplicates(cell, gem_vals, cols):
    """
    Purge duplicate entries identified by ['Merkmal', 'Auspraegung_Code'] from cell group.
    Duplicates occur because of gemeinde fill values being added as additional values to calculate cell index.

    :param cell: group of cell values
    :param gem_vals: gemeinde fill values
    :param cols: Columns to append the gemeinde fill values by
    :return: cell group purged of logically duplicate values; geo_point used to fill the missing geometries left by the merge of the gemeinde fill values
    """

    geo_point = grab_any_valid_value(cell, 'geometry')
    cell = cell.append(gem_vals)[cols].reset_index().set_crs(crs='EPSG:3035')
    mask = cell.duplicated(subset=['Merkmal', 'Auspraegung_Code'], keep='first')
    cell = rem_by_mask(cell, mask)
    return cell, geo_point


def grab_any_valid_value(df, col):
    """
    Grabs a valid (non nan) value of a column in a passed df.

    :param df: Dataframe
    :param col: Column to grab value from
    :return: Value of unspecified dtype.
    """
    return df[col].dropna().values[0]


def inner_merge_df(sum_codes, codes_count, on=['Merkmal', 'Auspraegung_Code']):
    return sum_codes.merge(codes_count, on=on, how='inner')


def get_code_counts(gem):
    """
    Counts occurrences of attribute - attribute code combinations.
    "How many observations are in the Gemeinde?"

    :param gem: Group object.
    :return: A group object / df with counts per [attributes and attribute code]. Index is reset.
    """
    return gem.value_counts(subset=['Merkmal', 'Auspraegung_Code']).reset_index().rename({0: "Counts"}, axis=1)


def calc_attr_median(attr_distro):
    """
    :param attr_distro: df containing sum of number of occurences and count of attribute - attribute code combinations
    :return: Df with Df[Calc Distro Attr/Cell] containing the median value for each attribute - attribute code combination in a group.
    """
    return attr_distro['Anzahl'].div(attr_distro['Counts'])


def infer_gem_vals(attr_median, attr_ratio, weight_map):
    """
    Calculates a gemeinde fill value based on the median value for each attribute-attribute code combination and
    the absolute share of attribute occurence in the Gemeinde.
    Gemeinde fille values are weighted by the weight_map provided.


    :param attr_median: Median value for each attribute-attribute-code combination in a gemeinde
    :param attr_ratio: Share of attribute value in gemeinde occurences.
    :param weight_map: weight mapping as specified in config.json
    :return: Df indicating a weighted Gemeinde Fill interpreted as Attribute Index for the Gemeinde passed.
    """
    gem_vals = attr_median.merge(attr_ratio, on='Merkmal', how='inner').rename({0: 'Ratio'}, axis=1)
    gem_vals['Gemeinde Fill Values'] = gem_vals['Calc Distro Attr/Cell'] * gem_vals['Ratio']
    gem_vals = mult_col_dict(gem_vals, weight_map, new_col='Attr Index', prdne='Gemeinde Fill Values',
                             prdtwo='Auspraegung_Code', cond='Merkmal')
    gem_vals.insert(8, "geometry", np.nan, True)
    return gpd.GeoDataFrame(gem_vals, geometry='geometry', crs='EPSG:3035')


def group_and_sum_code_counts(gem, cols=['Merkmal', 'Auspraegung_Code'], col_count='Anzahl'):
    """
    Sums the number of occurrences of every attribute - attribute code combination.

    :param gem: Group Object / Df
    :return: Group Object / Df with sums of groups.
    """
    return gem.groupby(cols)[col_count].sum().reset_index()


def normalize_column(series: pd.Series, new_max=1, new_min=0) -> float:
    """
    Casts series data points between 0-1 based on min and max values in series.

    :param: date_to_norm: int or float value to normalize.
    :param: abs_min: Min val. in scope.
    :param: abs_max: Max val. in scope.
    :return: Normalized value between 0-1 of Dataframe.
    """
    abs_max = series.max()
    abs_min = series.min()
    return series.apply(lambda date: ((date - abs_min) / (abs_max - abs_min)) * (new_max - new_min) + new_min)


def calc_gemeinde_index(gem_group):
    """
    Index is calculated as sum(Count_Haushalte * Cell_Index)

    :param gem_group:
    :return: Dictionary with Gemeinde name as key and gemeinde Index as value
    """
    gem_idx = {}
    for name, gem in gem_group:
        gem_index = gem.sum(numeric_only=True)['Haushalte_Index']
        gem_idx.update({name: gem_index})
    return gem_idx


def calc_sum_zba(gem_idx_dict):
    """
    Calc the sum of all Gemeinden in the Zulassungsbezirk.

    :param gem_idx_dict:
    :return: Dataframe containing the sum of all gemeinde indeces.
    """
    return pd.DataFrame.from_dict(gem_idx_dict, orient='index').reset_index().sum()[0]


def calc_gem_ratio(gemeinde_idx_dict, sum_zba):
    """
    Calculates the ratio of gemeinde index and the sum of all gemeinde indices in the zba.

    :param gemeinde_idx_dict:
    :param sum_zba:
    :return:
    """
    idx_keys = gemeinde_idx_dict.keys()
    ratio_dict = {}
    for key in idx_keys:
        gem_idx = gemeinde_idx_dict.get(key)
        ratio = gem_idx / sum_zba
        ratio_dict.update({key: ratio})
    return ratio_dict


def calc_num_ev_gem(ratios: dict, anzahl_evs_zb: int) -> dict:
    """
    Multiplies the count of EV in ZBA with the gemeinde ratio.

    :param ratios:
    :param anzahl_evs_zb:
    :return: Dict with number of cars in a gemeinde.
    """
    ev_dict = {}
    for key in ratios.keys():
        ev_dict.update({key: ratios.get(key) * anzahl_evs_zb})
    return ev_dict


def calc_cars_in_interest_area(gemeinde_ladestationen_poly, index_df, interest_area: str):
    gemeinde_ladestationen_poly['NAME_Zula'] = gemeinde_ladestationen_poly['NAME_Zula'].str.upper()
    gemeinde_ladestationen_poly['NAME_Zula'] = gemeinde_ladestationen_poly['NAME_Zula'].str.replace('Ü', 'UE')
    gemeinde_ladestationen_poly['NAME_Zula'] = gemeinde_ladestationen_poly['NAME_Zula'].str.replace('Ä', 'AE')
    gemeinde_ladestationen_poly['NAME_Zula'] = gemeinde_ladestationen_poly['NAME_Zula'].str.replace('Ö', 'OE')

    interest_area_ladestationen_poly = gemeinde_ladestationen_poly.loc[
        gemeinde_ladestationen_poly['NAME_Gemeinde'] == interest_area]
    # TODO: Implement flexible handly for all types of aoi (Bundesland, Landkreis, Zulassungsbezirk, Gemeinde)

    anzahl_evs_zb = interest_area_ladestationen_poly['EVIng'].iloc[0]

    ratios = calc_zula_ratio(index_df)
    car_count = calc_num_ev_gem(ratios, anzahl_evs_zb)

    interest_area_ladestationen_poly.insert(10, 'EVGem',
                                            interest_area_ladestationen_poly['NAME_Gemeinde'].map(car_count))

    interest_area_ladestationen_poly.insert(11, 'Bedarf Ladepunkte',
                                            ((interest_area_ladestationen_poly['EVGem'] / 11) -
                                             interest_area_ladestationen_poly['Anzahl Ladepunkte']))

    return interest_area_ladestationen_poly


def add_gemeinde_index(index_df, ratios):
    index_df.insert(6, 'Gemeinde_Index', index_df['Gemeinde'].map(ratios))

    return index_df


def add_haushalte_index(index_df):
    index_df.insert(5, 'Haushalte_Index',
                    (index_df['Anzahl'].astype('float64') * index_df['Cell Index'].astype('float64')))

    return index_df


def calc_zula_ratio(index_df):
    gem_group = index_df.groupby(by='Gemeinde')
    gemeinde_idx_dict = calc_gemeinde_index(gem_group)
    sum_zba = calc_sum_zba(gemeinde_idx_dict)
    ratios = calc_gem_ratio(gemeinde_idx_dict, sum_zba)

    return ratios

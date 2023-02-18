# function written by Christian L.
import numpy as np
import pandas
import pandas as pd
from pandas import DataFrame
import random as rnd
import geopandas as gpd
import unittest


class TestBedarfe(unittest.TestCase):

    def test_split_val_by_share(self):
        self.assertEqual(split_val_by_share(1, 0.5))


def split_val_by_share(val_in: int, share: float) -> tuple:
    """
    Calcs two values from one with val1 + val2 = 1, if given share value rangeing from 0-1.

    :param val_in: Original value to be distributed.
    :param share: Variable to split val. by.
    :return: Tuple of two floats, summing to approx 1.
    :exception Throws Attribute Error if splitter not between 0-1.
    """
    if share > 1 or share < 0:
        print("The splitter value you have passed in the age disaggregation is not valid.\n"
              "The value has to range between 0-1 to return sensical results for the index calc."
              f"Splitter value given {share}\n"
              f"Splitter value required float: 0-1")
        raise AttributeError

    val_one = int(val_in) * share
    val_two = int(val_in) - val_one
    return val_one, val_two


def reallocate_vals(orig_group: int, distro_val: tuple, row: tuple, new_attr: str,
                    group_mapping={1: [1, 2], 2: [3, -1], 3: [4, 5], 4: [6, 7], 5: [8, 9]}) -> list:
    """
    Allocates values given in pairs into one or two groups based on mapping provided.

    :param orig_group: Group to be split.
    :param distro_val: Original group values as distribution tuple to be split.
    :param row: Dataframe row passed containing original data.
    :param new_attr: Attribute Name to be inserted for calc values to be identified later.
    :return: List of two elements, with disaggregated dataframe row values
    """
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
    Runs through dataframe and disaggregates attribute age data into provided groups by given splitter value.

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
    for row in alter_kurz.itertuples():
        splitter = rnd.uniform(dis_low,
                               dis_high)  # runs for every row to add certain level of noise to group allocation
        val_tuple = split_val_by_share(row.Anzahl, splitter)
        res_list = reallocate_vals(row.Auspraegung_Code, val_tuple, row, attr_ident)
        group_one = res_list[0]
        group_two = res_list[1]
        result_list.append(group_one)
        result_list.append(group_two)
    result_df = pd.DataFrame(result_list, columns=cols)
    # TODO: test if loc is necessary here - i think it is an old loc as no group allocation works with -1 as identifier anymore
    return result_df.loc[result_df['Auspraegung_Code'] != -1]


def rem_by_mask(df: pd.DataFrame, mask: pd.Series, val=True):
    """
    Drops values provided by boolean mask.

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
    Based on a dict passed, the Auspraegung Code of an Attr is redefined.


    :param df: Dataframe to remap
    :param mapping: dictionary providing a mapping to regroup. All groups that exist in df
    should be specified here.
    :return: Dataframe with new attribute Codes based on mapping provided in mapping dict.
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
    :param col_to_group: Column(s) to group by.
    :param col_to_max: Column to find max from.
    :param cols: Column names to be returned.
    :return: Dataframe where max in a specific column has been found for each group.
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
    index_list = normalize_list(index_list)
    return index_list


def count_attr_in_gem(gem, col='Merkmal'):
    """
    Counts the number of occurences of an attribute in a group object.

    :param gem: Group Object.
    :return: Returns a df with a share for each attribute in the gemeinde. Index is reset.
    """
    return gem.value_counts(subset=[col], normalize=True).reset_index()


def sum_cell_index(cell, gem_vals, cols):
    cell, geo_point = purge_duplicates(cell, gem_vals, cols)
    index = cell['Attr Index'].sum()
    return geo_point, index


def purge_duplicates(cell, potential_dupl, cols, geom='geometry', subset=['Merkmal', 'Auspraegung_Code']):
    """
    Purges duplicates identified by df subset. Keeps first occurrence.
    Duplicates occur because of gemeinde fill values being added as additional values to calculate cell index.

    :param cell: group of cell values
    :param potential_dupl: fill values
    :param cols: Columns to append the gemeinde fill values by
    :return: cell group purged of logically duplicate values; geo_point used to fill the missing geometries left by the merge of the gemeinde fill values
    """

    geo_point = grab_any_valid_value(cell, geom)
    cell = cell.append(potential_dupl)[cols].reset_index().set_crs(crs='EPSG:3035')
    mask = cell.duplicated(subset=subset, keep='first')
    cell = rem_by_mask(cell, mask)
    return cell, geo_point


def grab_any_valid_value(df, col):
    """
    Drops na values in col and returns remaining value at position 0.

    :param df: Dataframe
    :param col: Column to grab value from
    :return: Value of unspecified dtype.
    """
    return df[col].dropna().values[0]


def inner_merge_df(sum_codes: pd.DataFrame, codes_count: pd.DataFrame,
                   on=['Merkmal', 'Auspraegung_Code']) -> pd.DataFrame:
    return sum_codes.merge(codes_count, on=on, how='inner')


def get_code_counts(group, subset=['Merkmal', 'Auspraegung_Code'], col_name='Counts'):
    """
    Counts occurrences of attribute - attribute code combinations. Creates new column in df with col_name.

    :param col_name: Name of result column to append to df.
    :param group: Group object.
    :return: A pandas group object with counts per [attributes and attribute code]. Index is reset.
    """
    return group.value_counts(subset=subset).reset_index().rename({0: col_name}, axis=1)


def calc_attr_median(attr_distro: pd.DataFrame) -> pd.DataFrame:
    """
    :param attr_distro: df containing sum of number of occurences and count of attribute - attribute code combinations
    :return: Df with Df[Calc Distro Attr/Cell] containing the median value for each attribute - attribute code combination in a group.
    """
    return attr_distro['Anzahl'].div(attr_distro['Counts'])


def infer_gem_vals(df_one: pd.DataFrame, df_two: pd.DataFrame, weight_map: dict) -> gpd.GeoDataFrame:
    """
    Merges two dataframes. Creates the product of two columns in the merged df. Multiplies the values in the new column with
    mapping values based on a condition column.


    :param df_one: Median value for each attribute-attribute-code combination in a gemeinde
    :param df_two: Share of attribute value in gemeinde occurences.
    :param weight_map: weight mapping as specified in config.json
    :return: Geodataframe with a weighted multiplication result of two merged dataframes.
    """
    # TODO: merge is not necessary for logic of program. Purge to run more efficiently
    gem_vals = df_one.merge(df_two, on='Merkmal', how='inner').rename({0: 'Ratio'}, axis=1)
    gem_vals['Gemeinde Fill Values'] = gem_vals['Calc Distro Attr/Cell'] * gem_vals['Ratio']
    gem_vals = mult_col_dict(gem_vals, weight_map, new_col='Attr Index', prdne='Gemeinde Fill Values',
                             prdtwo='Auspraegung_Code', cond='Merkmal')
    gem_vals.insert(8, "geometry", np.nan, True)
    return gpd.GeoDataFrame(gem_vals, geometry='geometry', crs='EPSG:3035')


def group_and_sum_code_counts(group, cols=['Merkmal', 'Auspraegung_Code'], col_count='Anzahl'):
    """
    Sums the number of occurrences of every attribute - attribute code combination.

    :param group: Group Object / Df
    :return: Group Object / Df with sums of groups.
    """

    return group.groupby(cols)[col_count].sum().reset_index()


def normalize_column(series: pd.Series, new_max=1, new_min=0) -> pd.Series:
    """
    Casts series data points between 0-1 based on min and max values in series.

    :param: date_to_norm: int or float value to normalize.
    :param: abs_min: Min val. in scope.
    :param: abs_max: Max val. in scope.
    :return: Series with normalized values between 0-1.
    """
    abs_max = series.max()
    abs_min = series.min()
    return series.apply(lambda date: ((date - abs_min) / (abs_max - abs_min)) * (new_max - new_min) + new_min)


def normalize_list(liste, new_max=1, new_min=0) -> list:
    """
    Casts series data points between 0-1 based on min and max values in series.

    :param: date_to_norm: int or float value to normalize.
    :param: abs_min: Min val. in scope.
    :param: abs_max: Max val. in scope.
    :return: Series with normalized values between 0-1.
    """
    indexes = []
    for index in liste:
        indexes.append(index[3])
    abs_max = max(indexes)
    abs_min = min(indexes)
    new_liste = []
    for elem in liste:
        elem[3] = ((elem[3] - abs_min) / (abs_max - abs_min)) * (new_max - new_min) + new_min
        new_liste.append(elem)
    return new_liste


def sum_gemeinde_idx(df_group: pd.DataFrame, sum_col='Haushalte_Index') -> dict:
    """
    Sums values of sum_col for every group object in passed df.

    :param df_group: grouped pd.Dataframe
    :return: Dictionary group name as key and sum of sum_col as value.
    """
    idx = {}
    for name, gem in df_group:
        gem_index = gem.sum(numeric_only=True)[sum_col]
        idx.update({name: gem_index})
    return idx


def calc_sum_zba(gem_idx_dict):
    """
    Casts dictionary to pd.Dataframe and sums the values. Index is reset.

    :param gem_idx_dict:
    :return: Dataframe containing the sum of all gemeinde indeces.
    """
    return pd.DataFrame.from_dict(gem_idx_dict, orient='index').reset_index().sum()[0]


def calc_gem_ratio(dict: dict, divisor: float) -> dict:
    """
    Calculates a ratio for each value in dict as val/divisor.

    :param dict: Dictionary of float values.
    :param divisor: Int or Float.
    :return: Dictionary with ratios for each original entry.
    """
    idx_keys = dict.keys()
    ratio_dict = {}
    for key in idx_keys:
        gem_idx = dict.get(key)
        ratio = gem_idx / divisor
        ratio_dict.update({key: ratio})
    return ratio_dict


def calc_num_ev_gem(ratios: dict, anzahl_evs_zb: int) -> dict:
    """
    Multiplies each dict entry with an int.

    :param ratios:
    :param anzahl_evs_zb:
    :return: Dictionary of multiplied value entries.
    """
    ev_dict = {}
    for key in ratios.keys():
        ev_dict.update({key: ratios.get(key) * anzahl_evs_zb})
    return ev_dict


def calc_cars_in_interest_area(gemeinde_ladestationen_poly, index_df, interest_area: str, aoi_type: str,
                               ars_dict: dict):
    gem_idx_dict = {}
    gem_groups = index_df.groupby(by='Gemeinde')

    for name, gem in gem_groups:
        gem_idx = gem['Gemeinde_Index'].iloc[0]
        gem_idx_dict.update({name: gem_idx})

    # TODO: Write into function
    factor = 1.0 / sum(gem_idx_dict.values())

    for k in gem_idx_dict:
        gem_idx_dict[k] = gem_idx_dict[k] * factor

    if aoi_type == 'Gemeinden':

        interest_area_ladestationen_poly = gemeinde_ladestationen_poly.loc[
            gemeinde_ladestationen_poly['NAME_Gemeinde'] == interest_area]

        anzahl_evs_aoi = interest_area_ladestationen_poly['EVIng'].iloc[0]
        aoi_ev_gem = calc_num_ev_gem(gem_idx_dict, anzahl_evs_aoi)

    elif aoi_type == 'Zulassungsbezirk':

        interest_area = interest_area.upper()
        interest_area = interest_area.replace('Ü', 'UE')
        interest_area = interest_area.replace('Ä', 'AE')
        interest_area = interest_area.replace('Ö', 'OE')

        interest_area_ladestationen_poly = gemeinde_ladestationen_poly.loc[
            gemeinde_ladestationen_poly['NAME_Zula'] == interest_area]

        anzahl_evs_aoi = interest_area_ladestationen_poly['EVIng'].iloc[0]
        aoi_ev_gem = calc_num_ev_gem(gem_idx_dict, anzahl_evs_aoi)

    elif aoi_type == 'Bundesland':

        interest_area = interest_area.upper()
        interest_area = interest_area.replace('Ü', 'UE')
        interest_area = interest_area.replace('Ä', 'AE')
        interest_area = interest_area.replace('Ö', 'OE')

        ars_key = ars_dict.get(interest_area)
        ars_key = int(ars_key)
        gemeinde_ladestationen_poly = gemeinde_ladestationen_poly.astype({'ARS': 'int32'})
        interest_area_ladestationen_poly = gemeinde_ladestationen_poly.loc[
            gemeinde_ladestationen_poly['ARS'] == ars_key]

        aoi_group = interest_area_ladestationen_poly.groupby(by='NAME_Zula')
        aoi_ev_gem = {}

        # TODO: Refactor

        for zula in aoi_group:

            ev_zula = zula[1]['EVIng'].iloc[0]
            zula_gemeinden = zula[1]['NAME_Gemeinde']
            zula_gem_idx_dict = {gem: gem_idx_dict.get(gem, None) for gem in zula_gemeinden}

            # TODO: Normalization in zula_gem_idx_dict
            factor = 1.0 / sum(filter(None, zula_gem_idx_dict.values()))

            for gem in zula_gem_idx_dict:
                gem_val = zula_gem_idx_dict.get(gem)
                if gem_val is None:
                    aoi_ev_gem.update({gem: 0})
                else:
                    gem_val = gem_val * factor * ev_zula
                    aoi_ev_gem.update({gem: gem_val})

    interest_area_ladestationen_poly.insert(10, 'EVGem',
                                            interest_area_ladestationen_poly['NAME_Gemeinde'].map(aoi_ev_gem))

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
    gemeinde_idx_dict = sum_gemeinde_idx(gem_group)
    sum_zba = calc_sum_zba(gemeinde_idx_dict)
    ratios = calc_gem_ratio(gemeinde_idx_dict, sum_zba)

    return ratios

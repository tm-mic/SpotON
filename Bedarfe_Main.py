# Written by Christian Larsen

import warnings

from bedarfe_funcs import import_raw, concat_df, del_df_columns, calc_df_ratio, calc_sum_ratio, calc_count_ratio, \
    weigh_attr_mean

warnings.simplefilter(action='ignore', category=FutureWarning)

import pandas as pd
from data_aggregation import join_geo_to_calc_mean as geo_mean
from data_aggregation import communal_mean as cm
from IandO import json_utility as ju
from data_aggregation.normalize_index_zero_one import normalize_column as nc

# [0] import and concat dataframes
df_import_list = ["Familie", "Gebaeude", "Haushalte", "Bevoelkerung"]
config_obj = ju.read_json("/Users/msccomputinginthehumanities/PycharmProjects/SpotON/IandO/config.json")
mapping_attr_grouping = ju.read_json_elements(config_obj, "Familie", "attr_mapping")
weigth_mapping = ju.read_json_elements(config_obj, "Familie", "weight_mapping")
path_to_gemeinde_shp = '/Users/msccomputinginthehumanities/Uni_Bamberg/02_Studium/03_WS22_23/Klnf-P Projekt/Datensätze/shapefiles/vg5000_01-01.utm32s.shape.ebenen/vg5000_ebenen_0101/VG5000_GEM.shp'

# [0.1] Group Data, aggregate, write ratios
imports = import_raw(df_import_list, config_obj, 500)
all_df = imports[0]
complete_raw = concat_df(all_df)
complete_ratios = calc_df_ratio(complete_raw, mapping_attr_grouping, weigth_mapping)
# [0.2] Validate Data


# [1] get all unique crs in all Gemeinden
# [1.1] Create polygon gdf to match crs with polygons
crs_in_gemeinde_polygons = geo_mean.get_crs_df_for_gemeinde(complete_raw, path_to_gemeinde_shp)

# [1.2] group crs by gemeinde Name -> Group objects with unique crs (value_counts())
grouped_gemeinde_crs = crs_in_gemeinde_polygons.loc[:,
                       ['NAME', 'Gitter_ID_100m_neu']].value_counts().reset_index().groupby('NAME')
# [1.3] get value dataframes from polygone gdf -> Group objects with gemeinde Name and all values
grouped_gemeinde_values = crs_in_gemeinde_polygons.loc[:,
                          ['NAME', 'Gitter_ID_100m_neu', 'Merkmal', 'Auspraegung_Code', 'Anzahl']].groupby('NAME')

# [2] all following operations will be done for the groups in unique_crs_in_gemeinde
# get unique keys from gemeinde Groups -> Name : str
gemeinde_keys = grouped_gemeinde_crs.groups.keys()

# result list to be appended
crs_tuple_list = []
gemeinde_tuple_list = []

# run through every gemeinde in the gemeinde groups to create gemeinde average values and to finally merge with complete ratios as calculated above
for gemeinde in gemeinde_keys:

    # Gemeinde Mean operations
    g_index = []
    gemeinde_crs = grouped_gemeinde_crs.get_group(gemeinde)
    gemeinde_values = grouped_gemeinde_values.get_group(gemeinde).reset_index()
    del_df_columns(gemeinde_values, 'index')

    # [3] calc mean and ratio for all in gemeinde
    mean_values = cm.group_and_mean(gemeinde_values)

    # [3.1] calculate val sum ratio in gemeinde
    sum_ratio = calc_sum_ratio(mean_values)

    # [3.2] calculate count ratio in gemeinde
    count_ratio = calc_count_ratio(gemeinde_values)

    # [3.3] weight the mean of each attr_val sum ratio by its occurence in the gemeinde
    gemeinde_mean = weigh_attr_mean(sum_ratio, count_ratio)

    # [4] group the gemeinde_mean by the mapping provided
    gemeinde_mean.rename(columns={'NAME': 'Gitter_ID_100m_neu'}, inplace=True)
    gemeinde_ratio = calc_df_ratio(gemeinde_mean, mapping_attr_grouping, weigth_mapping)
    gemeinde_ratio.rename(columns={'Gitter_ID_100m_neu': 'NAME'}, inplace=True)

    # Filter operations
    # [5] merge all values in complete df with the crs of the gemeinde. Each value row has a gemeinde Name and becomes groupable by gemeinde.
    values_gemeinde = complete_ratios.merge(gemeinde_crs, on='Gitter_ID_100m_neu',
                                            how='inner')

    # [6] fill each crs group with missing gemeinde values

    # [6.1] aggregate values of gemeinde by crs, because index is calculated on CRS level.
    crs_value_groups = values_gemeinde.groupby('Gitter_ID_100m_neu')
    keys = crs_value_groups.groups.keys()

    # Index Calc operations on crs level.
    crs_counter = 0
    index_summe = 0
    for i in keys:
        crs_counter += 1
        crs_rows = crs_value_groups.get_group(i)
        index_values = crs_rows.merge(gemeinde_ratio, on=['Merkmal', 'Auspraegung_Code'], how="outer")
        # fill all ratios into one column to calculate index over column
        index_values['Weighted_Ratio_x'] = index_values.Weighted_Ratio_x.fillna(index_values.Weighted_Ratio_y)
        cell_index = index_values['Weighted_Ratio_x'].sum()
        index_summe += cell_index
        crs_index_tuple = (i, cell_index)
        crs_tuple_list.append(crs_index_tuple)

    gemeinde_tuple_list.append((gemeinde, index_summe))

gemeinde_df = pd.DataFrame(gemeinde_tuple_list, columns=['Gemeinde', 'Index'])
index_df = pd.DataFrame(crs_tuple_list, columns=['Gitter_ID_100m_neu', 'Index'])
gemeinde_df['Normalized_Index'] = nc(gemeinde_df['Index'])
num_households = imports[1]

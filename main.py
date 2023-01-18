import warnings
import timeit
import geopandas as gpd
import pandas as pd
import IandO.json_utility as ju
import bedarfe as bed
import import_funcs as ss
from geometry_operations.coord_to_polygon import plot_geodataframe
import pd_display_options

warnings.simplefilter(action='ignore', category=FutureWarning)
# TODO: provide bl by terminal input - use input() in a function checking against a dict specified by the
# elements in the gem_shapefile
interest_area = 'Oberallgäu'

# import all nec. config infos
config_obj = ju.read_json("/Users/msccomputinginthehumanities/PycharmProjects/SpotON/IandO/config.json")
zensus_conversion = ju.read_json_elements(config_obj, "data_conversion", "Zensus_data")
data_conversion = ju.read_json_elements(config_obj, "data_conversion", "demographic_data")
area_shapefile = ju.read_json_elements(config_obj, "shapefile", "Landkreis")
gem_shapefile = ju.read_json_elements(config_obj, "shapefile", "Gemeinden")
zensus_file = ju.read_json_elements(config_obj, 'Zensus', "filepath")
imp_cols = ju.read_json_elements(config_obj, 'Zensus', "columns")
cols_keep = ju.read_json_elements(config_obj, 'Zensus', "cols_keep")
data_df = pd.DataFrame
bl_polygon = ss.obtain_bl_polygon(area_shapefile, interest_area)
bev_data = ju.read_json_elements(config_obj, 'Bevoelkerung', "filepath")
geb_data = ju.read_json_elements(config_obj, 'Gebaeude', "filepath")
hau_data = ju.read_json_elements(config_obj, 'Haushalte', "filepath")
fam_data = ju.read_json_elements(config_obj, 'Familie', "filepath")
data_list = [bev_data, geb_data, fam_data]
data_columns = ju.read_json_elements(config_obj, 'data', "columns")


point_ref_csv = ju.read_json_elements(config_obj, 'results', 'point_ref')
data_csv = ju.read_json_elements(config_obj, 'results', 'data')
index_csv = ju.read_json_elements(config_obj, 'results', 'cell_index')


chunks = 1000000
attr_mapping = ju .read_json_elements(config_obj, 'attr_mapping')
weight_map = ju.read_json_elements(config_obj, 'weight_mapping')
index_columns = ["Gitter_ID_100m", "Merkmal", "Auspraegung_Code", 'Bundesland', 'GEN', 'Attr Index', 'geometry']
poly_extrema = ss.calc_poly_extrema(bl_polygon)
bl_df = pd.DataFrame()

print("The program has started. Depending on the size of the area of interest or its geographical whereabouts the program might take a while to complete.")
# import zensus file information to create shapely points
start = timeit.default_timer()
lat_lon_df = ss.data_poly_import(zensus_file, imp_cols, poly_extrema, chunks, zensus_conversion)
stop = timeit.default_timer()
print(f"Importing the Zensus data took {stop-start}")


# create shapely points from zensus data files
start = timeit.default_timer()
area_point_reference = ss.points_in_bundesland(interest_area, area_shapefile, lat_lon_df, gem_shapefile).loc[:, cols_keep]
area_point_reference = area_point_reference.loc[area_point_reference['Bundesland'] == interest_area]
stop = timeit.default_timer()
print(f"Computing a geoDataframe from the Zensus data took {stop-start}")

ju.write_df_to_csv(area_point_reference, point_ref_csv, interest_area, sep=',')
# only import data that falls into given Bundesland
start = timeit.default_timer()

# TODO: Error Handling, f.e file not found error needs to be catched
for data_set in data_list:
    bl_df = bl_df.append(ss.data_poly_import(data_set, data_columns, poly_extrema, chunks, data_conversion))

ju.write_df_to_csv(bl_df, data_csv, interest_area, sep=',')

hau_df = ss.data_poly_import(hau_data, data_columns, poly_extrema, chunks, data_conversion)
hau_df = hau_df.loc[:, ['Gitter_ID_100m', 'Anzahl']].loc[hau_df['Merkmal'] == 'INSGESAMT']

bl_df['Anzahl'] = pd.to_numeric(bl_df['Anzahl'], errors='coerce', downcast='integer')
stop = timeit.default_timer()
print(f"Data for {bev_data} in {interest_area} has been imported. This operation took {stop - start}s")


# merge zensus points and data inside the bounding box
start = timeit.default_timer()
bl_df = gpd.GeoDataFrame(bl_df.merge(area_point_reference, how='left', on='Gitter_ID_100m').dropna(how='any'))
bl_df = bl_df.loc[:, ['Gitter_ID_100m', 'Merkmal', 'Auspraegung_Code', 'Anzahl', 'Bundesland', 'GEN', 'geometry']]
stop = timeit.default_timer()
print(f"The imported data for the Bundesland {interest_area} and the Zensus data have been merged. This operation took {stop - start}s")

# disaggregate age
# TODO: find improvement in runtime. This section takes very long -> disaggregate_age_atr still works with itertuples
# possible solutions: run operations on slice of bls_df to reduce size,
start = timeit.default_timer()
age_disaggr = bed.disaggregate_age_attr(bl_df)
bl_df = bl_df.append(age_disaggr)
age_mask = bl_df.duplicated(subset=['Gitter_ID_100m', 'Merkmal', 'Auspraegung_Code'], keep='first')
bl_df = bed.rem_by_mask(bl_df, age_mask)
bl_df = bl_df.loc[bl_df['Merkmal'] != 'ALTER_KURZ']
stop = timeit.default_timer()
print(f'Age groups have been disaggregated. This operation took {stop-start}s')
print((bl_df['GEN']==interest_area).any())

# remap codes based on mapping dict
bl_df = bed.remap_groups(bl_df, attr_mapping)
# calc the percentwise ratio for each merkmal ausprägung
start = timeit.default_timer()
cell_max = bed.calc_group_max(bl_df)
bl_df = bl_df.merge(cell_max, on="Gitter_ID_100m", how='inner')
bl_df['% of tot Obs'] = bl_df[['Anzahl']].div(bl_df['TotalObservations'], axis=0)
stop = timeit.default_timer()
print(f'% of Attr Code counts to total observations has been calculated. This operation took {stop-start}s')
print((bl_df['GEN']==interest_area).any())
# multiply % of tot Obs with weight Mapping
start = timeit.default_timer()
bl_df = bed.mult_col_dict(bl_df, weight_map, new_col='Attr Index', prdne='% of tot Obs', prdtwo='Auspraegung_Code', cond='Merkmal')
stop = timeit.default_timer()
print(f'The weight of the Attr Code has been calculated. This operation took {stop-start}s')

# calculate gemeinde fill values
gemeinden = bl_df.groupby('GEN')
index_list = bed.calc_cell_index(gemeinden, weight_map, index_columns, interest_area)
index_df = gpd.GeoDataFrame(pd.DataFrame(index_list, columns=['Gitter_ID_100m', 'Bundesland', 'Gemeinde', 'Cell Index', 'geometry']), geometry='geometry')
index_df = index_df.merge(hau_df, on='Gitter_ID_100m', how='left').dropna(how='any')
index_df = index_df.loc[:, ['Gitter_ID_100m', 'Bundesland', 'Gemeinde', 'Cell Index', 'Anzahl', 'geometry']]
print(bl_df['Gemeinde'].any())
# normalize df between 0-1
index_df['Cell Index'] = bed.normalize_column(index_df['Cell Index'])

ju.write_df_to_csv(index_df, index_csv, interest_area, sep=',')
plot_geodataframe(index_df, 'Cell Index')
print("The Cell indeces have been calculated. Next the amount of cars for each Gemeinde based on the cell indeces will be calculated.")

anzahl_evs_zb = 1000
index_df['Haushalte_Index'] = (index_df['Anzahl']*index_df['Cell Index'])
gem_group = index_df.groupby(by='Gemeinde')
gem_idx_dict = bed.calc_gemeinde_index(gem_group)

gemeinde_idx_dict = bed.calc_gemeinde_index(gem_group)
sum_zba = bed.calc_sum_zba(gemeinde_idx_dict)
idx_keys = gemeinde_idx_dict.keys()

ratios = bed.calc_gem_ratio(gemeinde_idx_dict, sum_zba)

print(bed.calc_num_ev_gem(ratios, anzahl_evs_zb))
# TODO: gemeinde shows up in lk and gemeinde


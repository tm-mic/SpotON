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
interest_area = 'Bamberg'

# import all nec. config infos
config_obj = ju.read_json("/Users/msccomputinginthehumanities/PycharmProjects/SpotON/IandO/config.json")
zensus_conversion = ju.read_json_elements(config_obj, "data_conversion", "Zensus_data")
demo_data_conversion = ju.read_json_elements(config_obj, "data_conversion", "demographic_data")
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
data_columns = ju.read_json_elements(config_obj, 'data', "columns")
chunks = 1000000
attr_mapping = ju.read_json_elements(config_obj, 'attr_mapping')
weight_map = ju.read_json_elements(config_obj, 'weight_mapping')
index_columns = ["Gitter_ID_100m", "Merkmal", "Auspraegung_Code", 'Bundesland', 'GEN', 'Attr Index', 'geometry']

# import zensus file information to create shapely points
# TODO: implement 2 options
#  (1) use same operation as for data import. shortening the time necessary to import and especially to create geodataframe -> used when generated at runtime
#  (2) use when writing to file : create csv with all data mapped in it.
start = timeit.default_timer()
lat_lon_df = ss.import_csv_chunks(zensus_file, imp_cols, type_conversion_dict=zensus_conversion, nrows=20000000)
stop = timeit.default_timer()
print(f"Importing the Zensus data took {stop-start}")

# create shapely points from zensus data files
start = timeit.default_timer()
area_point_reference = ss.points_in_bundesland(interest_area, area_shapefile, lat_lon_df, gem_shapefile).loc[:, cols_keep]
area_point_reference = area_point_reference.loc[area_point_reference['Bundesland'] == interest_area]
stop = timeit.default_timer()
print(f"Computing a geoDataframe from the Zensus data took {stop-start}")

# only import data that falls into given Bundesland
poly_extrema = ss.calc_poly_extrema(bl_polygon)
bl_df = pd.DataFrame()
start = timeit.default_timer()
c = 0

# TODO: create and concat all demographic data such that imported_bev + imported_fam ... can be used in the for data in imported_data loop || etract to function & loop through different files and concat after import
# TODO: Error Handling, some of the data are sep. with ',' not with ';' f.e fam_data has a , sep and bev data has a ; sep -> use import function already written which handles these errors
# TODO: Import Haushalte, but do not concat to final df but rather handle as seperate df. then easily merge in at the end with final dataframe
while True:
    imported_data = pd.read_csv(bev_data, usecols=data_columns, engine='python', encoding_errors='replace', on_bad_lines="skip", chunksize=chunks, sep=';', dtype=zensus_conversion)
    for data in imported_data:
        north_min_mask = ss.calc_coord_mask(poly_extrema[1], data, xoy='y')
        north_max_mask = ss.calc_coord_mask(poly_extrema[3], data, xoy='y')

        if ss.all_smaller(north_min_mask):
            print("None of imported data in range of Polygon.")
            continue
        if ss.all_bigger(north_max_mask):
            print("This part of the dataset is north of the given Polygon. Data import is stopped.")
            break
        in_poly = ss.data_in_poly(poly_extrema, data)
        if in_poly[0]:
            bl_df = bl_df.append(in_poly[1])
            c += 1
            print(bl_df.head(5), f'\n{c} chunks have been added to the dataframe so far. \nThe df has a size of {bl_df.shape}')
        else:
            print("Data is in range of y_max-y_min, but not in x_max-x_min. Import continues.")
            continue
    break
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

# remap codes based on mapping dict
bl_df = bed.remap_groups(bl_df, attr_mapping)

# calc the percentwise ratio for each merkmal ausprägung
start = timeit.default_timer()
cell_max = bed.calc_group_max(bl_df)
bl_df = bl_df.merge(cell_max, on="Gitter_ID_100m", how='inner')
bl_df['% of tot Obs'] = bl_df[['Anzahl']].div(bl_df['TotalObservations'], axis=0)
stop = timeit.default_timer()
print(f'% of Attr Code counts to total observations has been calculated. This operation took {stop-start}s')

# multiply % of tot Obs with weight Mapping
start = timeit.default_timer()
bl_df = bed.mult_col_dict(bl_df, weight_map, new_col='Attr Index', prdne='% of tot Obs', prdtwo='Auspraegung_Code', cond='Merkmal')
stop = timeit.default_timer()
print(f'The weight of the Attr Code has been calculated. This operation took {stop-start}s')


# calculate gemeinde fill values
index_list = []
gemeinden = bl_df.groupby('GEN')
for name, gem in gemeinden:
    # TODO: etract  to function easily as all of these operations are done on same gem groups
    codes_counts = gem.value_counts(subset=['Merkmal', 'Auspraegung_Code']).reset_index().rename({0: "Counts"}, axis=1)
    sum_codes = gem.groupby(['Merkmal', 'Auspraegung_Code'])['Anzahl'].sum().reset_index()
    attr_distro = sum_codes.merge(codes_counts, on=['Merkmal', 'Auspraegung_Code'], how='inner')
    attr_distro['Calc Distro Attr/Cell'] = attr_distro['Anzahl'].div(attr_distro['Counts'])
    attr_ratio = gem.value_counts(subset=['Merkmal'], normalize=True).reset_index()
    gem_vals = attr_distro.merge(attr_ratio, on='Merkmal', how='inner').rename({0: 'Ratio'}, axis=1)
    gem_vals['Gemeinde Fill Values'] = gem_vals['Calc Distro Attr/Cell']*gem_vals['Ratio']
    gem_vals = bed.mult_col_dict(gem_vals, weight_map, new_col='Attr Index', prdne='Gemeinde Fill Values', prdtwo='Auspraegung_Code', cond='Merkmal')

    cell_gem_group = gem.groupby('Gitter_ID_100m')
    for id, cell in cell_gem_group:
        # TODO: etract easily to function as all of these operations are done on same cell groups
        geo_point = cell['geometry'].dropna().values[0]
        cell = cell.append(gem_vals)[index_columns].reset_index()
        mask = cell.duplicated(subset=['Merkmal', 'Auspraegung_Code'], keep='first')
        cell = bed.rem_by_mask(cell, mask)
        index = cell['Attr Index'].sum()
        index_list.append([id, interest_area, name, index, geo_point])
index_df = gpd.GeoDataFrame(pd.DataFrame(index_list, columns=['Gitter_ID_100m', 'Bundesland', 'Gemeinde', 'Cell Index', 'geometry']), geometry='geometry')

# TODO: normalize Index with normalize function
print(index_df)
plot_geodataframe(index_df)
print("done")
# imported_data = pd.read_csv(fam_data, engine='python', encoding_errors='replace', on_bad_lines="skip", nrows=10000, sep=';')
# data_in_extrema = ss.imp_only_bl_data(poly_extrema, imported_data)
print('commit')

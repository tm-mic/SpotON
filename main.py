from pyarrow import csv
import warnings
import timeit
import geopandas as gpd
import pandas as pd
import IandO.json_utility as ju
import bedarfe as bed
import import_funcs
import import_funcs as ss
from geometry_operations.coord_to_polygon import plot_geodataframe
from IandO import user_input as ui
import geometry_operations.coord_to_polygon as cop
from normalize_index_zero_one import normalize_column as normalize
from IandO import file_exists as fe
import pyarrow as pa
import pd_display_options
from matplotlib import pyplot as plt
from geometry_operations import ladestation_in_zulassungsbezirk as liz
from geometry_operations.coord_to_polygon import load_gemeinde_polygon_to_gdf as lgp
warnings.simplefilter(action='ignore', category=FutureWarning)
from IandO.user_input import ui_aoi as ui
# elements in the gem_shapefile
# import all nec. config infos
config_obj = ju.read_json("/Users/msccomputinginthehumanities/PycharmProjects/SpotON/IandO/config.json")
shp_config = config_obj.get('shapefile')
point_ref_path = ju.read_json_elements(config_obj, 'results', 'point_ref')
data_path = ju.read_json_elements(config_obj, 'results', 'data')
index_path = ju.read_json_elements(config_obj, 'results', 'cell_index')
zensus_conversion = ju.read_json_elements(config_obj, "data_conversion", "Zensus_data")
data_conversion = ju.read_json_elements(config_obj, "data_conversion", "demographic_data")
area_shapefile = ju.read_json_elements(config_obj, "shapefile", "Landkreis")
gem_shapefile = ju.read_json_elements(config_obj, "shapefile", "Gemeinden")
zensus_file = ju.read_json_elements(config_obj, 'Zensus', "filepath")
zensus_cols = ju.read_json_elements(config_obj, 'Zensus', "columns")
cols_keep = ju.read_json_elements(config_obj, 'Zensus', "cols_keep")
data_df = pd.DataFrame
bev_data = ju.read_json_elements(config_obj, 'Bevoelkerung', "filepath")
geb_data = ju.read_json_elements(config_obj, 'Gebaeude', "filepath")
hau_data = ju.read_json_elements(config_obj, 'Haushalte', "filepath")
fam_data = ju.read_json_elements(config_obj, 'Familie', "filepath")
data_list = [bev_data, geb_data, fam_data]
data_columns = ju.read_json_elements(config_obj, 'data', "columns")
zba_shp = ju.read_json_elements(config_obj, 'shapefile', 'Zulassungsbezirk')
ev_regs_path = ju.read_json_elements(config_obj, 'ev_charging_points', 'filepath')
point_ref_csv = ju.read_json_elements(config_obj, 'results', 'point_ref')
data_csv = ju.read_json_elements(config_obj, 'results', 'data')
index_csv = ju.read_json_elements(config_obj, 'results', 'cell_index')
gdf_csv = ju.read_json_elements(config_obj, 'results', 'gdf')
kfz_shapefile = ju.read_json_elements(config_obj, "shapefile", "Zulassungsbezirk")
kfz_data = ju.read_json_elements(config_obj, "kfz_data", "filepath")
ladestationen_data = ju.read_json_elements(config_obj, "ev_charging_points", "filepath")
attr_mapping = ju .read_json_elements(config_obj, 'attr_mapping')
weight_map = ju.read_json_elements(config_obj, 'weight_mapping')
index_columns = ["Gitter_ID_100m", "Merkmal", "Auspraegung_Code", 'AOI', 'GEN', 'Attr Index', 'geometry']


# ask user for aoi
u_input = ui(shp_config)
interest_area = u_input[0]
aoi_polygon = u_input[1]

# setup point ref path
folders = [point_ref_path, data_path, index_path, gdf_csv]
for folder in folders:
    fe.setup_folders(folder)

# check if necessary paths exists
file_paths = [zensus_file, bev_data, geb_data, hau_data, fam_data, kfz_data, ladestationen_data]
shp_keys = shp_config.keys()
shp_check = []
for shp in shp_keys:
    valid_shp = shp_config.get(shp)
    file_paths.append(valid_shp)

c = 0
for path in file_paths:
    fe.files_exists(path)

totaltimes = timeit.default_timer()
# check if point ref. for aoi already exists
if fe.path_exists(import_funcs.concat_filepath(point_ref_path, interest_area)) is False:
    print(
        "Zensus data is being imported and a geodataframe with reference points for the aoi is being calculated. This might take up to 5 min.")
    start = timeit.default_timer()
    lat_lon_df = ss.read_df(zensus_file, zensus_cols)
    stop = timeit.default_timer()
    print(f"Importing the Zensus data took {stop - start}")

    start = timeit.default_timer()
    area_point_reference = ss.points_in_bundesland(interest_area, aoi_polygon, lat_lon_df, gem_shapefile).loc[:, cols_keep]
    area_point_reference = area_point_reference.loc[area_point_reference['AOI'] == interest_area]
    stop = timeit.default_timer()
    print(f"Computing a geoDataframe from the Zensus data took {stop - start}")
    out = area_point_reference.to_parquet(ss.concat_filepath(point_ref_path, interest_area, ending='.parquet'))
    print(f"The point reference file has been written to your results folder {point_ref_csv}."
          f"For future runs in the same aoi this step will be skipped.")
    del out
    del lat_lon_df
else:
    print(f"\nThe point of reference file of {interest_area} does already exist in {point_ref_path}.\n"
          f"This file is being used for further calculations.")
    area_point_reference = gpd.read_parquet(ss.concat_filepath(point_ref_path, interest_area, ending='.parquet'))

# if all data does not exist yet write back as parquet
if fe.path_exists(ss.concat_filepath(data_path, 'all_data')) is False:
    read_opt = csv.ReadOptions(use_threads=True,
                               column_names=['Gitter_ID_100m', 'Gitter_ID_100m_neu', 'Merkmal', 'Auspraegung_Code',
                                             'Auspraegung_Text', 'Anzahl', 'Anzahl_q'])
    parse_options = csv.ParseOptions(delimiter=';')
    convert = csv.ConvertOptions(include_columns=data_columns)
    start = timeit.default_timer()
    bev = csv.read_csv(bev_data, read_opt, parse_options, convert)
    parse_options = csv.ParseOptions(delimiter=',', invalid_row_handler=import_funcs.invalid_row_handler)
    fam = csv.read_csv(fam_data, read_opt, parse_options, convert)
    geb = csv.read_csv(geb_data, read_opt, parse_options, convert)
    hau = csv.read_csv(hau_data, read_opt, parse_options, convert)
    stop = timeit.default_timer()
    print(f"Importing the data took : {stop - start}")
    data = pa.concat_tables([bev, fam, geb])
    pa.parquet.write_table(data, ss.concat_filepath(data_path, 'all_data', ending='.parquet'))
    pa.parquet.write_table(hau, ss.concat_filepath(data_path, 'all_data', ident='_haushalte', ending='.parquet'))
    print(f"The data has been written to {data_path}.")
else:
    print(f"The data is already in the {data_path} folder. No new data has been imported.")


# merge zensus points and data
if fe.path_exists(ss.concat_filepath(data_path, interest_area)) is False:
    start = timeit.default_timer()
    print("Reading data into df. This might take a minute.")
    aoi_df = pd.read_parquet(import_funcs.concat_filepath(data_path, "all_data", ending='.parquet')).convert_dtypes()
    print(aoi_df.head())
    stop = timeit.default_timer()
    print(f"The data import took {stop-start}.")
    start = timeit.default_timer()
    aoi_df = gpd.GeoDataFrame(aoi_df.merge(area_point_reference, how='left', on='Gitter_ID_100m').dropna(how='any'))
    aoi_df = aoi_df.loc[:, ['Gitter_ID_100m', 'Merkmal', 'Auspraegung_Code', 'Anzahl', 'AOI', 'GEN', 'geometry']]
    stop = timeit.default_timer()
    print(f"Merging the data took {stop-start}.")
    aoi_df.to_parquet(ss.concat_filepath(data_path, interest_area))
else:
    print(f"The data for this aoi is already in your filesystem at {data_path}. This data is used for further calculations.")
    aoi_df = gpd.read_parquet(ss.concat_filepath(data_path, interest_area))

#create index from file
if fe.path_exists(ss.concat_filepath(index_path, interest_area)) is False:
    # import aoi data
    aoi_df["Auspraegung_Code"] = aoi_df["Auspraegung_Code"].astype('int8')
    aoi_df["Anzahl"] = aoi_df["Anzahl"].astype('int8')
    # disaggregate age
    age_disaggr = bed.disaggregate_age_attr(aoi_df)
    aoi_df.append(age_disaggr)
    age_mask = aoi_df.duplicated(subset=['Gitter_ID_100m', 'Merkmal', 'Auspraegung_Code'], keep='first')
    aoi_df = bed.rem_by_mask(aoi_df, age_mask)
    aoi_df = aoi_df.loc[aoi_df['Merkmal'] != 'ALTER_KURZ']
    print(f'Age groups have been disaggregated.')
    # TODO: user warning to use loc instead of df[] notation -> somewhere in subroutine. Shoud
    # remap codes based on mapping dict
    aoi_df = bed.remap_groups(aoi_df, attr_mapping)

    # calc the percentwise ratio for each merkmal ausprägung
    cell_max = bed.calc_group_max(aoi_df)
    aoi_df = aoi_df.merge(cell_max, on="Gitter_ID_100m", how='inner')
    aoi_df['% of tot Obs'] = aoi_df[['Anzahl']].div(aoi_df['TotalObservations'], axis=0)
    print(f'% of Attr Code counts to total observations has been calculated.')

    # multiply % of tot Obs with weight Mapping
    aoi_df = bed.mult_col_dict(aoi_df, weight_map, new_col='Attr Index', prdne='% of tot Obs', prdtwo='Auspraegung_Code',
                               cond='Merkmal')
    print(f'The weight of the Attr Code has been calculated.')

    # calculate gemeinde fill values
    gemeinden = aoi_df.groupby('GEN')
    index_list = bed.calc_cell_index(gemeinden, weight_map, index_columns, interest_area)
    index_df = gpd.GeoDataFrame(
        pd.DataFrame(index_list, columns=['Gitter_ID_100m', 'AOI', 'Gemeinde', 'Cell Index', 'geometry']),
        geometry='geometry')
    print("The filler indices for each gemeinde in your aoi have been calculated.")
    # import haushalte
    hau_df = pd.read_parquet(ss.concat_filepath(data_path, 'all_data', ident='_haushalte', ending='.parquet'))
    # multiply haushalte with index
    index_df = index_df.merge(hau_df, on='Gitter_ID_100m', how='left').dropna(how='any')
    index_df = index_df.loc[:, ['Gitter_ID_100m', 'AOI', 'Gemeinde', 'Cell Index', 'Anzahl', 'geometry']]
    print("The index for each cell in the aoi has been calculated.")
    # normalize df between 0-1
    index_df['Cell Index'] = bed.normalize_column(index_df['Cell Index'])
    index_df = bed.add_haushalte_index(index_df)
    ratios = bed.calc_zula_ratio(index_df)
    index_df = bed.add_gemeinde_index(index_df, ratios)
    print("The index has been normalized.")
    # write indices back to
    index_df.to_parquet(ss.concat_filepath(index_path, interest_area))
else:
    index_df = gpd.read_parquet(ss.concat_filepath(index_path, interest_area))

print(index_df.head())
index_df.plot(column="Cell Index")
plt.show()

totaltimestop = timeit.default_timer()
print(totaltimestop-totaltimes)
print("The Cell indeces have been calculated. Next the amount of cars for each Gemeinde based on the cell indeces will be calculated.")

# if fe.path_exists(ss.concat_filepath(index_path, interest_area)) is False:
#     # TODO: set into write back loop
#     # THOMAS part
#     kfz_data = ju.read_json_elements(config_obj, "kfz_data", "filepath")
#     kfz_data = ss.import_vehicle_registration_by_excel(kfz_data)
#     kfz_shapefile = liz.str_replace_of_name_in_base_polygon_gdf(kfz_shapefile)
#     kfz_shapefile = liz.delete_doubled_zulassungsbezirke(kfz_shapefile)
#     kfz_shapefile = liz.rename_names_of_some_cities(kfz_shapefile)
#
#     kfz_data_in_shapefile = liz.cars_with_zulassungsbezirk_polygon_gdf(kfz_data, kfz_shapefile)
#
#     gemeinden_polygon_gdf = lgp(gem_shapefile)
#     ladestationen_gdf = lgp(ladestationen_data)
#     # TODO: Get rid of "None" column in ladestationen_gdf
#
#     ladesaeulen_in_gemeinde_gdf = oels_in_gemeinde(gemeinden_polygon_gdf, ladestationen_gdf)
#
#     ladesaeulen_in_gemeinde_gdf = add_lp_to_gdf_gemeinde_oels(ladestationen_gdf, gemeinden_polygon_gdf, ladesaeulen_in_gemeinde_gdf)
#     # TODO: Refactoring
#
#     oels_gemeinde_in_zula = oels_of_gemeinde_in_zula(ladesaeulen_in_gemeinde_gdf, kfz_data_in_shapefile)
#
#     missinggemeinde = lost_gemeinden_gdf(ladesaeulen_in_gemeinde_gdf)
#
#     oels_gemeinde_in_zula = add_remaining_gemeinden(kfz_data_in_shapefile, missinggemeinde, oels_gemeinde_in_zula)
#
#     gemeinde_ladestationen_poly = set_geometry_to_gemeinde_poly(gemeinden_polygon_gdf, oels_gemeinde_in_zula)
#
#     gemeinde_ladestationen_poly = car_oels_gemeinde_zula_gdf(kfz_data_in_shapefile, gemeinde_ladestationen_poly)
#
#     interest_area_ladestationen_poly = bed.calc_cars_in_interest_area(gemeinde_ladestationen_poly, index_df, "OBERALLGAEU")
#     # TODO: Implement consistent handling of lowercase and uppercase interest area str
#
#     interest_area_ladestationen_poly.to_parquet(ss.concat_filepath(gdf_csv, interest_area))
#
#     print("The amount of EV for each Gemeinde in the interest area has been calculated.")
# else:
#     cars_in_aoi = gpd.read_parquet(ss.concat_filepath(gdf_csv, interest_area))
#
# print(cars_in_aoi)

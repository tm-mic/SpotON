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

warnings.simplefilter(action='ignore', category=FutureWarning)
import os
from IandO.user_input import ui_aoi as ui

print("The program has started. Depending on the size of the area of interest "
      "or its geographical whereabouts the program might take a while to complete.")

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
zba_shp = ju.read_json_elements(config_obj, 'Zulassungsbezirk', 'Shapefile')
ev_regs_path = ju.read_json_elements(config_obj, 'ev_charging_points', 'filepath')
path_ev_zba = ju.read_json_elements(config_obj, 'Zulassungsbezirk',
                                    'Anzahl_BEV')  # TODO: Thomas please change this variable to absolute number of cars in zba

# get absolute number of cars in zba
point_ref_csv = ju.read_json_elements(config_obj, 'results', 'point_ref')
data_csv = ju.read_json_elements(config_obj, 'results', 'data')
index_csv = ju.read_json_elements(config_obj, 'results', 'cell_index')
chunks = 500000
attr_mapping = ju.read_json_elements(config_obj, 'attr_mapping')
weight_map = ju.read_json_elements(config_obj, 'weight_mapping')
index_columns = ["Gitter_ID_100m", "Merkmal", "Auspraegung_Code", 'AOI', 'GEN', 'Attr Index', 'geometry']

# ask user for aoi
u_input = ui(shp_config)
interest_area = u_input[0]
aoi_polygon = u_input[1]

# setup point ref path
fe.setup_folders(point_ref_path)
fe.setup_folders(data_path)
fe.setup_folders(index_path)

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

# read data into dataframe
if fe.path_exists(ss.concat_filepath(data_path, interest_area)) is False:
    start = timeit.default_timer()
    print("Reading data into df. This might take a minute.")
    aoi_df = pd.read_parquet(import_funcs.concat_filepath(data_path, "all_data", ending='.parquet')).convert_dtypes()
    print(aoi_df.head())
    stop = timeit.default_timer()
    print(f"The data import took {stop-start}.")

    # merge zensus points and data
    start = timeit.default_timer()
    aoi_df = gpd.GeoDataFrame(aoi_df.merge(area_point_reference, how='left', on='Gitter_ID_100m').dropna(how='any'))
    aoi_df = aoi_df.loc[:, ['Gitter_ID_100m', 'Merkmal', 'Auspraegung_Code', 'Anzahl', 'AOI', 'GEN', 'geometry']]
    stop = timeit.default_timer()
    print(f"Merging the data took {stop-start}.")
    aoi_df.to_parquet(ss.concat_filepath(data_path, interest_area))
else:
    print(f"The data for this aoi is already in your filesystem at {data_path}. This data is used for further calculations.")
    # write back aoi data


#create index from file
if fe.path_exists(ss.concat_filepath(index_path, interest_area)) is False:
    # import aoi data
    aoi_df = gpd.read_parquet(ss.concat_filepath(data_path, interest_area))
    aoi_df["Auspraegung_Code"] = aoi_df["Auspraegung_Code"].astype('int8')
    aoi_df["Anzahl"] = aoi_df["Anzahl"].astype('int8')
    # disaggregate age
    start = timeit.default_timer()
    age_disaggr = bed.disaggregate_age_attr(aoi_df)
    aoi_df.append(age_disaggr)
    age_mask = aoi_df.duplicated(subset=['Gitter_ID_100m', 'Merkmal', 'Auspraegung_Code'], keep='first')
    aoi_df = bed.rem_by_mask(aoi_df, age_mask)
    aoi_df = aoi_df.loc[aoi_df['Merkmal'] != 'ALTER_KURZ']
    stop = timeit.default_timer()
    print(f'Age groups have been disaggregated. This operation took {stop - start}s')

    # remap codes based on mapping dict
    aoi_df = bed.remap_groups(aoi_df, attr_mapping)

    # calc the percentwise ratio for each merkmal ausprägung
    start = timeit.default_timer()
    cell_max = bed.calc_group_max(aoi_df)
    aoi_df = aoi_df.merge(cell_max, on="Gitter_ID_100m", how='inner')
    aoi_df['% of tot Obs'] = aoi_df[['Anzahl']].div(aoi_df['TotalObservations'], axis=0)
    stop = timeit.default_timer()
    print(f'% of Attr Code counts to total observations has been calculated. This operation took {stop - start}s')

    # TODO: User Warning of CRS not being set - check and fix where bug occurs
    # multiply % of tot Obs with weight Mapping
    start = timeit.default_timer()
    aoi_df = bed.mult_col_dict(aoi_df, weight_map, new_col='Attr Index', prdne='% of tot Obs', prdtwo='Auspraegung_Code',
                               cond='Merkmal')
    stop = timeit.default_timer()
    print(f'The weight of the Attr Code has been calculated. This operation took {stop - start}s')

    # calculate gemeinde fill values
    gemeinden = aoi_df.groupby('GEN')
    index_list = bed.calc_cell_index(gemeinden, weight_map, index_columns, interest_area)
    index_df = gpd.GeoDataFrame(
        pd.DataFrame(index_list, columns=['Gitter_ID_100m', 'AOI', 'Gemeinde', 'Cell Index', 'geometry']),
        geometry='geometry')
    index_df.to_parquet(ss.concat_filepath(index_path, interest_area))
else:
    index_df = gpd.read_parquet(ss.concat_filepath(index_path, interest_area))


print(index_df)
index_df.plot(column="Cell Index")
plt.show()

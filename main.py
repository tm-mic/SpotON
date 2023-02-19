from pyarrow import csv
import warnings
import timeit
import geopandas as gpd
import pandas as pd
import IandO.json_utility as ju
import bedarfe as bed
import import_funcs as ifunc
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
from geometry_operations.coord_to_polygon import ladestationen_to_gdf as lsg
from geometry_operations import oels_in_gemeinde as og
from IandO.user_input import ui_aoi as ui
from geometry_operations import map_parking_areas as mpa
from geometry_operations.plot_the_spot import plot_folium_map_from_GeoDataFrames as pts

warnings.simplefilter(action='ignore', category=FutureWarning)

config_obj = ju.read_json("IandO/config.json")
shp_config = config_obj.get('shapefile')
point_ref_path = ju.read_json_elements(config_obj, 'results', 'point_ref')
html = ju.read_json_elements(config_obj, 'results', 'html')
data_path = ju.read_json_elements(config_obj, 'results', 'data')
index_path = ju.read_json_elements(config_obj, 'results', 'cell_index')
gdf_path = ju.read_json_elements(config_obj, 'results', 'gdf')
zensus_conversion = ju.read_json_elements(config_obj, "data_conversion", "Zensus_data")
data_conversion = ju.read_json_elements(config_obj, "data_conversion", "demographic_data")
gem_shapefile = ju.read_json_elements(config_obj, "shapefile", "Gemeinden")
zensus_file = ju.read_json_elements(config_obj, 'Zensus', "filepath")
zensus_cols = ju.read_json_elements(config_obj, 'Zensus', "columns")
cols_keep = ju.read_json_elements(config_obj, 'Zensus', "cols_keep")
data_df = pd.DataFrame
bev_data = ju.read_json_elements(config_obj, 'Bevoelkerung', "filepath")
geb_data = ju.read_json_elements(config_obj, 'Gebaeude', "filepath")
hau_data = ju.read_json_elements(config_obj, 'Haushalte', "filepath")
fam_data = ju.read_json_elements(config_obj, 'Familie', "filepath")
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
attr_mapping = ju.read_json_elements(config_obj, 'attr_mapping')
weight_map = ju.read_json_elements(config_obj, 'weight_mapping')
index_columns = ["Gitter_ID_100m", "Merkmal", "Auspraegung_Code", 'AOI', 'GEN', 'Attr Index', 'geometry']
ars_dict = config_obj.get('ars_dict')


def create_point_ref(zensus, cols, aoi, aoi_poly, gem_shp):
    """
            Creates a geodf as with geopoints, aoi and gem names for each cell.

            :param zensus: Path to Census datafile with all cells.
            :param cols: columns to read.
            :param aoi: area of interest as specified by the user
            :param aoi_poly: aoi polygon
            :param gem_shp: gemeinde shapefile to match gemeinden to aoi
            :return: Returns a geodataframe for the given aoi with all cells provided in census files the corresponding geopoints
            and the corresponding gemeinde name.
            """

    zensus = ifunc.read_df(zensus, cols)
    apr = ifunc.points_in_aoi(aoi, aoi_poly, zensus, gem_shp).loc[:, cols_keep]
    return apr.loc[apr['AOI'] == interest_area]


def slice_df_cols(df: pd.DataFrame, keep_cols: list):
    """
        Slices df by columns provided.

        :param df: DF to slice.
        :param keep_cols: Columns to keep.
        :return: Returns a sliced df with keep_cols.
        """
    return df.loc[:, keep_cols]


def change_col_type(df: pd.DataFrame, col: str, type: str):
    """
        Change the data type of dataframe column.

        :param df: Dataframe to change type.
        :param col: Column to change type.
        :param type: type to change to.
        :return: Returns a dataframe with the column changed to the new type.
        """
    return df[col].astype(type)


def calc_attr_max_ratios(df):
    """
        Calculates the ratio of attribute count and total number of observations in a cell.

        :param df: Dataframe containing the attribute count and number of total observations.
        :return: Dataframe with ratio each attribute in each cell
        """
    attr_max_cell = bed.calc_group_max(df)
    df = df.merge(attr_max_cell, on="Gitter_ID_100m", how='inner')
    return df[['Anzahl']].div(df['TotalObservations'], axis=0)


def calc_cell_index(df, weight, index_col, aoi):
    """
        Groups Gemeinden and creates geodataframe from index list containing cell indices.

        :param df: Data df
        :param weight: weight Mapping for attribute codes
        :param index_col: columns to keep in the index result
        :param aoi: area of interest
        :return: Returns a geodataframe containing the calculated cell Indices.
        """

    gemeinden = df.groupby('GEN')
    index_list = bed.calc_cell_index(gemeinden, weight, index_col, aoi)
    return gpd.GeoDataFrame(
        pd.DataFrame(index_list, columns=['Gitter_ID_100m', 'AOI', 'Gemeinde', 'Cell Index', 'geometry']),
        geometry='geometry')


def gem_index(df, haus_df):
    """
    Calculates the gemeinde indices based on cell indices and the number of households in a cell.

        :param df: df containing the cell indices
        :param haus_df: df containing the number of households
        :return:
        """

    df = df.merge(haus_df, on='Gitter_ID_100m', how='left').dropna(how='any')
    df = slice_df_cols(df, ['Gitter_ID_100m', 'AOI', 'Gemeinde', 'Cell Index', 'Anzahl', 'geometry'])
    # df['Cell Index'] = bed.normalize_column(df['Cell Index']) # unnecessary ass only gemeinde values need normalization
    df = bed.add_haushalte_index(df)
    zula_ratio = bed.calc_zula_ratio(df)
    g_index = bed.add_gemeinde_index(df, zula_ratio)
    return g_index


def read_data_from_parquet(file, delim=';',
                           cols=['Gitter_ID_100m', 'Gitter_ID_100m_neu', 'Merkmal', 'Auspraegung_Code',
                                 'Auspraegung_Text', 'Anzahl', 'Anzahl_q']):
    read_opt = csv.ReadOptions(use_threads=True, column_names=cols)
    parse_options = csv.ParseOptions(delimiter=delim, invalid_row_handler=ifunc.invalid_row_handler)
    convert = csv.ConvertOptions(include_columns=data_columns)
    try:
        csv_file = csv.read_csv(file, read_opt, parse_options, convert)
        return csv_file
    except FileNotFoundError:
        i = 0
        while i < 5:
            print(
                f"The .parquet file {file} you are trying to read could not be found. Please check your data folder for {file}."
                f"After adding the data file restart the program.")
            raise FileNotFoundError


def disaggr_age_df(df, distro_val=[0.4, 0.6]):
    """
        Disaggregates alter_kurz into alter_10jg attributes groups based on a low and high distro val.

        :param df: df containing age groups
        :param distro_val: range controlling the random value used to distribute age_kurz into age_10_jg groups. 0.5, 0.5 will yield a
        50/50 distribution where possible.
        :return: Dataframe containing the disaggregated age groups. Existing alter_10_jg age groups are retained.
        """
    age_disaggr = bed.disaggregate_age_attr(df, dis_low=distro_val[0], dis_high=distro_val[1])
    df.append(age_disaggr)
    age_mask = df.duplicated(subset=['Gitter_ID_100m', 'Merkmal', 'Auspraegung_Code'], keep='first')
    df = bed.rem_by_mask(df, age_mask)
    return df.loc[aoi_df['Merkmal'] != 'ALTER_KURZ']


def merge_to_gdf(df, to_merge, on='Gitter_ID_100m',
                 slice_cols=['Gitter_ID_100m', 'Merkmal', 'Auspraegung_Code', 'Anzahl', 'AOI', 'GEN', 'geometry']):
    df = gpd.GeoDataFrame(df.merge(to_merge, how='left', on=on).dropna(how='any'))
    return slice_df_cols(df,
                         slice_cols)


def shp_unique_names(shp):
    shp = liz.str_replace_of_name_in_base_polygon_gdf(shp)
    shp = liz.delete_doubled_zulassungsbezirke(shp)
    shp = liz.rename_names_of_some_cities(shp)
    return shp.to_file('data/KFZ250_new.shp', encoding='utf-8')


# overwrite zula shapefile to shp with unique names
if fe.path_exists('data/KFZ250_new.shp') is False:
    kfz_shapefile = shp_unique_names(kfz_shapefile)
    ju.write_json(config_obj, 'IandO/config.json', 'data/KFZ250_new.shp', 'shapefile', 'Zulassungsbezirk')
    kfz_shapefile = ju.read_json_elements(config_obj, "shapefile", "Zulassungsbezirk")
else:
    kfz_shapefile = ju.read_json_elements(config_obj, "shapefile", "Zulassungsbezirk")

if __name__ == "__main__":

    # ask user for aoi
    u_input = ui(shp_config)
    interest_area = u_input[0]
    aoi_polygon = u_input[1]
    aoi_type = u_input[2]

    # setup result folders
    folders = [point_ref_path, data_path, index_path, gdf_csv, html]
    for folder in folders:
        fe.setup_folders(folder)

    # check if files corresponding to config files paths exist in folder
    file_paths = [zensus_file, bev_data, geb_data, hau_data, fam_data, kfz_data, ladestationen_data]
    c = 0
    for path in file_paths:
        fe.files_exists(path)

    totaltimes = timeit.default_timer()
    # check if point ref. for aoi already exists
    if fe.path_exists(ifunc.concat_aoi_path(point_ref_path, interest_area, aoi_type)) is False:
        print("A geo-dataframe with reference points for the aoi is being calculated."
              "This might take a few minutes.")
        area_p_ref = create_point_ref(zensus_file, zensus_cols, interest_area, aoi_polygon, gem_shapefile)
        area_p_ref.to_parquet(ifunc.concat_aoi_path(point_ref_path, interest_area, aoi_type, ending='.parquet'))
        print(f"The point reference file has been written to your results folder {point_ref_csv}."
              f"For future runs in the same aoi this step will be skipped.")
    else:
        print(f"\nThe point of reference file of {interest_area} does already exist in {point_ref_path}.\n"
              f"This file is being used for further calculations.")
        area_p_ref = gpd.read_parquet(ifunc.concat_aoi_path(point_ref_path, interest_area, aoi_type, ending='.parquet'))

    # if all data does not exist yet write back as parquet
    if fe.path_exists(ifunc.concat_filepath(data_path, 'all_data')) is False:
        bev = read_data_from_parquet(bev_data, delim=';')
        fam = read_data_from_parquet(fam_data, delim=',')
        geb = read_data_from_parquet(geb_data, delim=',')
        data = pa.concat_tables([bev, fam, geb])
        pa.parquet.write_table(data, ifunc.concat_filepath(data_path, 'all_data', ending='.parquet'))

        # Haushalte data is read and written seperatly as it is merged to the dataframe at a later point in time.
        hau = read_data_from_parquet(hau_data, delim=',')
        pa.parquet.write_table(hau, ifunc.concat_filepath(data_path, 'all_data', ident='_haushalte', ending='.parquet'))
        print(f"The point reference data has been written to {data_path}.")
    else:
        print(f"All demographic data is already in the {data_path} folder. No new data has been imported.")

    # merge zensus points and data
    if fe.path_exists(ifunc.concat_aoi_path(data_path, interest_area, aoi_type)) is False:
        aoi_df = pd.read_parquet(ifunc.concat_filepath(data_path, "all_data", ending='.parquet')).convert_dtypes()
        aoi_df = merge_to_gdf(aoi_df, area_p_ref)
        print(f"Zensus data and point reference data have been merged successfully.")
        aoi_df.to_parquet(ifunc.concat_aoi_path(data_path, interest_area, aoi_type))
    else:
        aoi_df = gpd.read_parquet(ifunc.concat_aoi_path(data_path, interest_area, aoi_type))
        print(
            f"The data for this aoi is already in your filesystem at {data_path}. This data is used for further calculations.")

    # create index from file
    if fe.path_exists(ifunc.concat_aoi_path(index_path, interest_area, aoi_type)) is False:
        aoi_df["Auspraegung_Code"] = change_col_type(aoi_df, "Auspraegung_Code", 'int8')
        aoi_df["Anzahl"] = change_col_type(aoi_df, "Anzahl", 'int8')

        # disaggregate ageD
        aoi_df = disaggr_age_df(aoi_df, distro_val=[0.40, 0.60])
        print(f'Age groups have been disaggregated.')

        # TODO: user warning to use loc instead of df[] notation -> somewhere in subroutine.
        # remap codes based on mapping dict
        aoi_df = bed.remap_groups(aoi_df, attr_mapping)
        print(f'Groups have been remapped to match config specification.')

        # calc the percentwise ratio for each merkmal ausprägung
        aoi_df['Attr_to_total'] = calc_attr_max_ratios(aoi_df)
        print(f'Ratio of attribute code counts to total observations has been calculated.')

        # multiply Attr_to_total with weight Mapping
        aoi_df = bed.mult_col_dict(aoi_df, weight_map, new_col='Attr Index', prdne='Attr_to_total',
                                   prdtwo='Auspraegung_Code',
                                   cond='Merkmal')
        print(f'The weight of the attribute code has been calculated.')

        # calculate gemeinde fill values
        index_df = calc_cell_index(aoi_df, weight_map, index_columns, interest_area)
        print("The index for each cell in the aoi has been calculated.")

        # calc ratio based on haushalte and normalize
        hau_df = pd.read_parquet(ifunc.concat_filepath(data_path, 'all_data', ident='_haushalte', ending='.parquet'))
        index_df = gem_index(index_df, hau_df)
        print("The Gemeinde-index has been calculated and normalized.")

        index_df.to_parquet(ifunc.concat_aoi_path(index_path, interest_area, aoi_type))
    else:
        index_df = gpd.read_parquet(ifunc.concat_aoi_path(index_path, interest_area, aoi_type))

    print(
        "The Cell indeces have been calculated. Next the amount of cars for each Gemeinde based on the cell indeces will be calculated.")

    if fe.path_exists(ifunc.concat_aoi_path(gdf_path, interest_area, aoi_type)) is False:
        # TODO: set into write back loop
        # THOMAS part
        kfz_data = ifunc.import_vehicle_registration_by_excel(kfz_data)
        kfz_data = liz.renaming_some_zulassungsbezirke(kfz_data)
        kfz_shp_new = gpd.read_file(kfz_shapefile, encoding='utf-8')
        kfz_data_in_shapefile = liz.cars_with_zulassungsbezirk_polygon_gdf(kfz_data, kfz_shp_new)
        gemeinden_polygon_gdf = lgp(gem_shapefile)
        ladestationen_gdf = lsg(ladestationen_data)
        # TODO: Get rid of "None" column in ladestationen_gdf

        ladesaeulen_in_gemeinde_gdf = og.oels_in_gemeinde(gemeinden_polygon_gdf, ladestationen_gdf)

        ladesaeulen_in_gemeinde_gdf = og.add_lp_to_gdf_gemeinde_oels(ladestationen_gdf, gemeinden_polygon_gdf,
                                                                     ladesaeulen_in_gemeinde_gdf)
        # TODO: Refactoring

        oels_gemeinde_in_zula = og.oels_of_gemeinde_in_zula(ladesaeulen_in_gemeinde_gdf, kfz_data_in_shapefile)

        missinggemeinde = og.lost_gemeinden_gdf(ladesaeulen_in_gemeinde_gdf)

        oels_gemeinde_in_zula = og.add_remaining_gemeinden(kfz_data_in_shapefile, missinggemeinde,
                                                           oels_gemeinde_in_zula)

        gemeinde_ladestationen_poly = og.set_geometry_to_gemeinde_poly(gemeinden_polygon_gdf, oels_gemeinde_in_zula)

        gemeinde_ladestationen_poly = og.car_oels_gemeinde_zula_gdf(kfz_data_in_shapefile, gemeinde_ladestationen_poly)

        interest_area_ladestationen_poly = bed.calc_cars_in_interest_area(gemeinde_ladestationen_poly, index_df,
                                                                          interest_area, aoi_type, ars_dict)

        # interest_area_ladestationen_poly.to_csv('interest_area_ladestationen_poly.csv',
        #                                         sep='\t',
        #                                         encoding='utf-8',
        #                                         index=False)

        # TODO: Adjusting dtype of geometry column of interest_area_ladestationen_poly to handle writing to .parquet

    #     interest_area_ladestationen_poly = interest_area_ladestationen_poly.astype('object')
    #     interest_area_ladestationen_poly.to_parquet(ifunc.concat_aoi_path(gdf_path, interest_area, aoi_type))
    #     #
    # else:
    #     interest_area_ladestationen_poly = gpd.read_parquet(ifunc.concat_aoi_path(gdf_path, interest_area, aoi_type))

    print("The amount of EV for each Gemeinde in the interest area has been calculated.")

    # TODO: Add following segment into writeback-loop
    parking_data = ju.read_json_elements(config_obj, 'parking_values', "filepath")
    parking_areas_of_intr = mpa.parking_areas_in_interest_area(parking_data, interest_area_ladestationen_poly)
    parking_areas_of_intr = mpa.get_ladesaeulen_locations(
        parking_areas_of_intr)
    pts(parking_areas_of_intr, aoi_polygon, interest_area)

    print(".html plot for amount of EV for each Gemeinde in the interest area has been created.")

    totaltimestop = timeit.default_timer()
    print(totaltimestop - totaltimes)

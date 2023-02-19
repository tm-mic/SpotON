import unittest
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
import main as main


class TestBedarfe(unittest.TestCase):

    def setUp(self):
        # aoi_df for BAMBERG taken from main before disaggr_age_df
        self.df = pd.read_csv(r'E:\Universität\KInf-Projekt-BM\spoton\aoi_df.csv',
                              sep=',',
                              engine='python',
                              nrows=1000,
                              encoding='utf-8',
                              encoding_errors='replace',
                              on_bad_lines='warn')

        self.df["Auspraegung_Code"] = main.change_col_type(self.df, "Auspraegung_Code", 'int8')
        self.df["Anzahl"] = main.change_col_type(self.df, "Anzahl", 'int8')
        self.index_columns = ["Gitter_ID_100m", "Merkmal", "Auspraegung_Code", 'AOI', 'GEN', 'Attr Index', 'geometry']
        self.interest_area = 'BAMBERG'

        # Import mapping
        self.config_obj = ju.read_json("IandO/config.json")
        self.attr_mapping = ju.read_json_elements(self.config_obj, 'attr_mapping')
        self.weight_map = ju.read_json_elements(self.config_obj, 'weight_mapping')

    def tearDown(self):
        self.df = pd.read_csv(r'E:\Universität\KInf-Projekt-BM\spoton\aoi_df.csv',
                              sep=',',
                              engine='python',
                              nrows=1000,
                              encoding='utf-8',
                              encoding_errors='replace',
                              on_bad_lines='warn')

    def test_split_val_by_share(self):
        val_in = 1
        share = 0.5
        val_one = int(val_in) * share
        val_two = int(val_in) - val_one
        result = bed.split_val_by_share(1, 0.5)
        self.assertEqual(result[0], val_one)
        self.assertEqual(result[1], val_two)
        with self.assertRaises(AttributeError):
            self.bed.split_val_by_share(1, 1.5)
            self.bed.split_val_by_share(1, -0.5)

    # def test_reallocate_vals(self):
    # Not tested since this is merely a helper function

    def test_disaggr_age_df(self):
        self.df = self.df.loc[self.df['Merkmal'] == 'ALTER_KURZ']
        # dataframes for attribute testing
        self.geb_df = self.df.assign(Merkmal='GEBTYPGROESSE').iloc[0:10]
        self.geb_df = self.geb_df.assign(Auspraegung_Code=[1, 2, 3, 4, 5, 6, 7, 8, 9, 10])

        self.hh_df = self.df.assign(Merkmal='HHTYP_FAM').iloc[0:5]
        self.hh_df = self.hh_df.assign(Auspraegung_Code=[1, 2, 3, 4, 5])

        self.age_df = self.df.assign(Merkmal='ALTER_10JG').iloc[0:9]
        self.age_df = self.age_df.assign(Auspraegung_Code=[1, 2, 3, 4, 5, 6, 7, 8, 9])

        df = main.disaggr_age_df(self.df, distro_val=[0.4, 0.6])
        cols = df['Merkmal'].to_list()
        values = df['Auspraegung_Code'].to_list()
        expected_range = [1, 2, 3, 4, 5, 6, 7, 8, 9]
        # Cannot test specific value conversion due to noise in group allocation
        self.assertNotIn('ALTER_KURZ', cols)
        for n in values:
            self.assertIn(n, expected_range)

    # def test_rem_by_mask(self):
    # Simply drops values, helper function to test_disaggr_age_df, no test

    # Used in main after test_disaggregate_age_attr
    def test_remap_groups(self):
        expected_geb = [1, 1, 1, 1, 1, 1, 2, 2, 2, 2]
        expected_hh = [3, 2, 1, 2, 3]
        expected_age = [0, 0, 4, 3, 2, 1, 3, 4, 0]
        self.geb_df = bed.remap_groups(self.geb_df, self.attr_mapping)
        self.assertEqual(self.geb_df['Auspraegung_Code'].to_list(), expected_geb)
        self.hh_df = bed.remap_groups(self.hh_df, self.attr_mapping)
        self.assertEqual(self.hh_df['Auspraegung_Code'].to_list(), expected_hh)
        self.age_df = bed.remap_groups(self.age_df, self.attr_mapping)
        self.assertEqual(self.age_df['Auspraegung_Code'].to_list(), expected_age)

    def test_calc_group_max(self):
        df = bed.remap_groups(self.df, mapping=self.attr_mapping)
        df = bed.calc_group_max(df)
        expected_res = [10, 10, 10]
        res = df['Anzahl'].to_list()
        self.assertEqual(res, expected_res)

    # Essentially a max() function for groups, won't be tested due to time constraints
    def test_calc_attr_max_ratios(self):
        df = main.calc_attr_max_ratios(self.df)


    # def test_mult_col_dict(self):

    def test_calc_cell_index(self):
        cell_df = bed.disaggregate_age_attr(self.df, dis_low=0.4, dis_high=0.6)
        cell_df = bed.remap_groups(cell_df, self.attr_mapping)
        attr_max_cell = bed.calc_group_max(cell_df)

        # This part mimics calc_attr_max_ratios from main
        cell_df = cell_df.merge(attr_max_cell, on="Gitter_ID_100m", how='inner')
        cell_df['Attr_to_total'] = bed.calc_group_max(cell_df)
        cell_df[['Anzahl']].div(cell_df['TotalObservations'], axis=0)

        cell_df = bed.mult_col_dict(cell_df, self.weight_map, new_col='Attr Index', prdne='Attr_to_total',
                                   prdtwo='Auspraegung_Code',
                                   cond='Merkmal')
        gemeinden = cell_df.groupby('GEN')
        cell_df = bed.calc_cell_index(gemeinden, self.weight_map, self.index_columns, self.interest_area)



if __name__ == '__main__':
    unittest.main()

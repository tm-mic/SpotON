import unittest
import geopandas as gpd
import pandas as pd
import IandO.json_utility as ju
import bedarfe as bed
import main as main


class TestBedarfe(unittest.TestCase):

    def setUp(self):
        d = {'Gitter_ID_100m': ["ID_1", "ID_1", "ID_2", "ID_2", "ID_3", "ID_3", "ID_4", "ID_4", "ID_5", "ID_5"],
             'Merkmal': pd.Series,
             'Auspraegung_Code': [0, 1, 2, 3, 4, 0, 1, 2, 3, 4],
             'Anzahl': [1, 2, 3, 6, 4, 8, 5, 10, 6, 12],
             'AOI': pd.Series,
             'GEN': pd.Series,
             'geometry': None}
        self.df = gpd.GeoDataFrame(d, geometry=None, crs=None)

        self.df["Auspraegung_Code"] = main.change_col_type(self.df, "Auspraegung_Code", 'int8')
        self.df["Anzahl"] = main.change_col_type(self.df, "Anzahl", 'int8')
        self.index_columns = ["Gitter_ID_100m", "Merkmal", "Auspraegung_Code", 'AOI', 'GEN', 'Attr Index', 'geometry']
        self.interest_area = 'Testfield'

        # Import mapping
        self.config_obj = ju.read_json("IandO/config.json")
        self.attr_mapping = ju.read_json_elements(self.config_obj, 'attr_mapping')
        self.weight_map = ju.read_json_elements(self.config_obj, 'weight_mapping')

        # Attribute dfs
        self.geb_df = self.df.assign(Merkmal='GEBTYPGROESSE').iloc[0:10]
        self.geb_df = self.geb_df.assign(Auspraegung_Code=[1, 2, 3, 4, 5, 6, 7, 8, 9, 10])
        self.hh_df = self.df.assign(Merkmal='HHTYP_FAM').iloc[0:5]
        self.hh_df = self.hh_df.assign(Auspraegung_Code=[1, 2, 3, 4, 5])
        self.age_df = self.df.assign(Merkmal='ALTER_KURZ').iloc[0:5]
        self.age_df = self.age_df.assign(Auspraegung_Code=[1, 2, 3, 4, 5])

        # multicoldict df
        self.mcd_df = self.df.assign(Merkmal=('GEBTYPGROESSE',
                                              'GEBTYPGROESSE',
                                              'HHTYP_FAM',
                                              'HHTYP_FAM',
                                              'HHTYP_FAM',
                                              'ALTER_10JG',
                                              'ALTER_10JG',
                                              'ALTER_10JG',
                                              'ALTER_10JG',
                                              'ALTER_10JG'))
        self.mcd_df = self.mcd_df.assign(Auspraegung_Code=(1, 2,
                                                           1, 2, 3,
                                                           0, 4, 3, 2, 1))

        self.index_df = self.mcd_df
        self.index_df['Attr_to_total'] = main.calc_attr_max_ratios(self.mcd_df)
        self.index_df = bed.mult_col_dict(self.index_df, self.weight_map, new_col='Attr Index', prdne='Attr_to_total',
                                          cond='Merkmal')
        self.index_df = self.index_df.assign(AOI='Testfield')

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
        age_df = main.disaggr_age_df(self.age_df, distro_val=[0.4, 0.6])
        cols = age_df['Merkmal'].to_list()
        # Cannot test specific value conversion due to noise in group allocation
        self.assertNotIn('ALTER_KURZ', cols)
        age_values = age_df['Auspraegung_Code'].to_list()
        age_expected_range = [1, 2, 3, 4, 5, 6, 7, 8, 9]
        for n in age_values:
            self.assertIn(n, age_expected_range)

    # def test_rem_by_mask(self):
    # Simply drops values, helper function to test_disaggr_age_df, no test

    def test_remap_groups(self):
        expected_geb = [1, 1, 1, 1, 1, 1, 2, 2, 2, 2]
        expected_hh = [3, 2, 1, 2, 3]
        expected_age = [0, 0, 4, 3, 2, 1, 3, 4, 0]
        geb_df = bed.remap_groups(self.geb_df, self.attr_mapping)
        self.assertEqual(geb_df['Auspraegung_Code'].to_list(), expected_geb)
        hh_df = bed.remap_groups(self.hh_df, self.attr_mapping)
        self.assertEqual(hh_df['Auspraegung_Code'].to_list(), expected_hh)
        # disaggregation is done in main before remapping, mimicked here
        age_df = main.disaggr_age_df(self.age_df, distro_val=[0.4, 0.6])
        age_df = bed.remap_groups(age_df, self.attr_mapping)
        self.assertEqual(age_df['Auspraegung_Code'].to_list(), expected_age)

    def test_calc_attr_max_ratios(self):
        attr_df = self.df.assign(Merkmal=('ALTER10JG'))
        # test
        attr_df['Attr_to_total'] = main.calc_attr_max_ratios(attr_df)
        expected_res = [0.5, 1, 0.5, 1, 0.5, 1, 0.5, 1, 0.5, 1]
        self.assertEqual(attr_df['Attr_to_total'].to_list(), expected_res)

    def test_mult_col_dict(self):
        mcd_df = self.mcd_df
        mcd_df['Attr_to_total'] = main.calc_attr_max_ratios(self.mcd_df)
        # test
        mcd_df = bed.mult_col_dict(mcd_df, self.weight_map, new_col='Attr Index', prdne='Attr_to_total',
                                   cond='Merkmal')
        expected_res = [0.375, 0.25, 0.375, 0.5, 0.125, 0, 0.125, 0.35, 0.225, 0.75]
        self.assertEqual(mcd_df['Attr Index'].to_list(), expected_res)

    def test_get_code_counts(self):
        res = bed.get_code_counts(self.index_df)
        expected_res = 1
        self.assertIn(expected_res, res['Counts'].to_list())

    def test_calc_attr_median(self):
        codes_count = bed.get_code_counts(self.index_df)
        sum_codes = bed.group_and_sum_code_counts(self.index_df)
        attr_median = bed.inner_merge_df(sum_codes, codes_count)
        attr_median['Calc Distro Attr/Cell'] = bed.calc_attr_median(attr_median)
        self.assertEqual((attr_median['Anzahl'].div(attr_median['Counts']).to_list()),
                         attr_median['Calc Distro Attr/Cell'].to_list())

    def test_infer_gem_vals(self):
        codes_count = bed.get_code_counts(self.mcd_df)
        sum_codes = bed.group_and_sum_code_counts(self.mcd_df)
        attr_median = bed.inner_merge_df(sum_codes, codes_count)
        attr_median['Calc Distro Attr/Cell'] = bed.calc_attr_median(attr_median)
        attr_ratio = bed.count_attr_in_gem(self.mcd_df)
        merge = attr_median.merge(attr_ratio, on='Merkmal', how='inner').rename({0: 'Ratio'}, axis=1)
        expected_res = (merge['Calc Distro Attr/Cell'] * merge['Ratio']).to_list()
        gem_vals = bed.infer_gem_vals(attr_median, attr_ratio, self.weight_map)
        self.assertEqual(gem_vals['Gemeinde Fill Values'].to_list(), expected_res)


if __name__ == '__main__':
    unittest.main()

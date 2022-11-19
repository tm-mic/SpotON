import pandas as pd
import general_data_aggregation
from IandO import IandO, IandOMetaData
import unique_crs


def calculate_weight_cell_ratio_for_attr_val(aggregated_data, weight_mapping, default_weight=0, print_attr_for_cell=False,
                                             included_columns=None):
    """The function to calculat averaged and weigthed ratios for each cell and attribute value combination.


    Returns:
        This function returns a weighted average for each attr_value combination in each cell.


    Parameters:
        [1] Dataframe with following necessary columns: ["Gitter_ID_100m_neu", "Merkmal", "Auspraegung_Code", "Anzahl"] <--> to be sure not to throw errors import your
        data via the IandO module and aggregate the data via general_data_aggregation function

        [2] A weight mapping dict of dicts specifying a weight between 0 - 1 (or any other pre-determined scale) for each attr_value combination
        f.e.


    If no weight is specified or a key error is encountered the default weight of 0 is assumed and your results will not be valid.

    example Input with csv import specified:

        meta_data_path = IandO.pull_path_column_list_in_config('/Users/msccomputinginthehumanities/PycharmProjects/SpotON/fileconfig.txt',
                                              'Gebaeude')[2]

        imported_df = IandO.import_single_file_as_UTF(
            '/Users/msccomputinginthehumanities/PycharmProjects/SpotON/fileconfig.txt', 'Gebaeude', nrows=5000)

        mapping_dict = {
            'INSGESAMT' : {0:0},
            'GEBTYPGROESSE': {1: 1, 2: 1, 3: 1, 4: 1, 5: 1, 6: 1, 7: 2, 8: 2, 9: 2, 10: 2},
            'EIGENTUM': {1: 1, 2: 1, 3: 1, 4: 2, 5: 2, 6: 3, 7: 3, 8: 3},
            'WOHNEIGENTUM': {1: 1, 2: 2, 3: 2, 4: 2, 99: 0},
            'HEIZTYP': {1: 3, 2: 3, 3: 15, 4: 15, 5: 15, 6: 15}
        }

        weight_mapping = {
            'GEBTYPGROESSE': {1:1, 2:1},
            'EIGENTUM':{1:1, 2:1,3:1},
            'Heiztyp':{3:0, 15:0.9}
        }
        aggregated_data = general_data_aggregation.data_aggregation(imported_df, meta_data_path, mapping_dict)


    Note: the arithmetic middle of two ratios [1. ratio of ratio_value=(attr_values / attr_count) and ratio_to_total=(attr_values / total observation in the cell)]
    are calculated to adjust for the fact that the total of observations is often unequal to the amount specified for certain
    attr. Without this middeling of values certain attributes would be overstated in their importance.
    If total_count of observations and sum of attribute observations is equal this operation does not have an effect."""

    if included_columns is None:
        included_columns = ['Gitter_ID_100m_neu', 'Merkmal', 'Weighted_Ratio']

    crs_list = unique_crs.get_unique_crs(imported_df, 1)
    result_df = pd.DataFrame(columns=included_columns)

    for unique_cell_crs in crs_list:
        queried_group = aggregated_data.query('Gitter_ID_100m_neu == @unique_cell_crs')

        max_attr_count = max(queried_group['Anzahl'])
        cumulative_df = queried_group.groupby(['Gitter_ID_100m_neu', 'Merkmal'], group_keys=True)['Anzahl'].sum()
        cumulative_df = cumulative_df.reset_index(name='Anzahl')

        ratio_df = queried_group.merge(cumulative_df, on=['Gitter_ID_100m_neu', 'Merkmal'], how='left')
        ratio_df[['Ratio']] = ratio_df[['Anzahl_x']].div(ratio_df['Anzahl_y'], axis=0)

        ratio_df['Max Total'] = max_attr_count
        ratio_df[['Ratio to Total']] = ratio_df[['Anzahl_x']].div(ratio_df['Max Total'], axis=0)

        attr_spec_weighted_ratios = []

        for row in ratio_df.itertuples(index=False):
            attr_code = row[2]

            try:
                attr_weight = weight_mapping[row[1]].get(attr_code)
                if attr_weight is False:
                    attr_weight = default_weight

            except KeyError:
                default_weight = 0

            ratio_value = row[5]
            ratio_to_total = row[7]
            arithmetic_middle_ratio = (ratio_value+ratio_to_total)/2

            attr_spec_weighted_ratios.append(arithmetic_middle_ratio * attr_weight)
        ratio_df['Weighted_Ratio'] = attr_spec_weighted_ratios

        if print_attr_for_cell:
            print(ratio_df)

        result_df = pd.concat([result_df, ratio_df], ignore_index=True, sort=False)[result_df.columns]

    return result_df

calculate_weight_cell_ratio_for_attr_val()

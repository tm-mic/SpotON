#funKtion geschrieben von Christian L.

from IandO import IandO, IandOMetaData
import pandas as pd
from datetime import datetime

#Example on how the input should be formatted
# meta_data_path = IandO.pull_path_column_list_in_config('/Users/msccomputinginthehumanities/PycharmProjects/SpotON/fileconfig.txt', 'Gebaeude')[2]
# imported_df = data_df = IandO.import_single_file_as_UTF('/Users/msccomputinginthehumanities/PycharmProjects/SpotON/fileconfig.txt', 'Gebaeude', nrows=500000)
#
# mapping_dict = {
#     'INSGESAMT':{0:0},
#     'GEBTYPGROESSE':{1:1, 2:1, 3:1, 4:1, 5:1, 6:1, 7:2, 8:2, 9:2, 10:2},
#     'EIGENTUM':{1:1, 2:1, 3:1, 4:2, 5:2, 6:2, 7:2, 8:2},
#     #'WOHNEIGENTUM':{1:1, 2:2, 3:2, 4:2, 99:0}
# }

# calling the function f.e. data_aggregation(imported_df,meta_data_path,mapping_dict)


def check_if_attr_exists(meta_data_path: str, single_attr_to_aggr: str):
    metadata_df: dict = IandOMetaData.create_attribute_value_dict(meta_data_path)
    if single_attr_to_aggr in metadata_df:
        pass
    else:
        print(f"The attribute {single_attr_to_aggr} you are trying to aggregate over does not exist in this data table. "
                  f"Please choose a different attribute. Following attributes and corresponding values that can be aggregated over in this table {metadata_df}.")
        check_if_attr_exists(meta_data_path, single_attr_to_aggr)


def data_aggregation(
        imported_df,
        metadata_path: str,
        mapping_dict: dict,
        return_dataframe: bool = True,
) -> pd.DataFrame :
    """data aggregation function is used to aggregate attributes over predefined groups by unique crs.
    Input:
    [1] imported dataframe containing at least following columns ["Gitter_ID_100m_neu", "Merkmal", "Auspraegung_Code", "Anzahl"]
    [2] path to metadata
    [3] dictionary mapping existing attribute values to desired grouped attribute values

    Output:
    The function returns a dataframe with the 4 rows specified above. CRS cells are unique, Merkmale are grouped(attr value) and aggregated (count)."""
    for single_attr in mapping_dict.keys():
        check_if_attr_exists(metadata_path, single_attr)


    nec_columns = ["Gitter_ID_100m_neu", "Merkmal", "Auspraegung_Code", "Anzahl"]
    try:
        imported_df = imported_df[nec_columns]
    except KeyError:
        print(f'The keys necessary for this function do not exist in your dataframe. You need to include {nec_columns} when importing for this function to work.')

    # pass mapping of valid attr-values and data to be aggregated to
    result_list = []
    # TODO: avoid running through imported df as often attributes are requested --> possibly using zip() function
    for single_attr in mapping_dict.keys():
        for index, row in imported_df.iterrows():
            if row.Merkmal == single_attr:
                current_code = row.Auspraegung_Code
                aggr_code = mapping_dict[single_attr].get(current_code)
                row_to_append = [row.Gitter_ID_100m_neu, single_attr, aggr_code, row.Anzahl]
                result_list.append(row_to_append)

    result_df = pd.DataFrame(result_list, columns=nec_columns)

    attr_values = result_df.groupby(['Gitter_ID_100m_neu','Merkmal','Auspraegung_Code'], group_keys=True)['Anzahl'].sum()

    if return_dataframe:
        attr_values = attr_values.reset_index(name='Anzahl')
    return attr_values


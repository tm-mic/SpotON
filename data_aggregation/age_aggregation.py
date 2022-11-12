# function written by Christian L.

import pandas as pd
from IandO import IandO
import random as rnd


val_sum = 1
splitter = 1
result_list = []
age_dict = {}
result_df = pd.DataFrame


def age_aggregation(
        file_config_path: str,
        file_identifier: str,
        nrows=5000,
        dis_even=0.5,
        dis_uneven_low=0.25,
        dis_uneven_high=0.75,
        alter_kurz='ALTER_KURZ',
        attr_ident = 'ALTER_10JG_NEU',
        printer=False,
        return_dataframe=False
) -> dict:
    """Function specific solely for reading and disaggregating age groups in Bevoelkerung table.

    Takes config file path and file identifier as argument. Returns dict with key: unique coordinate identifier of Gitterzelle;
    list of lists containing [Auspraegung_Code, Anzahl].

    ALTER_10JG attribute values are prefered, if none are present data from ALTER_KURZ is disaggregated and inserted into the dataframe. Each Group of ALTER_10JG only appears once for the given coordinate.

    Groups are disaggregated by percentage as full ints given by [dis_even]; [dis_uneven_low, dis_uneven_high].
    Lower age group is served first. Older age group is served second.

    Printer: is set to false by default. True value will log dataframe section with unique Gitter_100m_neu to console.

    attr_ident: allows you to set a custom attribute identifier for values from disaggregated groups. The default is set to Alter_10JG_NEU for you to see
    which data has been inserted.

    return_dataframe: can be used to return dataframe instead of dict.
    """

    try:
        if nrows=='all':
            data_import = IandO.import_single_file_as_UTF(file_config_path, file_identifier)
        else:
            data_import = IandO.import_single_file_as_UTF(file_config_path, file_identifier, nrows=nrows)
    except:
        print("IandO.import_single_file_as_UTF does not function correctly. Check your configfile.")

    try:
        data_import = data_import[["Gitter_ID_100m_neu", "Merkmal", "Auspraegung_Code", "Anzahl"]]
    except KeyError:
        print("Error: The age_aggregation function needs following columns to work correctly Gitter_ID_100m_neu, Merkmal, Auspraegung_Code, Anzahl; if these are not passed"
              "with the dataimport the function does not work. Please reconsider your config file and include these columns among the others to be imported.")
    for index, row in data_import.iterrows():
        if str(row.Merkmal).__contains__('ALTER_10JG'):
            result_list.append(row)

        if row.Merkmal == alter_kurz:
            if row.Anzahl > 1:
                val_sum = row.Anzahl
                split_val = val_sum % 2
                if split_val == 0:
                    splitter = dis_even
                else:
                    splitter = rnd.uniform(dis_uneven_low, dis_uneven_high)
            if row.Auspraegung_Code == 1:
                val_sum_to_pass = int(val_sum * splitter)
                row_to_add = [row.Gitter_ID_100m_neu, attr_ident, 1, val_sum_to_pass]
                result_list.append(row_to_add)
                rest_val_sum = val_sum - val_sum_to_pass
                if rest_val_sum > 0:
                    row_to_add = [row.Gitter_ID_100m_neu, attr_ident, 2, rest_val_sum]
                    result_list.append(row_to_add)

            if row.Auspraegung_Code == 2:
                row_to_add = [row.Gitter_ID_100m_neu, attr_ident, 3, val_sum]
                result_list.append(row_to_add)

            if row.Auspraegung_Code == 3:
                val_sum_to_pass = int(val_sum * splitter)
                row_to_add = [row.Gitter_ID_100m_neu, attr_ident, 4, val_sum_to_pass]
                result_list.append(row_to_add)
                rest_val_sum = val_sum - val_sum_to_pass
                if rest_val_sum > 0:
                    row_to_add = [row.Gitter_ID_100m_neu, attr_ident, 5, rest_val_sum]
                    result_list.append(row_to_add)

            if row.Auspraegung_Code == 4:
                val_sum_to_pass = int(val_sum * splitter)
                row_to_add = [row.Gitter_ID_100m_neu, attr_ident, 6, val_sum_to_pass]
                result_list.append(row_to_add)
                rest_val_sum = val_sum - val_sum_to_pass
                if rest_val_sum > 0:
                    row_to_add = [row.Gitter_ID_100m_neu, attr_ident, 7, rest_val_sum]
                    result_list.append(row_to_add)

            if row.Auspraegung_Code == 5:
                val_sum_to_pass = int(val_sum * splitter)
                row_to_add = [row.Gitter_ID_100m_neu, attr_ident, 8, val_sum_to_pass]
                result_list.append(row_to_add)
                rest_val_sum = val_sum - val_sum_to_pass
                if rest_val_sum > 0:
                    row_to_add = [row.Gitter_ID_100m_neu, attr_ident, 9, rest_val_sum]
                    result_list.append(row_to_add)


    result_df = pd.DataFrame(result_list, columns=['Gitter_100m_neu', 'Merkmal', 'Auspraegung_Code', 'Anzahl'])
    unique_gitter = pd.unique(result_df.iloc[:, 0])

    for gitter in unique_gitter:
        attr_values = result_df[result_df['Gitter_100m_neu'] == gitter]
        attr_values = attr_values.drop_duplicates(subset='Auspraegung_Code', keep='first')
        attr_values = attr_values.sort_values(by='Auspraegung_Code')
        if printer is True:
            print(attr_values)
        pd.concat([result_df, attr_values], ignore_index=True)
    if return_dataframe is True:
        return result_df
    else:
        value_list = attr_values[['Auspraegung_Code', 'Anzahl']].values.tolist()
        age_dict.__setitem__(gitter, value_list)
        return age_dict

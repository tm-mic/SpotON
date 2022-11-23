from IandO import *

"""Function requires dict of dicts "nested_bundeslaender" from IandO"""
"""Specify if Gitter_ID_100m or Gitter_ID_100m_neu is used"""
"""TODO: Automate said specification"""

def subset_all_files_by_bundesland_and_column(filelist: list, config_file_path: str,
                                              bundesland: str, column: str, nrows=1):

    df_dict = import_all_config_specified_csv(filelist,
                                              config_file_path,
                                              nrows)

    keylist = list(df_dict.keys())
    crs_100km = (nested_bundeslaender[bundesland]["crs_100km"])

    for item in keylist:
        matches = []
        df = df_dict.get(item)
        crs_series = df[column]

        for crs in crs_series:
            # if s.search('N+\D\D') + s.search('E+\D\D') == (nested_bundeslaender[bundesland_name]["crs_keys"]):
            for crs_key in crs_100km:
                """substring an Gitter_ID_100m oder Gitter_ID_100m_neu anpassen"""
                if (crs[14:17] + crs[22:25]) == crs_key:  # Gitter_ID_100m_neu
                    # if (crs[4:7] + crs[10:13]) == crs_key:  # Gitter_ID_100m
                    matches.append(crs)

        df_bundesland = df.loc[df[column] == matches]

        return df_bundesland


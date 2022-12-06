from IandO import *
from IandO.IandO import import_single_file_as_UTF
from subset_operations.get_crs_by_bundesland import get_crs_by_bundesland

"""Specify in code if Gitter_ID_100m or Gitter_ID_100m_neu is used"""
"""TODO: Automate said specification"""
"""Consider hard coding gitter_path & shapefile_path or getting both from fileconfig.txt"""

def subset_single_file_by_bundesland_and_column(config_file_path: str,
                                                file_identifier: str,
                                                column: str,
                                                gitter_path: str,
                                                shapefile_path: str,
                                                bundesland_ARS: int,
                                                nrows = 5000):

    df = import_single_file_as_UTF(config_file_path, file_identifier, nrows)
    crs_series = df[column]
    matches = []

    crs_100km = get_crs_by_bundesland(gitter_path, shapefile_path)
    crs_100km["ARS"] = crs_100km["ARS"].astype('int')
    crs_100km = crs_100km[crs_100km["ARS"] == bundesland_ARS]
    crs_100km = crs_100km["id"]

    for crs in crs_series:
        for crs_key in crs_100km:
            """substring an Gitter_ID_100m oder Gitter_ID_100m_neu anpassen"""
            # if (crs[14:17] + crs[22:25]) == crs_key[5:11]: # Gitter_ID_100m_neu
            if (crs[4:7] + crs[10:13]) == crs_key[5:11]:  # Gitter_ID_100m
                matches.append(crs)

    df_bundesland = df.loc[df[column] == matches]

    return df_bundesland
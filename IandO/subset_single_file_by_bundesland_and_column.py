from IandO import *

def subset_single_file_by_bundesland_and_column(config_file_path: str, file_identifier: str, bundesland: str,
                                                column: str, nrows = 5000):

    df = import_single_file_as_UTF(config_file_path, file_identifier, nrows)
    crs_series = df[column]
    crs_100km = (nested_bundeslaender[bundesland]["crs_100km"])
    matches = []

    for crs in crs_series:
        for crs_key in crs_100km:
            """substring an Gitter_ID_100m oder Gitter_ID_100m_neu anpassen"""
            # if (crs[14:17] + crs[22:25]) == crs_key: # Gitter_ID_100m_neu
            if (crs[4:7] + crs[10:13]) == crs_key:  # Gitter_ID_100m
                matches.append(crs)

    df_bundesland = df.loc[df[column] == matches]

    # return df_bundesland
    print(df_bundesland)

subset_single_file_by_bundesland_and_column("/Users/BMS/OneDrive - Otto-Friedrich-Universität Bamberg/Dokumente/Universität/CitH/KInf-Projekt-BM/spoton/fileconfig.txt",
                                            "Bevoelkerung",
                                            "Bayern",
                                            "Gitter_ID_100m",
                                            nrows=500)
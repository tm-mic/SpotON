import geopandas
import pandas

"""Function requires import_single_file_as_UTF from IandO.IandO and geogitter_bundeslaender_gdf from subset_operations.get_crs_by_bundesland
Returns subset of dataframe_dictionary_of_imported_csv of all imported csv by given bundesland"""

def subset_single_file_by_bundesland_and_column(imported_csv: pandas.DataFrame,
                                                geogitter_bundeslaender_gdf: geopandas.GeoDataFrame,
                                                bundesland: str):

    geogitter_bundeslaender_gdf["ARS"] = geogitter_bundeslaender_gdf["ARS"].astype('int')
    geogitter_bundesland_gdf = geogitter_bundeslaender_gdf[geogitter_bundeslaender_gdf["NAME"].str.contains(bundesland, case = False)]
    geogitter_bundesland_gdf = geogitter_bundesland_gdf["id"]

    matches = []

    df = imported_csv

    for col in df.columns:
        if 'Gitter_ID_100m' == col:
            crs_series = df[col]
            for crs in crs_series:
                for id in geogitter_bundesland_gdf:
                    if (crs[4:7] + crs[10:13]) == id[5:11]:
                        matches.append(crs)
            df_bundesland = df[df[col].isin(matches)]

        elif 'Gitter_ID_100m_neu' == col:
            crs_series = df[col]
            for crs in crs_series:
                for id in geogitter_bundesland_gdf:
                    if (crs[14:17] + crs[22:25]) == id[5:11]:
                        matches.append(crs)
            df_bundesland = df[df[col].isin(matches)]

    return df_bundesland
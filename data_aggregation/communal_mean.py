import geopandas


def group_gemeinde_attr_val(geo_df_gemeinde_crs: geopandas.GeoDataFrame,
                            groupby_list=None, rename_list=None):
    """Returns: Dataframe with the calculated mean of the number of attr-value characteristic.
    """
    if groupby_list is None:
        groupby_list = ['NAME', 'Merkmal', 'Auspraegung_Code']
    if rename_list is None:
        rename_list = ['NAME', 'Merkmal', 'Auspraegung_Code', 'Anzahl']
    mean_attr_val_gemeinde = geo_df_gemeinde_crs.groupby(by=groupby_list, axis=0).mean('Anzahl')
    mean_attr_val_gemeinde = mean_attr_val_gemeinde.reset_index()
    return mean_attr_val_gemeinde[rename_list]


def get_attr_val_ratio_per_crs(geo_df_gemeinde_crs: geopandas.GeoDataFrame,
                            groupby_list=None, rename_list=None):
    """Returns: Ratio of number of attribute value occurences in the given Geodataframe divided by total number of
    occurences of this specific attribute in the same Geodataframe."""

    if groupby_list is None:
        groupby_list = ['NAME', 'Merkmal', 'Auspraegung_Code']

    if rename_list is None:
        rename_list = ['NAME', 'Merkmal', 'Auspraegung_Code', 'Count_Ratio']

    ratio_attr_val_to_total_gemeinde = geo_df_gemeinde_crs.groupby(by=groupby_list, axis=0).count()
    ratio_attr_val_to_total_gemeinde = ratio_attr_val_to_total_gemeinde.reset_index()
    ratio_attr_val_to_total_gemeinde = ratio_attr_val_to_total_gemeinde.rename(columns={'Anzahl':'Count'})

    ratio_attr_val_to_total_gemeinde = ratio_attr_val_to_total_gemeinde[['NAME', 'Merkmal', 'Auspraegung_Code', 'Count']]
    attr_val_total_count = ratio_attr_val_to_total_gemeinde.groupby(['NAME', 'Merkmal']).sum('Count').reset_index()[['NAME', 'Merkmal', 'Count']]

    ratio_attr_val_to_total_gemeinde = ratio_attr_val_to_total_gemeinde.merge(attr_val_total_count, how='left', on=['NAME', 'Merkmal'])
    ratio_attr_val_to_total_gemeinde['Count_Ratio'] = ratio_attr_val_to_total_gemeinde['Count_x']/ratio_attr_val_to_total_gemeinde['Count_y']
    return ratio_attr_val_to_total_gemeinde[rename_list]

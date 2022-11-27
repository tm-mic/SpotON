import geopandas
import numpy as np


def group_gemeinde_attr_val(geo_df_gemeinde_crs: geopandas.GeoDataFrame,
                            groupby_list=None, value_to_agg='Anzahl'):
    if groupby_list is None:
        groupby_list = ['NAME', 'Merkmal', 'Auspraegung_Code']
    mean_attr_val_gemeinde = geo_df_gemeinde_crs.groupby(by=groupby_list, axis=0).agg({value_to_agg: [np.mean]})
    return mean_attr_val_gemeinde.reset_index()

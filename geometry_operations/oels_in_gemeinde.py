# Responsible for Module: Max W.; written with support by Christian, Thomas
# oels_in_gemeinde is used for the creation of geodataframes with information
# about the ladestationen in a gemeinde.



import pandas as pd


def oels_in_gemeinde(gemeinde_polygon_gdf, ladestationen_gdf):
    """Creates a gdf in which the information about the amount of
    ladestationen for each gemeinde is given."""
    oels_in_gemeinde = gemeinde_polygon_gdf.sjoin(ladestationen_gdf)
    oels_in_gemeinde_values = oels_in_gemeinde['NAME'].value_counts()
    df_oels_in_gemeinde_values = pd.DataFrame(oels_in_gemeinde_values)
    df_oels_in_gemeinde_values = df_oels_in_gemeinde_values.reset_index()
    df_oels_in_gemeinde_values.rename(columns={'index': 'NAME', 'NAME': 'Ladestationen'}, inplace=True)
    gdf_gemeinde_oels = gemeinde_polygon_gdf.merge(df_oels_in_gemeinde_values, how='outer', left_on='NAME',
                                                   right_on='NAME')
    gdf_gemeinde_oels = gdf_gemeinde_oels.fillna(0)
    gdf_gemeinde_oels = gdf_gemeinde_oels.iloc[:, [0, 1, 2, 4, 3]]
    gdf_gemeinde_oels = gdf_gemeinde_oels.set_crs(crs='EPSG:3035')
    return gdf_gemeinde_oels

def add_lp_to_gdf_gemeinde_oels(ladestationen_gdf, gemeinde_polygon_gdf, gdf_gemeinde_oels):
    """Adding the amount of ladepunkte for every gemeinde."""
    ladepunkte_in_gemeinde = gemeinde_polygon_gdf.sjoin(ladestationen_gdf)
    ladepunkte_in_gemeinde = ladepunkte_in_gemeinde[['NAME', 'Anzahl Ladepunkte']]
    ladepunkte_in_gemeinde = ladepunkte_in_gemeinde.groupby(['NAME']).sum()
    ladepunkte_in_gemeinde = pd.DataFrame(ladepunkte_in_gemeinde)
    ladepunkte_in_gemeinde = ladepunkte_in_gemeinde.reset_index()
    gdf_gemeinde_oels = gdf_gemeinde_oels.merge(ladepunkte_in_gemeinde, how='outer', left_on='NAME', right_on='NAME')
    gdf_gemeinde_oels = gdf_gemeinde_oels.fillna(0)
    gdf_gemeinde_oels = gdf_gemeinde_oels.iloc[:, [0, 1, 2, 3, 5, 4]]
    return gdf_gemeinde_oels

def oels_of_gemeinde_in_zula(gdf_gemeinde_oels, base_polygon_gdf):
    """Intersects the centroids of gemeindepolygons with the polygons
    of the zulassungsbezirke. 14 gemeinden are not matched because of there shape.
    This is managed by the next functions."""
    gdf_gemeinde_oels['geometry'] = gdf_gemeinde_oels['geometry'].centroid
    oels_gemeinde_in_zula = base_polygon_gdf.sjoin(gdf_gemeinde_oels)
    oels_gemeinde_in_zula = oels_gemeinde_in_zula.rename(columns={'NAME_left': 'NAME_Zula'
        , 'ARS_left': 'ARS', 'NAME_right': 'NAME_Gemeinde'})
    oels_gemeinde_in_zula = oels_gemeinde_in_zula[
        ['ARS', 'NAME_Zula', 'NAME_Gemeinde', 'BEZ', 'Ladestationen', 'Anzahl Ladepunkte', 'geometry']]
    return oels_gemeinde_in_zula

def lost_gemeinden_gdf(gdf_gemeinde_oels):
    """Creates a gdf with all the missing gemeinden of the previous intersection."""
    missinggemeinde = gdf_gemeinde_oels.loc[
        [483, 933, 4425, 4738, 6176, 6186, 8477, 9151, 9333, 9363, 9428, 9429, 9430, 9461]]
    missinggemeinde = missinggemeinde.reset_index(drop=True)
    missinggemeinde = missinggemeinde.rename(columns={'NAME': 'NAME_Gemeinde'})
    return missinggemeinde

def add_remaining_gemeinden(base_polygon_gdf, missinggemeinde, oels_gemeinde_in_zula):
    """Adds the missing 14 gemeinden to the oels_gemeinde_in_zula gdf."""
    vorpommern = missinggemeinde[missinggemeinde['ARS'] == '13']
    vorpommern = vorpommern.drop([8, 13])
    vorpommern['NAME_Zula'] = base_polygon_gdf['NAME'].loc[369]
    vorpommern['geometry'] = base_polygon_gdf['geometry'].loc[369]
    konstanz = missinggemeinde[missinggemeinde['ARS'] == '08']
    konstanz['NAME_Zula'] = base_polygon_gdf['NAME'].loc[175]
    konstanz['geometry'] = base_polygon_gdf['geometry'].loc[175]
    nwmeck = missinggemeinde[missinggemeinde['NAME_Gemeinde'] == 'Wismar']
    nwmeck['NAME_Zula'] = base_polygon_gdf['NAME'].loc[243]
    nwmeck['geometry'] = base_polygon_gdf['geometry'].loc[243]
    pinneberg = missinggemeinde[missinggemeinde['NAME_Gemeinde'] == 'Helgoland']
    pinneberg['NAME_Zula'] = base_polygon_gdf['NAME'].loc[273]
    pinneberg['geometry'] = base_polygon_gdf['geometry'].loc[273]
    bitburg = missinggemeinde[missinggemeinde['NAME_Gemeinde'] == 'Waldhof-Falkenstein']
    bitburg['NAME_Zula'] = base_polygon_gdf['NAME'].loc[82]
    bitburg['geometry'] = base_polygon_gdf['geometry'].loc[82]
    saarburg = missinggemeinde[missinggemeinde['NAME_Gemeinde'] == 'Ralingen']
    saarburg['NAME_Zula'] = base_polygon_gdf['NAME'].loc[355]
    saarburg['geometry'] = base_polygon_gdf['geometry'].loc[355]
    lindau = missinggemeinde[missinggemeinde['NAME_Gemeinde'] == 'Sigmarszell']
    lindau['NAME_Zula'] = base_polygon_gdf['NAME'].loc[192]
    lindau['geometry'] = base_polygon_gdf['geometry'].loc[192]
    rostock = missinggemeinde[missinggemeinde['NAME_Gemeinde'] == 'Rerik']
    rostock['NAME_Zula'] = base_polygon_gdf['NAME'].loc[303]
    rostock['geometry'] = base_polygon_gdf['geometry'].loc[303]
    oels_gemeinde_in_zula = pd.concat([oels_gemeinde_in_zula, vorpommern, konstanz,
                                       nwmeck, pinneberg, bitburg, saarburg, lindau, rostock])
    oels_gemeinde_in_zula = oels_gemeinde_in_zula.sort_values('NAME_Zula')
    oels_gemeinde_in_zula = oels_gemeinde_in_zula.reset_index(drop=True)
    return oels_gemeinde_in_zula
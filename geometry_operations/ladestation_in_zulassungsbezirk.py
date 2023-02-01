# Responsible for Module: Max W.; written with support by Christian, Thomas
# ladestationen_in_zulassungbezirk is used for the creation of geodataframes with information
# about the ladestationen in a zulassungebezirk. Uses some funtions of coord_to_polygon.



import pandas as pd
import geopandas as gpd
from geometry_operations import coord_to_polygon


def create_car_in_zulassungsbezirk_from_csv(Path: str):
    """From csv several columns are taken and tranformed in a more suitable way for further operations with it.
    For example empty rows got dropped, NAME column transformed and a column with all electric cars is added in EVing."""
    car_in_zula = gpd.GeoDataFrame.from_file(Path)
    car_in_zula = car_in_zula[['field_3','field_4','field_9','field_10','geometry']]
    car_in_zula = car_in_zula.rename(columns={'field_3':'NAME','field_4':'Insgesamt_Pkw','field_9':'PIHybrid','field_10':'Elektro_BEV'})
    car_in_zula = car_in_zula.drop([0,1,2,3,4,5,6,7])
    empty_row = car_in_zula[car_in_zula['NAME']==''].index
    car_in_zula.drop(empty_row, inplace=True)
    car_in_zula['NAME'] = car_in_zula['NAME'].str.slice(7, )
    car_in_zula = car_in_zula.sort_values('NAME')
    car_in_zula['Insgesamt_Pkw'] = car_in_zula['Insgesamt_Pkw'].str.replace('.', '').astype('int64')
    car_in_zula['PIHybrid'] = car_in_zula['PIHybrid'].str.replace('.', '').astype('int64')
    car_in_zula['Elektro_BEV'] = car_in_zula['Elektro_BEV'].str.replace('.', '').astype('int64')
    car_in_zula["EVIng"] = car_in_zula['PIHybrid'].add(car_in_zula['Elektro_BEV'])
    car_in_zula = car_in_zula.iloc[:, [0, 1, 2, 3, 5, 4]]
    return car_in_zula

def renaming_some_zulassungsbezirke(car_in_zula):
    """Does exactly that and sort it alphabetically. Is needed for later merge operations.
    Should not be used on its own."""
    car_in_zula['NAME'] = car_in_zula['NAME'].str.replace('AACHEN, STAEDTEREGION', 'STADTREGION AACHEN')
    car_in_zula['NAME'] = car_in_zula['NAME'].str.replace('FRANKFURT/ODER,STADT', 'FRANKFURT (ODER)')
    car_in_zula['NAME'] = car_in_zula['NAME'].str.replace('GÃ–TTINGEN', 'GOETTINGEN')
    car_in_zula['NAME'] = car_in_zula['NAME'].str.replace('VORPOMMERN-GREIFSWALD', 'GREIFSWALD')
    car_in_zula['NAME'] = car_in_zula['NAME'].str.replace('HANNOVER', 'REGION HANNOVER')
    car_in_zula['NAME'] = car_in_zula['NAME'].str.replace('NEUSTADT/AISCH-WINDSHEIM',
                                                          'NEUSTADT A.D. AISCH-BAD WINDSHEIM')
    car_in_zula['NAME'] = car_in_zula['NAME'].str.replace('NEUSS', 'RHEIN-KREIS NEUSS')
    car_in_zula['NAME'] = car_in_zula['NAME'].str.replace('SAARBRUECKEN,STADTVERB.', 'REGIONALVERBAND SAARBRUECKEN')
    car_in_zula['NAME'] = car_in_zula['NAME'].str.replace('RHEIN.-BERGISCHER KREIS', 'RHEINISCH-BERGISCHER KREIS')
    car_in_zula['NAME'] = car_in_zula['NAME'].str.replace('SANKT WENDEL', 'ST. WENDEL')
    car_in_zula['NAME'] = car_in_zula['NAME'].str.replace('SAAR-PFALZ-KREIS', 'SAARPFALZ-KREIS')
    car_in_zula = car_in_zula.sort_values('NAME')
    car_in_zula = car_in_zula.reset_index(drop=True)
    return car_in_zula

def str_replace_of_name_in_base_polygon_gdf(Path: str):
    """Replaces the umlaute and sorts the gdf alphabetically.
    Should be not used on its own. Should not be used on its own."""
    base_polygon_gdf = coord_to_polygon.load_base_polygon_to_gdf(Path)
    base_polygon_gdf['NAME'] = base_polygon_gdf['NAME'].str.upper()
    base_polygon_gdf['NAME'] = base_polygon_gdf['NAME'].str.replace('Ü', 'UE')
    base_polygon_gdf['NAME'] = base_polygon_gdf['NAME'].str.replace('Ä', 'AE')
    base_polygon_gdf['NAME'] = base_polygon_gdf['NAME'].str.replace('Ö', 'OE')
    base_polygon_gdf = base_polygon_gdf.sort_values('NAME')
    base_polygon_gdf = base_polygon_gdf.reset_index(drop=True)
    return base_polygon_gdf

def delete_doubled_zulassungsbezirke(base_polygon_gdf):
    """Doubled zulassungsbezirke are deleted after a union with zulassungsbezirke which are
    referable for further operations. Should not be used on its own."""
    deletable_double = base_polygon_gdf.loc[[49, 84, 135, 234, 342, 351, 361, 374, 396, 377]]
    deletable_double = deletable_double.reset_index(drop=True)
    staying_double = base_polygon_gdf.loc[[178, 383, 207, 217, 318, 378, 362, 290, 247, 125]]
    staying_double = staying_double.reset_index(drop=True)
    union_double = staying_double.union(deletable_double)
    staying_double['geometry'] = union_double
    base_polygon_gdf = base_polygon_gdf.drop([49, 84, 135, 234, 342, 351, 361, 374, 396, 377])
    staying_double = staying_double.set_index(pd.Index([178, 383, 207, 217, 318, 378, 362, 290, 247, 125]))
    base_polygon_gdf.update(staying_double)
    base_polygon_gdf = base_polygon_gdf.reset_index(drop=True)
    return base_polygon_gdf

def rename_names_of_some_cities(base_polygon_gdf):
    """Does exactly that. Sort the name column alphabetically.
    Should not be used on its own."""
    base_polygon_gdf.at[13, 'NAME'] = 'ANSBACH, STADT'
    base_polygon_gdf.at[14, 'NAME'] = 'ASCHAFFENBURG, STADT'
    base_polygon_gdf.at[17, 'NAME'] = 'AUGSBURG, STADT'
    base_polygon_gdf.at[25, 'NAME'] = 'BAMBERG, STADT'
    base_polygon_gdf.at[28, 'NAME'] = 'BAYREUTH, STADT'
    base_polygon_gdf.at[55, 'NAME'] = 'COBURG, STADT'
    base_polygon_gdf.at[108, 'NAME'] = 'FUERTH, STADT'
    base_polygon_gdf.at[140, 'NAME'] = 'HEILBRONN, STADT'
    base_polygon_gdf.at[153, 'NAME'] = 'HOF, STADT'
    base_polygon_gdf.at[161, 'NAME'] = 'KAISERSLAUTERN, STADT'
    base_polygon_gdf.at[164, 'NAME'] = 'KARLSRUHE, STADT'
    base_polygon_gdf.at[165, 'NAME'] = 'KASSEL, STADT'
    base_polygon_gdf.at[184, 'NAME'] = 'LANDSHUT, STADT'
    base_polygon_gdf.at[188, 'NAME'] = 'LEIPZIG, STADT'
    base_polygon_gdf.at[227, 'NAME'] = 'MUENCHEN, STADT'
    base_polygon_gdf.at[261, 'NAME'] = 'OSNABRUECK, STADT'
    base_polygon_gdf.at[268, 'NAME'] = 'PASSAU, STADT'
    base_polygon_gdf.at[283, 'NAME'] = 'REGENSBURG, STADT'
    base_polygon_gdf.at[301, 'NAME'] = 'ROSENHEIM, STADT'
    base_polygon_gdf.at[303, 'NAME'] = 'ROSTOCK, STADT'
    base_polygon_gdf.at[327, 'NAME'] = 'SCHWEINFURT, STADT'
    base_polygon_gdf.at[393, 'NAME'] = 'WUERZBURG, STADT'
    base_polygon_gdf = base_polygon_gdf.sort_values('NAME')
    base_polygon_gdf = base_polygon_gdf.reset_index(drop=True)
    base_polygon_gdf = base_polygon_gdf.set_crs(crs='EPSG:3035')
    return base_polygon_gdf

def cars_with_zulassungsbezirk_polygon_gdf(car_in_zula, base_polygon_gdf):
    """Merges two geodataframes. Result is a gdf with a NAME, an ARS, an Insgesamt_Pkw, a PIHybrid, an Elektro_BEV,
    an EVIng and a geometry column."""
    del car_in_zula['NAME']
    del car_in_zula['geometry']
    car_and_zula = base_polygon_gdf.merge(car_in_zula, left_index=True, right_index=True)
    car_and_zula = car_and_zula.iloc[:, [0, 1, 3, 4, 5, 6, 2]]
    car_and_zula = car_and_zula.set_crs(crs='EPSG:3035')
    return car_and_zula

def ls_in_zulassungsbezirk(Path: str, car_and_zula):
    """Creates a df with the amount of ladestationen in every zulassungsbezirk."""
    ladestationen_gdf = coord_to_polygon.ladestationen_to_gdf(Path)
    ladestationen_in_zula = car_and_zula.sjoin(ladestationen_gdf)
    ladestationen_in_zula = ladestationen_in_zula['NAME'].value_counts()
    df_ladestationen_in_zula = pd.DataFrame(ladestationen_in_zula)
    df_ladestationen_in_zula = df_ladestationen_in_zula.reset_index()
    df_ladestationen_in_zula.rename(columns={'index': 'NAME', 'NAME': 'Ladestationen'}, inplace=True)
    return df_ladestationen_in_zula

def ls_lp_in_zulassungsbezirk(ladestationen_gdf, car_and_zula, df_ladestationen_in_zula):
    """Adding the amount of landpunkte in every zulassungsbezirk to the df_ladestationen_in_zula."""
    ls_lp_zula = car_and_zula.sjoin(ladestationen_gdf)
    ls_lp_zula = ls_lp_zula[['NAME', 'Anzahl Ladepunkte']]
    ladepunkte = ls_lp_zula.groupby(['NAME']).sum()
    ladepunkte = pd.DataFrame(ladepunkte)
    ladepunkte = ladepunkte.reset_index()
    df_ladestationen_in_zula = df_ladestationen_in_zula.merge(ladepunkte)
    return df_ladestationen_in_zula

def angebot_bedarf_ladestationen_in_zula(car_and_zula, df_ladestationen_in_zula):
    """Merges the amount of ladestationen from df with the gdf of zulassungebezirke with all cars.
    Creates three new columns in which an estimation for the needed expansion of ladestationen are made."""
    ladestationen_amount = car_and_zula.merge(df_ladestationen_in_zula, left_on='NAME', right_on='NAME')
    ladestationen_amount['Ladepunkteangebot_11zu1'] = ladestationen_amount['EVIng'] / 11
    ladestationen_amount['Ladepunkteangebot_20zu1'] = ladestationen_amount['EVIng'] / 20
    ladestationen_amount['Ausbaubedarf'] = (ladestationen_amount['Ladepunkteangebot_11zu1'] + ladestationen_amount[
        'Ladepunkteangebot_20zu1']) / 2 - ladestationen_amount['Anzahl Ladepunkte']
    ladestationen_amount = ladestationen_amount.iloc[:, [0, 1, 2, 3, 4, 5, 7, 8, 9, 10, 11, 6]]
    return ladestationen_amount

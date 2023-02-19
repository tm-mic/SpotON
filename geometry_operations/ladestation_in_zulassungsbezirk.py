# Responsible for Module: Max W.; written with support by Christian, Thomas
# ladestationen_in_zulassungbezirk is used for the creation of geodataframes with information
# about the ladestationen in a zulassungebezirk. Uses some funtions of coord_to_polygon.



import pandas as pd
from geometry_operations import coord_to_polygon



def renaming_some_zulassungsbezirke(car_in_zula):
    """
    Renaming and sorting alphabetically. Is needed for later merge operations.
    Should not be used on its own.

    :param car_in_zula: geodataframe from create_car_in_zulassungsbezirk_from_csv
    :return: input with some names changed
    """
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
    """
    Replaces the umlaute and sorts the gdf alphabetically.
    Should be not used on its own.

    :param Path: path to a shp with the geometrie data for the zulassungsbezirke of germany
    :return: gdf of the zulassungsbezirke in germany with their umlaute replaced
    """
    base_polygon_gdf = coord_to_polygon.load_base_polygon_to_gdf(Path)
    base_polygon_gdf['NAME'] = base_polygon_gdf['NAME'].str.upper()
    base_polygon_gdf['NAME'] = base_polygon_gdf['NAME'].str.replace('Ü', 'UE')
    base_polygon_gdf['NAME'] = base_polygon_gdf['NAME'].str.replace('Ä', 'AE')
    base_polygon_gdf['NAME'] = base_polygon_gdf['NAME'].str.replace('Ö', 'OE')
    base_polygon_gdf = base_polygon_gdf.sort_values('NAME')
    base_polygon_gdf = base_polygon_gdf.reset_index(drop=True)
    return base_polygon_gdf

def delete_doubled_zulassungsbezirke(base_polygon_gdf):
    """
    Doubled zulassungsbezirke are deleted after a union with zulassungsbezirke which are
    referable for further operations. Should not be used on its own.

    :param base_polygon_gdf: gdf of the zulassungsbezirke in germany
    :return: gdf in which the doubled zulassungbezirke are deleted
    """
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
    """
    Renames some zulassungsbezirke. Sort the name column alphabetically.
    Should not be used on its own. Information gathering by area comparison.

    :param base_polygon_gdf: gdf with all zulassungsbezirke in germany
    :return: gdf where some zulassungsbezirk names were changed
    """
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

def cars_with_zulassungsbezirk_polygon_gdf(kfz_data, kfz_shapefile):
    """
    Merges two geodataframes. Result is a gdf with a NAME, an ARS, an Insgesamt_Pkw, a PIHybrid, an Elektro_BEV,
    an EVIng and a geometry column.

    :param kfz_data: dataframe with the car amount in the zulassungsbezirke in germany
    :param kfz_shapefile: geodataframe with polygons of the zulassungsbezirke
    :return: gdf with the geometry and cars in all zulassungbezirke
    """
    del kfz_data['NAME']
    car_and_zula = kfz_shapefile.merge(kfz_data, left_index=True, right_index=True)
    car_and_zula = car_and_zula.iloc[:, [0, 1, 3, 4, 5, 6, 2]]
    car_and_zula = car_and_zula.set_crs(crs='EPSG:3035')
    return car_and_zula



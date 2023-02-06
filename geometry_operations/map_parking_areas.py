# Responsible for Module: Max W.; written with support by Christian, Thomas
# Generates a GeoDataFrames of  all parking areas in a specific area with the condition
# that the parking areas are suitable for the installation of ladesaeulen.
# Uses parking_areas_with_ls_and_value.geojson which is a GeoDataFrame with all parking areas
# and there values in germany, result of the osm_spot_calculator modul.

import geopandas as gpd


def parking_areas_in_interest_area(parking_areas_w_ls_v: str, interest_area_ladestationen_poly):
    # TODO: interest_area_ladestionen_poly benutzt in meiner Version noch die Funktion, wo ich den dateipfad einlese.
    #  Thomas da musst du schauen ob du da nochmal was an dieser Funktion ändern musst. (bzgl. column names oder so)
    """
    A GeoDataFrame of all (fee: no & access: open or customer[OSM keys]) parking areas in germany is
    joined by the interest_area_ladestationen_poly GeoDataFrame. The resulting Dataframe is filtered
    down by deleting some columns and checking whether the need for ladesaeulen in a gemeinde is fulfilled.
    If it is fulfilled these gemeinden are dropped. Result is a GeoDataFrame with the gemeinden which still need
    ladesaeulen and all possible parking areas in the gemeinde. At last the GeoDataFrame is sorted by
    NAME_Gemeinde and value.

    :param parking_areas_w_ls_v: path to parking_areas_with_ls_and_value.geojson
    :param interest_area_ladestationen_poly: GeoDataFrame with all cars and ladestationen in a zulassungsbezirk
    :return: GeoDataFrame with possible parking area polygons
    """

    parking_areas_w_ls_v = gpd.read_file(parking_areas_w_ls_v, encoding='utf-8')
    interest_area_ladestationen_poly = gpd.GeoDataFrame(interest_area_ladestationen_poly, geometry='geometry')
    parking_area_in_intr_area = parking_areas_w_ls_v.sjoin(interest_area_ladestationen_poly)
    parking_area_in_intr_area = parking_area_in_intr_area[['id', 'value', 'area', 'parking_spots',  'ladesaeulen',
                                                           'NAME_Zula', 'NAME_Gemeinde','BEZ', 'Ladestationen',
                                                           'Anzahl Ladepunkte', 'PIHybrid','Elektro_BEV', 'EVIng',
                                                           'EVGem', 'Bedarf Ladepunkte', 'geometry']]
    parking_area_in_intr_area = parking_area_in_intr_area.loc[parking_area_in_intr_area['Anzahl Ladepunkte'] <
                                                              parking_area_in_intr_area['Bedarf Ladepunkte']]
    parking_area_in_intr_area.insert(5, 'ladepunkte', parking_area_in_intr_area['ladesaeulen'] *2)
    parking_area_in_intr_area.insert(16, 'ausbau', parking_area_in_intr_area['Bedarf Ladepunkte'] -
                                     parking_area_in_intr_area['Anzahl Ladepunkte'])
    parking_areas_of_intr = parking_area_in_intr_area[['id', 'NAME_Gemeinde', 'value', 'area', 'parking_spots',
                                                       'ladesaeulen',  'ladepunkte', 'EVGem', 'Bedarf Ladepunkte',
                                                       'ausbau', 'geometry']]
    parking_areas_of_intr = parking_areas_of_intr.sort_values(by=['NAME_Gemeinde', 'value'], ascending=False)
    return parking_areas_of_intr

def get_ladesaeulen_locations(parking_areas_of_intr):
    """
    Function groups parking_areas_of_intr(sorted descending by value) by NAME_Gemeinde and initializes
    an empty id_list. Two loops are used to run row wise through the grouped dataframes. In those loops
    the ausbau is reduced by the amount of ladepunkte in the row. If the ausbau is lower or equals 0,
    all following parking areas in the same gemeinde are no longer of interest and this process starts
    for the next gemeinde. The ids of those parking areas are written to the id_list, which is used as
    mask on the original parking_areas_of_intr GeoDataFrame. The result is a GeoDataFrame with all
    parking areas, which should get ladesaeulen.

    :param parking_areas_of_intr: GeoDataFrame with potential parking areas in a specific administration area
    :return: GeoDataFrame with all parking areas and their ladesaeulen count
    """

    parking_areas_of_intr_group = parking_areas_of_intr.groupby('NAME_Gemeinde')
    id_list = []
    for name, gem in parking_areas_of_intr_group:
        ausbau = gem.ausbau.values[0]
        for i, row in gem.iterrows():
            while ausbau >= 0:
                if row.ladepunkte != 0:
                    ausbau = ausbau - row.ladepunkte
                    id_list.append(row.id)
                break
    reasonable_parking_areas = parking_areas_of_intr['id'].isin(id_list)
    parking_areas_of_intr = parking_areas_of_intr.loc[reasonable_parking_areas]
    return parking_areas_of_intr
# Responsible for Module: Max W.; written with support by Christian, Thomas
# coord_to_polygon is used for reading shapefiles and the Ladesaeulenregister.


import geopandas as gpd
import matplotlib.pyplot as plt
from shapely.geometry import Point

def load_base_polygon_to_gdf(basepolygon_path: str):
    """
    Loads the shapefile of all Zulassungskreise and transforms the
    EPSG and writes it into a gdf with a Name, ARS and geometry column.
    Slices ARS. Only first Numbers are needed for the Bundesland.

    :param basepolygon_path: path string of a shp of germanys Zulassungskreise
    :return: geodataframe with all Zulassungskreise in Germany (EPSG:3035)
    """
    base_polygon_gdf = gpd.read_file(basepolygon_path, encoding='utf-8')
    base_polygon_gdf = base_polygon_gdf[["NAME", "ARS", "geometry"]]
    base_polygon_gdf['ARS'] = base_polygon_gdf['ARS'].str.slice(0, 2)
    base_polygon_gdf = base_polygon_gdf.to_crs(crs='EPSG:3035')
    return base_polygon_gdf


def load_gemeinde_polygon_to_gdf(gemeindeshp: str):
    """
    Loads the shapefile of all Gemeinden and transforms the
    EPSG and writes it into a gdf with a Name, ARS, BEZ and geometry column.
    Slices ARS. Only first Numbers are needed for the Bundesland.

    :param gemeindeshp: path string of a shp of germanys gemeinden
    :return: geodataframe with all Gemeinden in germany (EPSG:3035)
    """
    gemeinde_polygon_gdf = gpd.read_file(gemeindeshp, encoding='utf-8')
    gemeinde_polygon_gdf = gemeinde_polygon_gdf[["GEN", "ARS", "BEZ", "geometry"]]
    gemeinde_polygon_gdf['ARS'] = gemeinde_polygon_gdf['ARS'].str.slice(0, 2)
    gemeinde_polygon_gdf.rename(columns={'GEN': 'NAME'}, inplace=True)
    gemeinde_polygon_gdf = gemeinde_polygon_gdf.to_crs(crs='EPSG:3035')
    return gemeinde_polygon_gdf


def ladestationen_to_gdf(path: str):
    """
    Creates gdf with all Ladestationen in Germany from csv.
    Geometries in column are points. Transfers it in EPSG:3035.

    :param path: path string for a csv with all the Ladestationen in germany
    :return: geodataframe with all Ladestationen in germany (EPSG:3035)
    """
    ladestationen_gdf = gpd.GeoDataFrame.from_file(path)
    ladestationen_gdf = ladestationen_gdf.rename(columns=ladestationen_gdf.iloc[9]).loc[10:]
    ladestationen_gdf['Breitengrad'] = ladestationen_gdf['Breitengrad'].str.replace(',', '.').astype(float)
    ladestationen_gdf['Laengengrad'] = ladestationen_gdf['Längengrad'].str.replace(',', '.').astype(float)
    ladestationen_gdf['geometry'] = ladestationen_gdf.apply(
        lambda x: Point((float(x.Laengengrad), float(x.Breitengrad))),
        axis=1)
    del ladestationen_gdf['Breitengrad']
    del ladestationen_gdf['Laengengrad']
    ladestationen_gdf['Anzahl Ladepunkte'] = ladestationen_gdf['Anzahl Ladepunkte'].astype('int64')
    ladestationen_gdf = ladestationen_gdf.set_crs(crs='WGS 84', epsg='EPSG:3857')
    ladestationen_gdf = ladestationen_gdf.to_crs(crs='EPSG:3035')
    return ladestationen_gdf


def plot_geodataframe(GeoDataFrame):
    """
    Plots a gdf and shows it.

    :param GeoDataFrame: any GeoDataFrame
    :return: Plot of the gdf
    """
    GeoDataFrame.plot()
    plt.show()
    return GeoDataFrame

import geopandas
import geopandas as gpd
import pandas
import pandas as pd
from pyarrow import csv
import pyarrow as pa
from shapely.geometry import Point


def create_points_from_crs(df, crs='EPSG:3035', lat_col='x_mp_100m', lon_col='y_mp_100m'):
    """
    Creates a gdf with points from df with lat, lon column.

    :param df: Input df with lat, lon column
    :param crs: Crs cast the points to.
    :return: GDF with shapely point column.
    """
    gdf = gpd.GeoDataFrame(df, geometry=geopandas.points_from_xy(df[lat_col], df[lon_col]))
    gdf = gdf.set_crs(crs=crs)
    return gdf.to_crs(crs)


def read_shp(shp_path, cols):
    """
    Reads shp files in UTF-8 encoding. Returns geodataframe containing the cols provided.

    :param shp_path:
    :param cols:
    :return:
    """
    return gpd.read_file(shp_path, encoding='utf-8')[cols]


def obtain_aoi_polygon(shp_path: str, aoi_name: str):
    """
    Grabs a polygon from a shapefile.

    :param shp_path: Path to shapefile
    :param aoi_name: identifier of the polygon to grab
    :param epsg: CRS to cast the polygon to.
    :return: One polygon from shp with x polygons.
    """
    polygon_gdf = gpd.read_file(shp_path, encoding='utf-8')
    if 'GEN' in polygon_gdf:
        polygon_gdf[['GEN', 'geometry']]
        polygon_gdf.rename(columns={'GEN': 'NAME'}, inplace=True)
    polygon_gdf = reproject(polygon_gdf)
    return polygon_gdf.where(polygon_gdf['NAME'] == aoi_name).dropna()


def clip_crs_by_polygon(polygon_mask, point_gdf):
    """
    Clips geodataframe with polygon mask.

    :param polygon_mask: One polygon to clip the geodataframe by.
    :param point_gdf: Geodataframe with point geometry. Might also be used with other geometries [not tested].
    :return: Geodataframe only containing gdf shapes in the polygon.
    """
    point_gdf['geometry'] = point_gdf['geometry'].clip(polygon_mask)
    return point_gdf.dropna(subset='geometry')


def reproject(gdf: gpd.GeoDataFrame, epsg='EPSG:3035'):
    """
    Reproject gdf to passed epsg.

    :param gdf: Input gdf.
    :param epsg: EPSG to reproject to.
    :return: Returns reprojected dataframe.
    """
    return gdf.to_crs(crs=epsg)


def points_in_aoi(interest_area, aoi_polygon, lat_lon_df, gem_shapefile):
    """
    Only return points in AOI.

    :param lat_lon_df:
    :param gem_shapefile:
    :return:
    """
    lat_lon_df = create_points_from_crs(lat_lon_df)
    point_gdf = clip_crs_by_polygon(aoi_polygon, lat_lon_df)
    point_gdf['AOI'] = interest_area
    gemeinden = reproject(gpd.GeoDataFrame.from_file(gem_shapefile))
    aoi_gemeinden = clip_crs_by_polygon(aoi_polygon, gemeinden).loc[:, ['GEN', 'geometry']]
    return point_gdf.sjoin(aoi_gemeinden, how='left', predicate='covered_by').dropna(subset='GEN')


def import_vehicle_registration_by_excel(filepath: str):
    """
    :param filepath: Path to .xlsx of vehicle registration by district
    :return: Dataframe containing registration district, amount of hybrid and full electric vehicles and the sum of both
    """

    df = pd.read_excel(filepath,
                       sheet_name=4,
                       names=['NAME', 'Insgesamt_Pkw', 'PIHybrid', 'Elektro_BEV'],
                       usecols='D, E, J, K',
                       dtype='object',
                       skiprows=8)

    df = df.dropna(how='any')
    df['EVIng'] = df['PIHybrid'] + df['Elektro_BEV']
    df['NAME'] = df['NAME'].str.slice(7, )
    df = df.sort_values('NAME')

    return df


def import_charging_pole_register(filepath: str):
    """
    :param filepath: Path to csv of charging pole register
    :return: Dataframe containing all charging poles with federal state, latitude, longitude and amount of charging points
    """
    df = pandas.read_csv(filepath,
                         sep=None,
                         header=0,
                         engine='python',
                         encoding_errors='replace',
                         on_bad_lines="warn",
                         encoding='cp1252',
                         skiprows=9)

    df = df.rename(columns=df.iloc[0]).loc[1:]

    df = df[["AOI", "Breitengrad", "Längengrad", "Anzahl Ladepunkte"]]

    df['Breitengrad'] = df['Breitengrad'].str.replace(',', '.').astype(float)
    df['Längengrad'] = df['Laengengrad'].str.replace(',', '.').astype(float)
    df['geometry'] = df.apply(lambda x: Point((float(x.Laengengrad), float(x.Breitengrad))),
                              axis=1)

    del df['Breitengrad']
    del df['Laengengrad']

    df['Anzahl Ladepunkte'] = df['Anzahl Ladepunkte'].astype('int64')
    df = df.set_crs(crs='WGS 84', epsg='EPSG:3857')
    df = df.to_crs(crs='EPSG:3035')

    return df


def read_df(path: str, cols, sep=';'): #TODO: basically same as read data from parquet. Union.
    sep = sep
    read_options = csv.ReadOptions(autogenerate_column_names=False, use_threads=True)
    parse_options = csv.ParseOptions(delimiter=sep, invalid_row_handler=invalid_row_handler)
    convert = csv.ConvertOptions(include_columns=cols)
    # TODO: optimization: value error when option string_to_bool and use_multithread is passed to_pandas()
    try:
        return csv.read_csv(path, read_options, parse_options).to_pandas()
    except FileNotFoundError:
        print(f"The file {path} you are trying to read does not exist."
              f" Set the correct filepath in 'config.json'.")
        raise FileNotFoundError


def write_pyarrow_to_csv(df, folderpath, filename, sep):
    write_options = csv.WriteOptions(delimiter=sep)
    df = pa.Table.from_pandas(df=df)
    try:
        csv.write_csv(df, concat_filepath(folderpath, filename), write_options)
    except FileNotFoundError:
        print('No folder exists at the location specified. No file has been written.')
        pass
    return


def concat_filepath(folderpath: str, aoi: str, ident='', ending='.parquet'):
    return folderpath + aoi + ident + ending


def concat_aoi_path(folderpath: str, aoi: str, aoi_type: str, ident='', ending='.parquet'):
    return folderpath + aoi + ident + "_" + aoi_type + ending


def invalid_row_handler(row):
    """
    Called by pyarrow import function
    skipping invalid rows in pyarrow table.

    :return: str "skip "
    """
    return "skip"

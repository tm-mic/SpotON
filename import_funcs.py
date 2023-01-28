import geopandas
import geopandas as gpd
import pandas
import pandas as pd
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


def read_shp(shp_path, cols: list):
    return gpd.read_file(shp_path, encoding='utf-8')[cols]


def obtain_bl_polygon(shp_path: str, bl_name: str):
    """
    Grabs a polygon from a shapefile.

    :param shp_path: Path to shapefile
    :param bl_name: identifier of the polygon to grab
    :param epsg: CRS to cast the polygon to.
    :return: One polygon from shp with x polygons.
    """
    polygon_gdf = read_shp(shp_path, ["GEN", "geometry"])
    polygon_gdf.rename(columns={'GEN': 'NAME'}, inplace=True)
    # polygon_gdf['NAME'] = polygon_gdf['NAME'].str.upper()
    # polygon_gdf['NAME'] = polygon_gdf['NAME'].str.replace('Ü', 'UE')
    # polygon_gdf['NAME'] = polygon_gdf['NAME'].str.replace('Ä', 'AE')
    # polygon_gdf['NAME'] = polygon_gdf['NAME'].str.replace('Ö', 'OE')
    # polygon_gdf = polygon_gdf.sort_values('NAME')
    # polygon_gdf = polygon_gdf.reset_index(drop=True)
    polygon_gdf = reproject(polygon_gdf)
    return polygon_gdf.where(polygon_gdf['NAME'] == bl_name).dropna()


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


# Import Raw Data in Chunks
def import_csv_chunks(filepath: str, header_list: object, type_conversion_dict=None, seperator=None, nrows=1,
                      chunks=None) -> pd.DataFrame:
    """
    Read csv to DataFrame from passed file.
    :param chunksize:
    :param type_conversion_dict: Conversion of types to reduce memory use.
    :param filepath: Path to csv file.
    :param header_list: List of headers to be read.
    :param nrows: Count of rows to import. Default "1" will import all rows of csv.
    :return: DataFrame with specified columns.
    """
    try:
        if nrows == 1:
            return pandas.read_csv(filepath, sep=seperator, header=0, usecols=header_list,
                                   engine='python', encoding_errors='replace', on_bad_lines="skip",
                                   dtype=type_conversion_dict, chunksize=chunks)
        else:
            return pandas.read_csv(filepath, sep=seperator, header=0, usecols=header_list,
                                   engine='python', encoding_errors='replace', on_bad_lines="skip", nrows=nrows,
                                   dtype=type_conversion_dict, chunksize=chunks)
    except ValueError:
        return pandas.read_csv(filepath, sep=seperator, header=0, engine='python', encoding_errors='replace',
                               on_bad_lines="skip", nrows=nrows, dtype=type_conversion_dict, chunksize=chunks)


def points_in_bundesland(bl: str, bl_shapefile, lat_lon_df, gem_shapefile):
    """
    Only return points in Bundesland.

    :param bl:
    :param bl_shapefile:
    :param lat_lon_df:
    :param gem_shapefile:
    :return:
    """
    lat_lon_df = create_points_from_crs(lat_lon_df)
    bl_polygon = obtain_bl_polygon(bl_shapefile, bl)
    point_gdf = clip_crs_by_polygon(bl_polygon, lat_lon_df)
    point_gdf['Bundesland'] = bl
    gemeinden = reproject(gpd.GeoDataFrame.from_file(gem_shapefile))
    bl_gemeinden = clip_crs_by_polygon(bl_polygon, gemeinden).loc[:, ['GEN', 'geometry']]
    return point_gdf.sjoin(bl_gemeinden, how='left', predicate='covered_by').dropna(subset='GEN')


def geo_points_from_id(id: str):
    """
    Creates points from crs ID by slicing N/E info from ID.

    :return:
    """
    north = id_to_lat_lon(id)[0]
    east = id_to_lat_lon(id)[1]
    df = pd.DataFrame(data={'x': [east], 'y': [north]})
    return gpd.points_from_xy(df['x'], df['y'], crs='EPSG:3035')


def calc_poly_extrema(polygon):
    """
    Calculates the extrema values of a polygon on one axis.
    :param polygon: Polygon or any other geometry.
    :return:
    """
    return polygon.total_bounds.tolist()


def id_to_lat_lon(id: str):
    """
    Slices string in two positions.
    Necessary to return Grid coord from ID as specified in DE_GRID ETRS89 UTM32 100m

    :param id:
    :return:
    """

    north = int(id[5:10] + '00')
    east = int(id[11:15] + '000')
    return north, east


def get_lat_lon_series(df: pd.DataFrame, north=True) -> pd.Series:
    """
    Extracts y(north) and x(east) values from coordinate IDs as specified by DE_GRID ETRS89 UTM32 100m

    :param df:
    :param north:
    :return: pd.Series of ints with
    """
    if north:
        return df.loc[:, 'Gitter_ID_100m'].apply(id_to_lat_lon).apply(lambda val: val[0])
    else:
        return df.loc[:, 'Gitter_ID_100m'].apply(id_to_lat_lon).apply(lambda val: val[1])


def check_value_in_range(input_val: int, minimum: int, maximum: int) -> bool:
    """
    Checks if an input value lies in between two given values min and max.

    :param input_val:
    :param minimum:
    :param maximum:
    :return:
    """
    if minimum < input_val < maximum:
        return True
    else:
        return False


def check_against_cond_val(input_val: int, cond_val: int) -> bool:
    """
    Checks if a given input value is smaller than the condition.

    :param input_val:
    :param cond_val:
    :return:
    """
    if cond_val > input_val:
        return True
    else:
        return False


def all_smaller(mask: pd.Series) -> bool:
    """
    Returns true if condition True applies to all rows in a pd.Series.
    Used to create mask for data import check.

    :param mask:
    :return:
    """

    if mask.all():
        return True
    else:
        return False


def all_bigger(mask: pd.Series) -> bool:
    """
    Returns False if condition True applies to all rows in a pd.Series.
    Used to create mask for data import check.

    :param mask:
    :return:
    """
    if mask.all():
        return False
    else:
        return True


def calc_coord_mask(cond_val_one: int, data: pd.DataFrame, xoy='y', unary=True, cond_val_two=None) -> pd.Series:
    """
    Creates a conditional mask(bools) based on unary x < val_one operation if unary == True. If unary == False
    creates a conditional mask (bools) based on val_one < x < val_two condition.

    :param cond_val_one: val_one for x < val_one or val_one < x < val_two check
    :param data: df
    :param xoy: to check with x or y value
    :param cond_val_two: second value for val_one < x < val_two operation
    :return: pd.Series of bool values
    """
    if unary:
        if xoy == 'y':
            mask_vals = get_lat_lon_series(data, north=True)
        else:
            mask_vals = get_lat_lon_series(data, north=False)
        return mask_vals.apply(check_against_cond_val, args=(cond_val_one,))
    else:
        if xoy == 'y':
            mask_vals = get_lat_lon_series(data, north=True)
        else:
            mask_vals = get_lat_lon_series(data, north=False)
        return mask_vals.apply(check_value_in_range, args=(cond_val_one, cond_val_two,))


def ID_in_poly(poly_extrema: list, data):
    """
    Gives back data which fall into the rectangle bounded by polygon extrema values [xmin, ymin, xmax, ymax].
    X values and Y values are calculated based the Gitter_ID_100m. Only Data that do fall into the range
    xmin - xmax and ymin - ymax are returned in the result dataframe.

    :param df: df to append to
    :param data: data that are being imported
    :param poly_extrema: extrema points of a polygon
    :param data_columns: columns to return
    :return: DataFrame of data inside a max polygon bounding box.
    """
    north_max = poly_extrema[3]
    north_min = poly_extrema[1]
    east_min = poly_extrema[0]
    east_max = poly_extrema[2]

    north_mask = calc_coord_mask(north_min, data, unary=False, xoy="y", cond_val_two=north_max)
    if north_mask.any():
        data.drop(data[~north_mask].index, inplace=True)
        east_mask = calc_coord_mask(east_min, data, unary=False, xoy="x", cond_val_two=east_max)
        if east_mask.any():
            data.drop(data[~east_mask].index, inplace=True)
            return True, data
        else:
            return False, None
    else:
        return False, None


def data_poly_import(filepath: str, data_columns: list, poly_extrema: list, chunks: int, conversion: dict):
    result: pd.DataFrame = pd.DataFrame()
    c = 0
    while True:
        imported_data = pd.read_csv(filepath, usecols=data_columns, engine='python', encoding_errors='replace',
                                    on_bad_lines="skip", chunksize=chunks, sep=r'(,|;)', dtype=conversion)

        for data in imported_data:
            in_poly = ID_in_poly(poly_extrema, data)

            if in_poly[0]:
                result = result.append(in_poly[1])
                c += 1
                print(
                    f'\n{c} chunks of {filepath} have been added to the dataframe so far. \nThe current df has a size of {result.shape}')

            north_min_mask = calc_coord_mask(poly_extrema[1], data, xoy='y')
            north_max_mask = calc_coord_mask(poly_extrema[3], data, xoy='y')

            if all_smaller(north_min_mask):
                print("None of imported data in range of Polygon.")
                continue

            if all_bigger(north_max_mask):
                print("This part of the dataset is north of the given Polygon. Data import is stopped.")
                break

            else:
                continue

        break
    return result


def converter(df, col='Anzahl'):
    return pd.to_numeric(df.loc[:, col], errors='coerce', downcast='integer')


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

    df = df[["Bundesland", "Breitengrad", "Längengrad", "Anzahl Ladepunkte"]]

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

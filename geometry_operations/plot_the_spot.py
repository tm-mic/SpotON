# Responsible for Module: Max W.; written with support by Christian, Thomas
# Plots with folium and saves the plot as html file.


import folium
import geopandas as gpd
from folium.plugins import MarkerCluster


def plot_folium_map_from_GeoDataFrames(parking_areas_of_intr, aoi_polygon, interest_area: str):
    """
    Generates a folium html file with area of interest layer and all parking areas which
    are of interest for placing new ladesaeulen. The map is centered on the aoi_polygon.

    :param parking_areas_of_intr: GeoDataFrame with all parking areas which will get ladesaeulen in
            the area of interest
    :param aoi_polygon: GeoDataFrame with the polygon of a gemeinde, landkreis, zulassungsbezirk or bundesland
    :param interest_area: A folium map saved as html.
    :return: html file (folium map in OSM)
    """

    aoi_polygon = gpd.GeoDataFrame(index=[0], crs='epsg:3035', geometry=[aoi_polygon])
    aoi_polygon = aoi_polygon.to_crs(epsg=4326)
    aoi_polygon_copy = aoi_polygon.copy()
    aoi_polygon_copy['geometry'] = aoi_polygon_copy['geometry'].centroid
    aoi_polygon_copy['lon'] = aoi_polygon_copy['geometry'].x
    aoi_polygon_copy['lat'] = aoi_polygon_copy['geometry'].y
    folium_map = folium.Map(location=[aoi_polygon_copy['lat'], aoi_polygon_copy['lon']], tiles="OpenStreetMap", zoom_start=10)

    parking_areas_of_intr = parking_areas_of_intr.to_crs(epsg=4326)
    parking_areas_of_intr_copy = parking_areas_of_intr.copy()
    parking_areas_of_intr_copy['geometry'] = parking_areas_of_intr_copy['geometry'].centroid
    parking_areas_of_intr_copy['lon'] = parking_areas_of_intr_copy['geometry'].x
    parking_areas_of_intr_copy['lat'] = parking_areas_of_intr_copy['geometry'].y

    marker_cluster = MarkerCluster(name="Cluster Marker").add_to(folium_map)
    tooltip = "Click polygon for more information."
    for row in parking_areas_of_intr_copy.itertuples():
        folium.Marker(location=[row.lat, row.lon], tooltip=tooltip).add_to(marker_cluster)

    folium.Choropleth(
        geo_data=parking_areas_of_intr,
        name="Parkflächen Polygone",
        data=parking_areas_of_intr,
        columns=["id", "ladesaeulen"],
        key_on="feature.properties.id",
        fill_color='RdPu',
        fill_opacity=0.7,
        line_opacity=1,
        line_color='blue',
        legend_name="Ladesäulen pro Parkfläche",
        show=False
    ).add_to(folium_map)

    folium.features.GeoJson(parking_areas_of_intr, name='Parkflächen Polygone Informationen', show=False,
                            popup=folium.features.GeoJsonPopup(fields=['NAME_Gemeinde','value', 'area', 'parking_spots',
                                                                       'ladesaeulen'])).add_to(folium_map)

    folium.GeoJson(data=aoi_polygon['geometry'], name='Area of Interest', style_function=lambda x: {'fillColor': 'None'})\
        .add_to(folium_map)

    folium.LayerControl().add_to(folium_map)

    interest_area = interest_area+".html"
    folium_map.save(interest_area)
    return 0




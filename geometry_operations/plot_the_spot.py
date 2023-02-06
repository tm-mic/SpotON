# Responsible for Module: Max W.; written with support by Christian, Thomas
# Plots with folium and saves the plot as html file.


import folium

def plot_folium_map_from_GeoDataFrames(parking_areas_of_intr, GeoDataFrame1, filename_with_html: str):
    """
    Generates a folium html file with administration area border layer and all parking areas which
    are of interest for placing new ladesaeulen. The map is centered on the administration area.

    :param parking_areas_of_intr: GeoDataFrame with all parking areas which will get ladesaeulen in
            the administration area
    :param GeoDataFrame1: GeoDataFrame with the polygon of a gemeinde, zulassungsbezirk or bundesland
            (adminstration area)
    :param filename_with_html: A folium map saved as html.
    :return: filename_with_html.html (folium map in OSM)
    """
    parking_areas_of_intr = parking_areas_of_intr.to_crs(epsg=4326)
    GeoDataFrame1 = GeoDataFrame1.to_crs(epsg=4326)
    gemeindecopy = GeoDataFrame1.copy()
    gemeindecopy['geometry'] = gemeindecopy['geometry'].centroid
    gemeindecopy['lon'] = gemeindecopy['geometry'].x
    gemeindecopy['lat'] = gemeindecopy['geometry'].y
    folium_map = folium.Map(location=[gemeindecopy['lat'], gemeindecopy['lon']], tiles="OpenStreetMap", zoom_start=10)
    folium.Choropleth(
        geo_data=parking_areas_of_intr,
        name="Ladesäulen",
        data=parking_areas_of_intr,
        columns=["id", "ladesaeulen"],
        key_on="feature.properties.id",
        fill_color='RdPu',
        fill_opacity=0.7,
        line_opacity=1,
        line_color='red',
        legend_name="Ladesäulen pro Parkfläche"
    ).add_to(folium_map)
    folium.features.GeoJson(parking_areas_of_intr, name='Ladesäulen', popup=folium.features.GeoJsonPopup(
        fields=['NAME_Gemeinde','value', 'area', 'parking_spots', 'ladesaeulen'])).add_to(folium_map)
    folium.GeoJson(data=GeoDataFrame1['geometry'], name='Gemeindeumriss', style_function=lambda x: {'fillColor': 'None'})\
        .add_to(folium_map)
    folium.LayerControl().add_to(folium_map)
    folium_map.save(filename_with_html)
    return 0




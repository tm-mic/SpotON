# Responsible for Module: Max W.; written with support by Christian, Thomas
# Holds the list of paths and values for the point of interest layers and street layers.

# 1. leisure: fitness_centre, park, pitch, sports_centre, water_park, sports_hall
# 2. landuse1: commercial, industrial
# 3. landuse2: allotments, cemetery
# 4. food:cafe, restaurant, fast_food
# 5. health: place_of_worship, hospital, dentist, doctors
# 6. retail: retail
# 7. appres1: app2-5, res3-5
# 8. appres2: app6-13, res6-8

import json

file = open('config_mce_weights_and_buffer.json')
weights = json.loads(file.read())

"""Weighted values of the layers. All are determined by MCE."""
leisure = weights['leisure']
landuse1 = weights['landuse1']
landuse2 = weights['landuse2']
food = weights['food']
health = weights['health']
retail = weights['retail']
appres1 = weights['appres1']
appres2 = weights['appres2']

motorway1 = weights['motorway1']
trunks1 = weights['trunks1']
primary1 = weights['primary1']
secondary1 = weights['secondary1']
tertiary1 = weights['tertiary1']

motorway2 = weights['motorway2']
trunks2 = weights['trunks2']
primary2 = weights['primary2']
secondary2 = weights['secondary2']
tertiary2 = weights['tertiary2']

"""Dicts with paths for the different layers and there values."""
poi = {r"C:\Users\maxwa\Documents\Universität\Master\Wintersemester 2022\KINF Projekt\osm daten\poi\leisure\slim\slim_pitch.geojson": leisure,
       r"C:\Users\maxwa\Documents\Universität\Master\Wintersemester 2022\KINF Projekt\osm daten\poi\leisure\slim\slim_fitness_centre.geojson": leisure,
       r"C:\Users\maxwa\Documents\Universität\Master\Wintersemester 2022\KINF Projekt\osm daten\poi\leisure\slim\slim_park.geojson": leisure,
       r"C:\Users\maxwa\Documents\Universität\Master\Wintersemester 2022\KINF Projekt\osm daten\poi\leisure\slim\slim_sports_centre.geojson":leisure,
       r"C:\Users\maxwa\Documents\Universität\Master\Wintersemester 2022\KINF Projekt\osm daten\poi\leisure\slim\slim_water_park.geojson":leisure,
       r"C:\Users\maxwa\Documents\Universität\Master\Wintersemester 2022\KINF Projekt\osm daten\poi\building\slim\slim_sports_hall.geojson":leisure,
       r"C:\Users\maxwa\Documents\Universität\Master\Wintersemester 2022\KINF Projekt\osm daten\poi\landuse\slim\slim_commercial.geojson": landuse1,
       r"C:\Users\maxwa\Documents\Universität\Master\Wintersemester 2022\KINF Projekt\osm daten\poi\landuse\slim\slim_industrial.geojson":landuse1,
       r"C:\Users\maxwa\Documents\Universität\Master\Wintersemester 2022\KINF Projekt\osm daten\poi\landuse\slim\slim_cemetery.geojson":landuse2,
       r"C:\Users\maxwa\Documents\Universität\Master\Wintersemester 2022\KINF Projekt\osm daten\poi\landuse\slim\slim_allotments.geojson":landuse2,
       r"C:\Users\maxwa\Documents\Universität\Master\Wintersemester 2022\KINF Projekt\osm daten\poi\amenity\slim\slim_cafe.geojson": food,
       r"C:\Users\maxwa\Documents\Universität\Master\Wintersemester 2022\KINF Projekt\osm daten\poi\amenity\slim\slim_fast_food.geojson": food,
       r"C:\Users\maxwa\Documents\Universität\Master\Wintersemester 2022\KINF Projekt\osm daten\poi\amenity\slim\slim_restaurant.geojson": food,
       r"C:\Users\maxwa\Documents\Universität\Master\Wintersemester 2022\KINF Projekt\osm daten\poi\amenity\slim\slim_dentist.geojson": health,
       r"C:\Users\maxwa\Documents\Universität\Master\Wintersemester 2022\KINF Projekt\osm daten\poi\amenity\slim\slim_doctors.geojson":health,
       r"C:\Users\maxwa\Documents\Universität\Master\Wintersemester 2022\KINF Projekt\osm daten\poi\amenity\slim\slim_place_of_worship.geojson":health,
       r"C:\Users\maxwa\Documents\Universität\Master\Wintersemester 2022\KINF Projekt\osm daten\poi\building\slim\slim_hospital.geojson":health,
       r"C:\Users\maxwa\Documents\Universität\Master\Wintersemester 2022\KINF Projekt\osm daten\poi\building\slim\slim_retail.geojson": retail,
       r"C:\Users\maxwa\Documents\Universität\Master\Wintersemester 2022\KINF Projekt\osm daten\poi\building\slim\slim_apartments2.geojson": appres1,
       r"C:\Users\maxwa\Documents\Universität\Master\Wintersemester 2022\KINF Projekt\osm daten\poi\building\slim\slim_apartments3.geojson": appres1,
       r"C:\Users\maxwa\Documents\Universität\Master\Wintersemester 2022\KINF Projekt\osm daten\poi\building\slim\slim_apartments4.geojson": appres1,
       r"C:\Users\maxwa\Documents\Universität\Master\Wintersemester 2022\KINF Projekt\osm daten\poi\building\slim\slim_apartments5.geojson": appres1,
       r"C:\Users\maxwa\Documents\Universität\Master\Wintersemester 2022\KINF Projekt\osm daten\poi\building\slim\slim_residential345.geojson": appres1,
       r"C:\Users\maxwa\Documents\Universität\Master\Wintersemester 2022\KINF Projekt\osm daten\poi\building\slim\slim_apartments6.geojson": appres2,
       r"C:\Users\maxwa\Documents\Universität\Master\Wintersemester 2022\KINF Projekt\osm daten\poi\building\slim\slim_apartments7.geojson":appres2,
       r"C:\Users\maxwa\Documents\Universität\Master\Wintersemester 2022\KINF Projekt\osm daten\poi\building\slim\slim_apartments8910.geojson":appres2,
       r"C:\Users\maxwa\Documents\Universität\Master\Wintersemester 2022\KINF Projekt\osm daten\poi\building\slim\slim_apartments111213.geojson":appres2,
       r"C:\Users\maxwa\Documents\Universität\Master\Wintersemester 2022\KINF Projekt\osm daten\poi\building\slim\slim_residential678.geojson": appres2
       }

streets1 = {r"C:\Users\maxwa\Documents\Universität\Master\Wintersemester 2022\KINF Projekt\osm daten\streets\slim\slim_motorway.geojson": motorway1,
            r"C:\Users\maxwa\Documents\Universität\Master\Wintersemester 2022\KINF Projekt\osm daten\streets\slim\slim_trunks.geojson": trunks1,
            r"C:\Users\maxwa\Documents\Universität\Master\Wintersemester 2022\KINF Projekt\osm daten\streets\slim\slim_primary.geojson": primary1,
            r"C:\Users\maxwa\Documents\Universität\Master\Wintersemester 2022\KINF Projekt\osm daten\streets\secondary\slim\slim_bavariasecondary.geojson": secondary1,
            r"C:\Users\maxwa\Documents\Universität\Master\Wintersemester 2022\KINF Projekt\osm daten\streets\secondary\slim\slim_bawuesecondary.geojson": secondary1,
            r"C:\Users\maxwa\Documents\Universität\Master\Wintersemester 2022\KINF Projekt\osm daten\streets\secondary\slim\slim_berlinsecondary.geojson": secondary1,
            r"C:\Users\maxwa\Documents\Universität\Master\Wintersemester 2022\KINF Projekt\osm daten\streets\secondary\slim\slim_brandenburgsecondary.geojson": secondary1,
            r"C:\Users\maxwa\Documents\Universität\Master\Wintersemester 2022\KINF Projekt\osm daten\streets\secondary\slim\slim_bremensecondary.geojson": secondary1,
            r"C:\Users\maxwa\Documents\Universität\Master\Wintersemester 2022\KINF Projekt\osm daten\streets\secondary\slim\slim_hamburgsecondary.geojson": secondary1,
            r"C:\Users\maxwa\Documents\Universität\Master\Wintersemester 2022\KINF Projekt\osm daten\streets\secondary\slim\slim_hessensecondary.geojson": secondary1,
            r"C:\Users\maxwa\Documents\Universität\Master\Wintersemester 2022\KINF Projekt\osm daten\streets\secondary\slim\slim_niedersachsensecondary.geojson": secondary1,
            r"C:\Users\maxwa\Documents\Universität\Master\Wintersemester 2022\KINF Projekt\osm daten\streets\secondary\slim\slim_nrwsecondary.geojson": secondary1,
            r"C:\Users\maxwa\Documents\Universität\Master\Wintersemester 2022\KINF Projekt\osm daten\streets\secondary\slim\slim_pommernsecondary.geojson": secondary1,
            r"C:\Users\maxwa\Documents\Universität\Master\Wintersemester 2022\KINF Projekt\osm daten\streets\secondary\slim\slim_rheinsecondary.geojson": secondary1,
            r"C:\Users\maxwa\Documents\Universität\Master\Wintersemester 2022\KINF Projekt\osm daten\streets\secondary\slim\slim_saarlandsecondary.geojson": secondary1,
            r"C:\Users\maxwa\Documents\Universität\Master\Wintersemester 2022\KINF Projekt\osm daten\streets\secondary\slim\slim_sachsenanhaltsecondary.geojson": secondary1,
            r"C:\Users\maxwa\Documents\Universität\Master\Wintersemester 2022\KINF Projekt\osm daten\streets\secondary\slim\slim_sachsensecondary.geojson": secondary1,
            r"C:\Users\maxwa\Documents\Universität\Master\Wintersemester 2022\KINF Projekt\osm daten\streets\secondary\slim\slim_schleswigholsteinsecondary.geojson": secondary1,
            r"C:\Users\maxwa\Documents\Universität\Master\Wintersemester 2022\KINF Projekt\osm daten\streets\secondary\slim\slim_thueringensecondary.geojson": secondary1,
            r"C:\Users\maxwa\Documents\Universität\Master\Wintersemester 2022\KINF Projekt\osm daten\streets\tertiary\slim\slim_bavariatertiary.geojson": tertiary1,
            r"C:\Users\maxwa\Documents\Universität\Master\Wintersemester 2022\KINF Projekt\osm daten\streets\tertiary\slim\slim_bawuetertiary.geojson": tertiary1,
            r"C:\Users\maxwa\Documents\Universität\Master\Wintersemester 2022\KINF Projekt\osm daten\streets\tertiary\slim\slim_berlintertiary.geojson": tertiary1,
            r"C:\Users\maxwa\Documents\Universität\Master\Wintersemester 2022\KINF Projekt\osm daten\streets\tertiary\slim\slim_brandenburgtertiary.geojson": tertiary1,
            r"C:\Users\maxwa\Documents\Universität\Master\Wintersemester 2022\KINF Projekt\osm daten\streets\tertiary\slim\slim_brementertiary.geojson": tertiary1,
            r"C:\Users\maxwa\Documents\Universität\Master\Wintersemester 2022\KINF Projekt\osm daten\streets\tertiary\slim\slim_hamburgtertiary.geojson": tertiary1,
            r"C:\Users\maxwa\Documents\Universität\Master\Wintersemester 2022\KINF Projekt\osm daten\streets\tertiary\slim\slim_hessentertiary.geojson": tertiary1,
            r"C:\Users\maxwa\Documents\Universität\Master\Wintersemester 2022\KINF Projekt\osm daten\streets\tertiary\slim\slim_niedersachsentertiary.geojson": tertiary1,
            r"C:\Users\maxwa\Documents\Universität\Master\Wintersemester 2022\KINF Projekt\osm daten\streets\tertiary\slim\slim_nrwtertiary.geojson": tertiary1,
            r"C:\Users\maxwa\Documents\Universität\Master\Wintersemester 2022\KINF Projekt\osm daten\streets\tertiary\slim\slim_pommerntertiary.geojson": tertiary1,
            r"C:\Users\maxwa\Documents\Universität\Master\Wintersemester 2022\KINF Projekt\osm daten\streets\tertiary\slim\slim_rheintertiary.geojson": tertiary1,
            r"C:\Users\maxwa\Documents\Universität\Master\Wintersemester 2022\KINF Projekt\osm daten\streets\tertiary\slim\slim_saarlandtertiary.geojson": tertiary1,
            r"C:\Users\maxwa\Documents\Universität\Master\Wintersemester 2022\KINF Projekt\osm daten\streets\tertiary\slim\slim_sachsenanhalttertiary.geojson": tertiary1,
            r"C:\Users\maxwa\Documents\Universität\Master\Wintersemester 2022\KINF Projekt\osm daten\streets\tertiary\slim\slim_sachsentertiary.geojson": tertiary1,
            r"C:\Users\maxwa\Documents\Universität\Master\Wintersemester 2022\KINF Projekt\osm daten\streets\tertiary\slim\slim_schleswigholsteintertiary.geojson": tertiary1,
            r"C:\Users\maxwa\Documents\Universität\Master\Wintersemester 2022\KINF Projekt\osm daten\streets\tertiary\slim\slim_thueringentertiary.geojson": tertiary1
            }

streets2 = {r"C:\Users\maxwa\Documents\Universität\Master\Wintersemester 2022\KINF Projekt\osm daten\streets\slim\slim_motorway.geojson": motorway2,
            r"C:\Users\maxwa\Documents\Universität\Master\Wintersemester 2022\KINF Projekt\osm daten\streets\slim\slim_trunks.geojson": trunks2,
            r"C:\Users\maxwa\Documents\Universität\Master\Wintersemester 2022\KINF Projekt\osm daten\streets\slim\slim_primary.geojson": primary2,
            r"C:\Users\maxwa\Documents\Universität\Master\Wintersemester 2022\KINF Projekt\osm daten\streets\secondary\slim\slim_bavariasecondary.geojson": secondary2,
            r"C:\Users\maxwa\Documents\Universität\Master\Wintersemester 2022\KINF Projekt\osm daten\streets\secondary\slim\slim_bawuesecondary.geojson": secondary2,
            r"C:\Users\maxwa\Documents\Universität\Master\Wintersemester 2022\KINF Projekt\osm daten\streets\secondary\slim\slim_berlinsecondary.geojson": secondary2,
            r"C:\Users\maxwa\Documents\Universität\Master\Wintersemester 2022\KINF Projekt\osm daten\streets\secondary\slim\slim_brandenburgsecondary.geojson": secondary2,
            r"C:\Users\maxwa\Documents\Universität\Master\Wintersemester 2022\KINF Projekt\osm daten\streets\secondary\slim\slim_bremensecondary.geojson": secondary2,
            r"C:\Users\maxwa\Documents\Universität\Master\Wintersemester 2022\KINF Projekt\osm daten\streets\secondary\slim\slim_hamburgsecondary.geojson": secondary2,
            r"C:\Users\maxwa\Documents\Universität\Master\Wintersemester 2022\KINF Projekt\osm daten\streets\secondary\slim\slim_hessensecondary.geojson": secondary2,
            r"C:\Users\maxwa\Documents\Universität\Master\Wintersemester 2022\KINF Projekt\osm daten\streets\secondary\slim\slim_niedersachsensecondary.geojson": secondary2,
            r"C:\Users\maxwa\Documents\Universität\Master\Wintersemester 2022\KINF Projekt\osm daten\streets\secondary\slim\slim_nrwsecondary.geojson": secondary2,
            r"C:\Users\maxwa\Documents\Universität\Master\Wintersemester 2022\KINF Projekt\osm daten\streets\secondary\slim\slim_pommernsecondary.geojson": secondary2,
            r"C:\Users\maxwa\Documents\Universität\Master\Wintersemester 2022\KINF Projekt\osm daten\streets\secondary\slim\slim_rheinsecondary.geojson": secondary2,
            r"C:\Users\maxwa\Documents\Universität\Master\Wintersemester 2022\KINF Projekt\osm daten\streets\secondary\slim\slim_saarlandsecondary.geojson": secondary2,
            r"C:\Users\maxwa\Documents\Universität\Master\Wintersemester 2022\KINF Projekt\osm daten\streets\secondary\slim\slim_sachsenanhaltsecondary.geojson": secondary2,
            r"C:\Users\maxwa\Documents\Universität\Master\Wintersemester 2022\KINF Projekt\osm daten\streets\secondary\slim\slim_sachsensecondary.geojson": secondary2,
            r"C:\Users\maxwa\Documents\Universität\Master\Wintersemester 2022\KINF Projekt\osm daten\streets\secondary\slim\slim_schleswigholsteinsecondary.geojson": secondary2,
            r"C:\Users\maxwa\Documents\Universität\Master\Wintersemester 2022\KINF Projekt\osm daten\streets\secondary\slim\slim_thueringensecondary.geojson": secondary2,
            r"C:\Users\maxwa\Documents\Universität\Master\Wintersemester 2022\KINF Projekt\osm daten\streets\tertiary\slim\slim_bavariatertiary.geojson": tertiary2,
            r"C:\Users\maxwa\Documents\Universität\Master\Wintersemester 2022\KINF Projekt\osm daten\streets\tertiary\slim\slim_bawuetertiary.geojson": tertiary2,
            r"C:\Users\maxwa\Documents\Universität\Master\Wintersemester 2022\KINF Projekt\osm daten\streets\tertiary\slim\slim_berlintertiary.geojson": tertiary2,
            r"C:\Users\maxwa\Documents\Universität\Master\Wintersemester 2022\KINF Projekt\osm daten\streets\tertiary\slim\slim_brandenburgtertiary.geojson": tertiary2,
            r"C:\Users\maxwa\Documents\Universität\Master\Wintersemester 2022\KINF Projekt\osm daten\streets\tertiary\slim\slim_brementertiary.geojson": tertiary2,
            r"C:\Users\maxwa\Documents\Universität\Master\Wintersemester 2022\KINF Projekt\osm daten\streets\tertiary\slim\slim_hamburgtertiary.geojson": tertiary2,
            r"C:\Users\maxwa\Documents\Universität\Master\Wintersemester 2022\KINF Projekt\osm daten\streets\tertiary\slim\slim_hessentertiary.geojson": tertiary2,
            r"C:\Users\maxwa\Documents\Universität\Master\Wintersemester 2022\KINF Projekt\osm daten\streets\tertiary\slim\slim_niedersachsentertiary.geojson": tertiary2,
            r"C:\Users\maxwa\Documents\Universität\Master\Wintersemester 2022\KINF Projekt\osm daten\streets\tertiary\slim\slim_nrwtertiary.geojson": tertiary2,
            r"C:\Users\maxwa\Documents\Universität\Master\Wintersemester 2022\KINF Projekt\osm daten\streets\tertiary\slim\slim_pommerntertiary.geojson": tertiary2,
            r"C:\Users\maxwa\Documents\Universität\Master\Wintersemester 2022\KINF Projekt\osm daten\streets\tertiary\slim\slim_rheintertiary.geojson": tertiary2,
            r"C:\Users\maxwa\Documents\Universität\Master\Wintersemester 2022\KINF Projekt\osm daten\streets\tertiary\slim\slim_saarlandtertiary.geojson": tertiary2,
            r"C:\Users\maxwa\Documents\Universität\Master\Wintersemester 2022\KINF Projekt\osm daten\streets\tertiary\slim\slim_sachsenanhalttertiary.geojson": tertiary2,
            r"C:\Users\maxwa\Documents\Universität\Master\Wintersemester 2022\KINF Projekt\osm daten\streets\tertiary\slim\slim_sachsentertiary.geojson": tertiary2,
            r"C:\Users\maxwa\Documents\Universität\Master\Wintersemester 2022\KINF Projekt\osm daten\streets\tertiary\slim\slim_schleswigholsteintertiary.geojson": tertiary2,
            r"C:\Users\maxwa\Documents\Universität\Master\Wintersemester 2022\KINF Projekt\osm daten\streets\tertiary\slim\slim_thueringentertiary.geojson": tertiary2}
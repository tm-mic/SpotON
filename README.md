**ÖLS-Standordsuche Gruppe A - Max W., Thomas M., Christian L.**
***
## SpotOn
*SpotOn* is a data analysis script that infers the position of ev charging points based on 
demographic zensus data and a multi-criteria analysis (AHP) of promising parking spot candidates. It is flexible in which aoi it processes.
The analysis done closely relates to the factual planning practices in Germany.
It is based on several assumptions:
1) Gemeinden are politically and legally responsible for the construction of ev charging points.
2) The likelihood of spatial ev distribution is based on the demographic structure of a region.
3) The spatial distribution of ev's can be expressed in a ratio based index.
4) Parking spots are prime candidates for the erection of ev charging points.
5) EV charging points are erected in "bulk". Meaning if a parking spot is identified as fitting all available spots will be erected there before other parking spots receive charging points.

SpotOn uses following data as specified in the IandO/config.json. 
- Familie
- Gebäude
- Haushalte
- Bevölkerung
- KFZ Data (on Zulassungsbezirke level)
- GeoJson with results of AHP analysis
- register of existing charging poles
- shapefiles:
    - Zulassungsbezirke DE
    - Gemeinden DE
    - Bundesländer DE

*As of version 0.1 SpotOn is tested to work with the data from following sources:*

- KFZ Data: https://gdz.bkg.bund.de/index.php/default/open-data/kfz-kennzeichen-1-250-000-kfz250.html
- Zensus Data: https://www.zensus2011.de/DE/Home/Aktuelles/DemografischeGrunddaten.html;jsessionid=0AB671F77E2D8760E5E3F6697ED156EC.live292?nn=559100
- VG5000_GEM: https://gdz.bkg.bund.de/index.php/default/open-data/verwaltungsgebiete-1-5-000-000-stand-01-01-vg5000-01-01.html
- VG2500_LAN:https://gdz.bkg.bund.de/index.php/default/open-data/verwaltungsgebiete-1-2-500-000-stand-31-12-vg2500-12-31.html
- Ladesaeulenregister_CSV: https://www.bundesnetzagentur.de/DE/Fachthemen/ElektrizitaetundGas/E-Mobilitaet/Ladesaeulenkarte/start.html

(Since the start of the project the bundesnetzagentur updated the Ladesaeulenregister_CSV file but in a different file format.
Our function is only working for csv.)

On a high level the algorithm for the calculation of the ev charging points can be described as follows:
- demographic data is imported, disaggregated, and regrouped
- groups are weighted
- a weighted index f.e. 100mx100m cell is calculated
- the index is summed to form a Gemeinde-Index
- the set of cars specified on ZBA level are distributed based on the normalized Gemeinde-index
- valid promising candidates (parking spots) are weighted and grouped
- the difference (delta) between the expansion goal for ev charging points and the existing charging points for each Gemeinde are calculated
- delta is used to distribute as many ev charging spots to parking area in bulk starting with the parking spot with the highest value 

## Installation and Running SpotOn
There is no standard installation process. 

To use SpotOn:
- Clone and pull the repo
- Open the repo in your python IDE
- add the necessary data (see data list above or check the config.json) to the empty folder /data/ in your local repo 
- run the .main.py file.

In the end a .csv file with the potential ev charging points for each parking spot is written to the results folder. Further, an <aoi name>.html file is written to the results folder. This file is the visualization of the .csv file.

## Roadmap
The project will remain in the state it is right now. Feel free to clone and make it your own.

## Contributing
The project is not open for contribution.

## Project status
Smaller issues arrised during development process. Especially runtime issues for large aois persist in this version. Abstraction for other data files (such as Zensus 2023) is not implemented.
In the late stages of the project we realised that our import function for the Ladesaeulenregister_CSV ist not working for Mac Users.
Sadly we had not enough time to fix this problem. Furthermore, Gemeinden is still listed as possible input at beginning of the program, 
but the selection of a Gemeinde will throw an error. This is due to the fact that the Gemeinde shapefile is necessary reference for the index.    
Lastly UserWarnings and FutureWarnings are supressed, but this does not affect the functionality of the program.
 
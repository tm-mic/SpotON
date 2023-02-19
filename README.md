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
**insert file links**

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

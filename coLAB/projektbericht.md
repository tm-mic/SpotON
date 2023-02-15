## Präambel
*SpotOn* is a data analysis script that infers the position of ev charging points based on 
demographic zensus data and a multi-criteria analysis of promising parking spot candidates. It is flexible in which aoi it processes.
The analysis done closely relates to the factual planning practices in Germany.
It is based on several assumptions:
1) Gemeinden are politically and legally responsible for the construction of ev charging points.
2) The likelihood of spatial ev distribution is based on the demographic structure of a region.
3) The spatial distribution of ev's can be expressed in a ratio based index.
4) Parking spots are prime candidates for the erection of ev charging points.
5) EV charging points are erected in "bulk". Meaning if a parking spot is identified as fitting all 
   available spots will be erected there before other parking spots receive charging points.

SpotOn uses following data basis as specified in the config.json. 
- Familie
- Gebäude
- Haushalte
- Bevölkerung
- KFZ Data (on Zulassungsbezirke level)\

*As of version 0.1 SpotOn is tested to work with the data sets from the 2011 Zensus data in CSV 
format. These can be found here:*
**insert file links**

On a high level the algorithm for the calculation of the ev charging points can be described as follows:
- demographic data is imported, disaggregated, and regrouped
- groups are weighted
- a weighted index f.e. 100mx100m cell is calculated
- the index is summed to form a Gemeinde-Index
- the set of cars specified on ZBA level are distributed based on the normalized index
- valid promising candidates (parking spots) are weighted and grouped
- the difference (phi) between the expansion goal for ev charging points and the existing charging points for each Gemeinde are calculated
- phi is used to distribute as many ev charging spots to parking area in bulk starting with the parking spot with the highest value 

## Conceptual Challenges and Algorithmic Solutions
### Cell Index Calculation (Christian)
Distributing ev from the administrative zba level to the gemeinde level based on raw data on 
100x100m  cell level is the key conceptual problem to solve in order to infer demand for ev 
charging  poles in a given region. Ev distribution is infered by demographic data and number of 
households.  An index based on ratios is created to allow for ev distribution on gemeinde level. 
The index is (conceptually) calculated in 1+6 steps

0) Data provided in the demographic data sets are mapped to geometries. (C)
The data provided comes with an id. This id implies the cell geometry middle point.  In order to 
allow for the data to be spatially interpreted it is necessary to map the results from the
index calculations to these geometries. However, calculating the geometries on the large scale demographic dataframes is slow. 

Therefore, the geometries are calculated on runtime from the cell x-y values pulled from the 
zensus  data set. The point geodataframe is clipped by the shapefile area of interest polygon. 
In order to run attribute merges and joins on Gemeinde Ebene the gemeinden inside the area of  
interest are calculated and joined (covered_by) to the geometry reference geodataframe.

This step is done at the beginning of the program to allow for attribute merges, joins, etc 
during runtime and reduce the needs for geometric operations on large dataframes.

1) Non-conforming age data grouping in the raw data is dissolved based on a 
   pre-defined age group mapping. (C)
Age data on cell level is encoded in the raw data in two different attribute groupings: Alter_Kurz & Alter_10_JG. These age groupings do overlap and are not exclusively used for each cell. Hence, age observations are "doubled" or the necessary detail grouping is not provided. Also Alter_Kurz does not provide the information density necessary. It is important to note that age is a key factor for spatial ev distribution.

During runtime the data is being disaggregated. Based on a mapping between Alter_Kurz and Alter_10_JG, Alter_Kurz age groups are redistributed into Alter_10_JG age groups. To do so a random splitter value ranging from 0.4 - 0.6 is assumed and the number of observations in Alter_Kurz is split into corresponding Alter_10_JG age groups. Original Alter_10_JG age group obersavtions are retained. Only non existing Alter_10_JG age groups are appended to the resulting data frame. 

This widens the data basis and allows to draw some information from the mixed use of different non conforming age groupings. However, the redistribution relies on the assumption, that age groups described in the Alter_Kurz are more or less evenly distributed matching their pair of Alter_10JG age groups (between 0.4 - 0.6).

2) Attribute groupings are remapped to match group-value specifications. (C)
The raw data attribute observations are grouped as described in the metadata. It is clear, however, that only certain groups carry information to be sensically interpreted (weighted) when trying to distribute evs spatially based on an index. Hence, groups are remapped to match the groups specified in the config.json.

3) Missing data values are infered by median aggregates on the gemeinde level. (C)
For many cells there are observations (Insgesamt > 0), however, no attributes are listed in the dataset. This is crucial as observations have been made but there is no qualification of these in the raw dataset. In tune with our approach to redistribute ev s and ev charging poles on the gemeinde level median values for each attribute-code combination and each Gemeinde are calculated and missing values are added to the cells of a gemeinde. These "filler" values allow to add a 'baseline' for each Gemeinde, hence, making Gemeinden more comparable. The filler values are summed into the cell index.

The median value for each attribute-code combination is calculated.
The median value is multiplied by the share (number of occurences in the Gemeinde / total number of observations in the Gemeinde) of the corresponding attribute.

The median value is calculated as *sum of all occurences of the attribute-code in the gemeinde* divided by the count of the *occurences of the attribute-code in the gemeinde*.

Multiplying the attribute-code value with the share of the attribute serves as a correcting factor. It helps to avoid over or understating the importance of an attribute as it is unclear how often, in absolute terms, it actually occurs in the Gemeinde.

4) Cell Indices are multiplied by the number of households in the cell.
5) Ratios are aggregated to create cell indices. (C)
6) Standard normalization (linear casting values between 0-1 based on min and max values)is used to create comparability between spatial areas. (C)*

### Other related problems:
1) #### Flexibilit
Allowing to choose different area of interest.
A key concept of our program is to introduce a certain level of flexibility to the data analysis. We used two strategies to increase flexibility:
	- allow to set key parameters (group mappings, weight mappings, etc.) in a config.json file
	- implement a simple ui terminal interface to allow the user to choose the aoi

2) #### Runtime.
Runtime is a major problem we encountered when broadening the data basis, running geographic operations, reading and writing the data for large areas. We used several strategies to increase the speed of the program significantly.
- multithreading to read the csv data 
- write csv data back to .parquet (HEX format) file format and read from there
- set up folder structure to save intermediate results and check for already existing data during each run
- vectorization of all dataframe operations where possible

## Architektur
SpotOn follows a basic 3 layer architecture. Essentially, the user specifies their area of 
interest via the terminal data is imported (I/O Module) indices are calculated
(Data processing) eventually important data points are written back to files.

A detailed overview of the architecture can be found in appendix 1. 

Additionally, we have created a call graph with snakeviz. The call graph shows which functions 
are called, how long they run and the connections between the functions. The call graph is 
appended as .png.

## Testkonzept (Thomas)

## Teamarbeit
Concepts and ideas were developed as a team. We met every week at least once in order to 
communicate on changes, new ideas, and problems that occurred during implementation.
Next to the common communication channels (whats app, email, face to face) we communicated 
insights won during individual research through a shared document.

Ideas were tested. Working ideas were kept. Not working implementations were discarded. However,
each idea and each concept brought forward was discussed and weighted against the shared 
understanding of the programs' usage.
The teamwork was close nit. After a while certain fields of "specialization" occurred.\
F.e. Christian mostly worked on import, data prep, cell and gemeinde-index calculation.\
Max mostly worked on questions of spatial dimension.\
Thomas mostly worked on spatial distribution of cars based on the gemeinde-Index, refactoring, and 
testing.\
While there was a bigger difference in competencies in the beginning of the project all members 
of the team worked their way into the project.

Tools and Methods used:
Tool | Usage
--------------
Teams | Video chat \
Whats App | unstructured communication \
Git | Software Version Control \
Trello | Task Management \
Programming Weekend | Face to Face developing ideas






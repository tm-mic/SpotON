## Präambel
Short description of the conceptual model.



## Conceptual Challenges and Algorithmic Solutions
### Cell Index Calculation (Christian)
Distributing ev from the administrative zba level to the gemeinde level based on raw data on 100x100m cell level is the key conceptual problem to solve in order to infer demand for ev charging poles in a given region. Ev distribution is infered by demographic data and number of households. An index based on ratios is created to allow for ev distribution on gemeinde level. The index is (conceptually) calculated in 1+6 steps

0) Data provided in the demographic data sets are mapped to geometries. (C)
The data provided comes with an id. This id implies the cell geometry middle point. In order to allow for the data to be spatially interpreted it is necessary to map the results from the index calculations to these geometries. However, calculating the geometries on the large scale demographic dataframes is slow. 

Therefore, the geometries are calculated on runtime from the cell x-y values pulled from the zensus data set. The point geodataframe is clipped by the shapefile area of interest polygon. 
In order to run attribute merges and joins on Gemeinde Ebene the gemeinden inside the area of interest are calculated and joined (covered_by) to the geometry reference geodataframe.

This step is done at the begining of the program to allow for attribute merges, joins, etc during runtime and reduce the needs for geometric operations on large dataframes.

1) Non conforming / unsutable age data grouping in the raw data is dissolved based on a pre-defined age group mapping. (C)
Age data on cell level is encoded in the raw data in two different attribute groupings: Alter_Kurz & Alter_10_JG. These age groupings do overlap and are not exclusively used for each cell. Hence, age observations are "doubled" or the necessary detail grouping is not provided. Also Alter_Kurz does not provide the information density necessary. It is important to note that age is a key factor for spatial ev distribution.

During runtime the data is being disaggregated. Based on a mapping between Alter_Kurz and Alter_10_JG, Alter_Kurz age groups are redistributed into Alter_10_JG age groups. To do so a random splitter value ranging from 0.4 - 0.6 is assumed and the number of observations in Alter_Kurz is split into corresponding Alter_10_JG age groups. Original Alter_10_JG age group obersavtions are retained. Only non existing Alter_10_JG age groups are appended to the resulting data frame. 

This widens the data basis and allows to draw some information from the mixed use of different non conforming age groupings. However, the redistribution relies on the assumption, that age groups described in the Alter_Kurz are more or less evenly distributed (between 0.4 - 0.6).

2) Attribute groupings are remapped to match group-value specifications. (C)
The raw data attribute observations are grouped as described in the metadata. It is clear, however, that only certain groups carry information to be sensically interpreted (weighted) when trying to distribute evs spatially based on an index. Hence, groups are remapped to match the groups specified in the config.json.

3) Missing data values are infered by median aggregates on the gemeinde level. (C)
For many cells there are observations (Insgesamt > 0), however, no attributes are listed in the dataset. This is crucial as observations have been made but there is no quantification of these in the raw dataset. In tune with our approach to redistribute ev s and ev charging poles on the gemeinde level median values for each attribute-code combination and each Gemeinde are calculated and missing values are added to the cells of a gemeinde. These "filler" values allow to add a 'baseline' for each Gemeinde, hence, making Gemeinden more comparable. The filler values are summed into the cell index.

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
	- allow to set key parameters (group mappings, weight mappings, etc) in a config.json file
	- implement a simple ui terminal interface to allow the user to choose the aoi 


2) #### Runtime.
Runtime is a major problem we encountered when broadening the data basis, reading and writing the data for large areas. We used several strategies to increase the speed of the program significantly.
- multithreading to read the csv data 
- write csv data back to .parquet (HEX format) file format and read from there
- folder structure to save intermediate results and check for already existing data during each run
- vectorization of all dataframe operations where possible

## Architektur

## Testkonzept

## Teamarbeit

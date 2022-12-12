# Knowledge Base

**This document collects knowledge from different aspects of the project.**

**All knowledge that has been researched to solve problems or basic syntax and semantics can be collected here.**

***
***
### Markdown Basics 
(https://www.markdownguide.org/basic-syntax/)

*This section lists the basics syntax and guides concerned with markdown.*

### Unordered list
    `+ list`
+ list

### Ordered list
    `1. First Element`
2. Second Element
3. Third Element

### Code
	indent TAB and use `backticks`

### Image
    `![ImageName](imagelink)`
![ImageName](imagelink)

### Links
    `[Title](Link)`
[Title](Link)

### Escape Chars
\ backlash before char

### Checkboxes
    `- []`
- []

### Comments
    `[This is a comment that will be hidden.]: #`

### Admonitions
    `>**Warning:** Do not push the big red button.`

>**Note:** Sunrises are beautiful.

>**Tip:** Remember to appreciate the little things in life.
***
***
## GIT 
*This section provides a basic overview on GIT and some GIT tutorials. Github is also part of this section.*

### GIT Tutorials
[GitLAB Docs and Tutorials](https://docs.gitlab.com/ee/tutorials/)

[FreeCodeCamp Tutorial GIT](https://www.freecodecamp.org/news/get-started-with-version-control-and-git/)

**Workflow in GIT**

**local**: Working copy --> staging area (Index) --> commit changes to local repository
	- include message for every commit

**remote**: push to remote --> 
fetch, merge or pull to get all changes from remote repository

**git ignore**
- create .gitignore
	- Auswahl der Dateien in gitignore über gitignore.io
	- auch virtualenv 
	- Bilder, Dateien, Daten

### Get complete Project
clone : clone everything from remote to local repository

### Local: Create Commands GIT
*Create .git Folder*
    `git init`

*View Git Log in Terminal*
	`git log`

*Stage Change to existing Branch*
	`git add xyz.txt`
	or use `<.>` to stage changes of all files in the directory
	**Stage Files Locally**
	
    git add . (for all files)

	git add <filename> (for one file)

	show all files for staging
	git status 

### Local: Commit & Branches
- commit changes to the branch with Message
`git commit -m "<Message>"`

### Switch branches 
- switch to another Branch
	`git checkout <Branch Name>`

- merge Branches
	`git merge <source branch name to update>`

### Remote : Push to remote
- add pointer to Repo
	`git remote add origin <URL to Repo>`

- push main / master to Repo
	`git push origin <main / master>`
	- conflicts need to be resolved via a pull and then a push

- create new Branch
	`git checkout -b <new Branch Name>`

- mirror local brach as is to remote branch .. all changes of local will be force updated in remote
	`git push --mirror`

### Reset and Revert 
- soft reset: uncommit and keep changes
- hard reset: uncommit and delete changes

	`git reset --soft HEAD~<number of commits to undo>`

- undo specific commit via hash (to be found in log)
	`git revert <hash value>`
***
***
## Python 
*This section lists important technical concepts for writing python code.*
### Lists and List Comprehension

- enumerate --> Listen mit Index durchsuchen 


    `for i, element in enumerate(my_list):`

--> i : counter
--> element : Listen Element

### List comprehension : 
Run through list and add +1 to each element of the list:
    
    new_list = [number+1 for number in my_list]

Conditions for list comprehension:

    even_list = [x for x in my_list if x%2 == 0]

### Objektdefinition

    class X:
	def __init__(self):
		self.attribute_one = None
		self.attribute_two = None
> **TIP:** After initial class definition all can be used analogues to java.

#### Difference `__new__` & `__init__`
`__new__` ist used to create a new object from immutable object types (f.e. lists, tuples)

#### Inspect an Object
use 
	`print(vars(<object>))`

cast specific attribute to a varaible
	`x = getattr(<objectname>, <attributename>)`


[Link: New and Init the difference](https://medium.com/thedevproject/new-vs-init-in-python-a-most-known-resource-7beb538dc3b)
### Parent Vererbung
    `class child(parent):`

- to call a parent method 
 - [] create object
 - [] call parent method same as child method
 	`child = child()
 	 child.method`
- parent methods can be called inside childrens overriden methods via 
	1) `parent.method()`
	2) `super().parentmethod()`
***

# Testing in Python
Reference: [Real Python](https://realpython.com/python-testing/)

## Terminology
Unit Test => checks individual componenents of your application
Integration Test => checks if the application delivers the output requested

### Unit Test
Consist of:
- test case
- assertion
- entry point

Example for simple sum() test case:
	
	`def test_sum():
    assert sum([1, 2, 3]) == 6, "Should be 6"
	if __name__ == "__main__":
		test_sum()
		print("Everything passed")`

Standard Library for testing in Python is unittest.
It is required that:
- You put your tests into classes as methods
- You use a series of special assertion methods in the unittest.TestCase class instead of the built-in assert statement

Example for unittest test:

	`import unittest
	class TestSum(unittest.TestCase):
    def test_sum(self):
        self.assertEqual(sum([1, 2, 3]), 6, "Should be 6")
    def test_sum_tuple(self):
        self.assertEqual(sum((1, 2, 2)), 6, "Should be 6")
	if __name__ == '__main__':
		unittest.main()`

Pytest is another test library, which reduces complexity and allows for use of many plugins.

### Setting up testing environment
- testfile should be able to import the functions - hence they should be structured above the module file in the project tree 
- functions, modules and classes can be imported to the test file using __import__()

### Test Structure
*Before you dive into writing tests, you’ll want to first make a couple of decisions:*
    - What do you want to test?
    - Are you writing a unit test or an integration test
*Then the structure of a test should loosely follow this workflow:*
    - Create your inputs
    - Execute the code being tested, capturing the output
    - Compare the output with an expected result
*For this application, you’re testing sum(). There are many behaviors in sum() you could check, such as:*
    - Can it sum a list of whole numbers (integers)?
    - Can it sum a tuple or set?
    - Can it sum a list of floats?
    - What happens when you provide it with a bad value, such as a single integer or a string?
    - What happens when one of the values is negative?

Example for a simple test in the test.py file:

	`import unittest
	from my_sum import sum					# import the function to be tested
	class TestSum(unittest.TestCase):		# create test class to inherit TestCase functionality
		def test_list_int(self):
			"""
			Test that it can sum a list of integers
			"""
			data = [1, 2, 3]				# provide data structure to be tested on
			result = sum(data)				
			self.assertEqual(result, 6)		# assert result of test case
	if __name__ == '__main__':
		unittest.main()`

**Assertion of results comes with different methods. These methods should fit your input data.**
Method 	Equivalent to
.assertEqual(a, b) 	a == b
.assertTrue(x) 	bool(x) is True
.assertFalse(x) 	bool(x) is False
.assertIs(a, b) 	a is b
.assertIsNone(x) 	x is None
.assertIn(a, b) 	a in b
.assertIsInstance(a, b) 	isinstance(a, b)
*.assertIs(), .assertIsNone(), .assertIn(), and .assertIsInstance() all have opposite methods, named .assertIsNot(), and so forth.*

To activate Test Runner insert following code at bottom of test.py 
	`if __name__ == '__main__':
    unittest.main()`

or use 

	`python -m unittest discover` #only works if test files follow naming convention of test*.py


***
## OSM and OSM API
*This section provides basics for dealing with OSM data retrieval via the API and basic knowledge on the structure of OSM.*
### Tags

- Features in OSM are described by tags.
- Tags are attached to basic data structures (way, node, relations)
- Tags are subs of keys
	- keys can have many different tags (user generated)

Tag descrtiptions can be found here:
https://taginfo.openstreetmap.org/keys 

### OSM OverPass API

[Test Queries via Overpass Turbo](http://overpass-turbo.eu/)

[Using the Overpass API with Python request](https://towardsdatascience.com/loading-data-from-openstreetmap-with-python-and-the-overpass-api-513882a27fd0)

- OSM features (mapped physical objects) can be of type node, way, or relation
	- node: 	mapped point with coordinates
	- way:		ordered list of nodes
	- relation:	list containing members (nodes, ways, relations) to map geographic or logic connections

**Access Overpass API via Terminal**

    curl --globoff -o output.xml http://overpass-api.de/api/interpreter?data=node(1);out;

>**Note**: data=node(1);out; specifies the querry

**Access Overpass API via Python Requests**

*Example*

    import requests
    import json
    overpass_url = "http://overpass-api.de/api/interpreter"
    #query 	
    overpass_query = 
        """
        [out:json];
        area["ISO3166-1"="DE"][admin_level=2];
        (node["amenity"="biergarten"](area);
        way["amenity"="biergarten"](area);
        rel["amenity"="biergarten"](area);
        );
        out center;
        """
    response = requests.get(overpass_url, 
                            params={'data': overpass_query})
    data = response.json()`

- requests package builds URL from URL and params and requests a json response from the server

### Overpass Language Details

[Refer to the Overpass Language Guide for detail information](https://wiki.openstreetmap.org/wiki/Overpass_API/Language_Guide#Selecting_areas_by_name)

#### Overpass QL
Example:

    area["ISO3166-1"="DE"][admin_level=2];(
    node["amenity"="biergarten"](area);
    way["amenity"="biergarten"](area);
    rel["amenity"="biergarten"](area);
    );
    out center;


- [] define the feature as f.e. feature_type["key"="tag"]({{bbox}});
- [] bbox defines a bounding box of an area by:
	
	[bbox as used in Overpass API](https://wiki.openstreetmap.org/wiki/Bounding_Box#Overpass_API)
	
    bbox = min lat, min long, max lat, max long  ;  f.e greater London -->
 
      `{{bbox|-0.489|51.28|0.236|51.686}}`

- [] use `out;` to define the end of the query and request data
	`node["amenity"="pub"]
  	(53.2987342,-6.3870259,53.4105416,-6.1148829); 
	out;`
-[] [if an area exists queries by area (features inside the area) can be 	done via](https://wiki.openstreetmap.org/wiki/Overpass_API/Language_Guide#Area_clauses_(%22area_filters%22))
	
    area[name="the name of the place you want to search"];)->a;
	
	- instead of the name="" one can also use ISO and admin_level keys; for more information refer to the [language guide](#OverpassLanguageDetails)
	the term
	`->a;`
	- allows to save the query in the variable a
	- recursive queries can be formulated
	`(area.a)`
	- is used to only display the features which intersect with the area a as constructed above
- [] put a ';' at the end of each statement
- [] the out; query printer can be used with different levels of verbosity refer to the [language guide](https://wiki.openstreetmap.org/wiki/Overpass_API/Language_Guide#Degree_of_verbosity)
- []to get results as JSON and not XML add a query header with 
	`[out:json]`
***
***
# Spatial Data Statistics
*The following section is a collection of thoughts and interesting aspecpts found [Geographic Data Science Book](https://geographicdata.science/book/intro.html).*

https://geographicdata.science/
https://pysal.org/
***
## General

Representation of geographic processes in:
- Objects : discrete objects with specific space and time
- Fields : continous surface
- Networks : set of connections between objects and / or fields

> Geographic data (models) need to be represented by datastructures these linkages between datamodel and datastructures are explained below. 
***
### Datastructures & Usage

#### Geographic Tables (representing objects)
*2D Representation of geometric Informations as objects*
Implementation in **Geopandas**
- each DF has a column for geometry
- this column ist used for calc. geometry 

**plotting**

    df_object.plot()

**changing geometry**
- when a new geometry is calculated this needs to be saved into a geometry column

f.e. calculating the centroid of polygons
	`df_object_new = df_object.geometry.centroid`
- when the new geometry is saved in the same df the geometry needs to be set as main
	`df_object_new.set_geometry("centroid")`

**changing regular csv to geopanda dataframe**

1) setting point coordinates
	
    
    `pt_geoms = geopandas.points_from_xy(
    x=gt_points["longitude"],
    y=gt_points["latitude"],
    crs="EPSG:4326",
    )`

2) create GeoPandas DF Object


    `gt_points = geopandas.GeoDataFrame(gt_points, geometry=pt_geoms)`


#### Surfaces (representing fields)
- represented in xarray s. - storage in uniform grids
- with x and y coordinates as row and columns
- cells as attribute storage

*read GEOTIFF* ≙ GEOTIFF : Geo encoded rasterImage

read with xarray f.e.
    
    xarray.open_rasterio("<filepath>")

coordinates are separated in x,y and band --> three dimensions can be covered

	`xarray_object.attrs` 
shows the attributes associated to the array

	`xarray_object.sel`
allows to select one band with x,y

> NAN data is recorded as negativ values. 

Hence, NAN needs to be taken out when plotting or calculating with :

	`xarray_object.where(xarray_object != -200).sel(band = 1).plot(cmap = RdPU);`


#### Spatial graphs (representing networks)
connections between objects defined by matrices
--> pysal
show relations between objects (need to be differentiated from simple object overlay relations)

- unstructered compared to surfaces and geotables because links can be multi- or one dimensional, or no linkage

- spatial graphs might be represented in different data structures

##### Spatial Weights Matrices (as special case of a spatial graph)
returns graph representation of the given place
	
    object = osmnx.graph_from_place("<placename>")

find specific node

	`object.nodes[<nodeNumber>]`

show geometry information of the edge
	
    `object.edges[nodeNumber1, nodeNumber2, edgeNumber]`

find adjecant nodes to the node given

	`list(object.adj[<nodeNumber>].keys())`



### Transforming Data Structures

#### Surfaces as Tables
*aggregating cells into objects - with centroid as object marker*
*calculating zonal statistics* [explanation](https://geographicdata.science/book/notebooks/03_spatial_data.html#surfaces-as-tables)

--> use **rasterstats** library

#### Tables as Surfaces
*Converting the dataset from a geo-table into a surface involves laying out a grid and counting how many points fall within each cell.*
--> use datashader
1) setup canvas
	`cvs = datashader.Canvas(plot_width=60, plot_height=60)`
2) transfer points to grid
	`grid = cvs.points(gt_points, x="longitude", y="latitude")`

#### Networks as Graphs and Tables
*converting graph information to nodes and edges in surfaces or geotables*
--> use osmnx 
	`gt_intersections, gt_lines = osmnx.graph_to_gdfs(graph)`

moving back to graphs --> 
	`new_graph = osmnx.graph_from_gdfs(gt_intersections, gt_lines)`

***
### Logical Implications for Spatial Calculations
#### Fallacies
Ecological Fallacy:
MAUP might come from:
- Spatial heterogeneity
- Aggregation level
- Zoning 

> conclusions cannot be infered on variables on different scale than the data is scaled upon ! --> scale dependency

#### Neighborhood and spatial Weights
[Spatial Matrices --> Neighborhood as Matrices](https://link.springer.com/referenceworkentry/10.1007/978-0-387-35973-1_1307)

- number of interactions N/(N-1)/2
- transformation of geography to a graph of interactions between neigbhboors
- define zeros to decrease number of elements or restrcit number of neighboors
- single relationsship factor for all interactions
	- single auto-correlation factor

*To deal with neihgborhood problem mathematically transform spatial correlation to matrix*

Based on different factors such as socioeconomic weights f.e
1) each node is represented as line in the matrix with 
	0 as non-relation 
	1 as relation
--> links (neigborhoods) must match empiric process -- spatial process must be represented



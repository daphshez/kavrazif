
GTFS Extender
-------------
GTFS extender extracts interesting data from the GTFS and dumps it to new files. 

### trip stories and full trips


### full_stops.txt
Like GTFS stops.txt file, but with the following extra fields:

- nearest_train_station: stop_id for the nearest train station 
- train_station_distance: distance in meters from the train station,
- routes_here: short names (=signed names) of the routes stopping in the station


### kavrazif routes 


data files from open train project
----------------------------------

[gtfs archive (until 2015-07, railway only)](http://192.241.154.128/gtfs-data/)

[gtfs archive (2015-11 and later)](http://gtfs.otrain.org/static/archive/)

[actual train data](http://otrain.org/files/)




Status
======
Results
-------
1. The advertised schedule isn't realistic at all, the expected travel time is the same all day\week long.  E.g. un 2016-5-1 gtfs file, 
there are 7962 routes and 8144 route stories (there are 194 routes with two route stories.) **It would be interesting to find if any of these is binding the operators.**

2. [Here's a map of bus stops up to 300 meters from train stations](< http://arcg.is/285fnu0>).

3. [A table of the number of bus stopping at each train station, by hour](https://docs.google.com/spreadsheets/d/1aAlK9Tmp0VT0LmBvEaLj0yJ_21-UtNWyrYKBa9pjWxo/edit?usp=sharing)


Tasks
-----
* ~~a table of train stations with the number of buses stopping near the station every hour of the day.~~
* make a list of "kav razif" routes
* Merge bus route alternatives in train visits calculations. 

    from Levenshtein import distance
    edit_dist = distance("ah", "aho")

* For each train station, how many buses in the area & in which frequency
* map routes to show where you can get from the train station 
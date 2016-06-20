
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

2. [A new map of stops near stations. Gray points are the train stations; green are up to 300m away, yellow 300-500m, red 500-800m](http://arcg.is/25LMpxg)

3. [A table of the number of bus stopping at each train station, by hour](https://docs.google.com/spreadsheets/d/1aAlK9Tmp0VT0LmBvEaLj0yJ_21-UtNWyrYKBa9pjWxo/edit?usp=sharing)

4. Most of the Kav Razif line start\end at the train station. Exceptions: Bet Yehosha lines 11, 12, Binyamina lines 
   23, 24 and Kfar Saba lines 40 & 41


Tasks
-----
* ~~a table of train stations with the number of buses stopping near the station every hour of the day.~~
* ~~make a list of "kav razif" routes~~
* How many of the train arrivals to the stations are served by each Kav Razif departure (merge alternates),
by hour, and how many of the Kaz Razif departure correspond to a train arrival 
* Merge bus route alternatives in train visits calculations. 
* For each train station, how many buses in the area & in which frequency
* map routes to show where you can get from the train station 


Issues
------
~~Kav Razif routes currently have multiple records for different trip stories. This probably means trip story was changed
during the GTFS period. We need to specify dates in order to select only one.~~
Also we currently have multiple alternatives which probably shouldn't be taken into account.

## Kav Razif

This project is done for the [Public Knowledge Workshop](http://www.hasadna.org.il)
in co-operation with 15minutes, and advocacy group of public transport passengers. 

We try to evaluate the co-ordination of between trains and buses and Israel. How well are the stations served
by buses? How well are the bus arrivals and departure co-ordinated with train arrivals and departures? 

At this stage the project focuses on planning data, as advertised by the transport regulator 
(the department of transport) in a GTFS file. 

## GTFS data
GTFS data is published nightly by the department of transport. It contains data on all scheduled bus and train trips
in Israel. Each file contains information for the following two months. 

The Open Train project has an archive of GTFS files: 

* [gtfs archive (until 2015-07, railway only)](http://192.241.154.128/gtfs-data/)
* [gtfs archive (2015-11 and later)](http://gtfs.otrain.org/static/archive/)

## Reading & extending GTFS data
GTFS is just a set of CSV files. ilgtfs.GTFS has methods for reading these files into memory. 

We expect the GTFS file name to be israel-public-transportation.zip. ilgtfs.GTFS object is initialized with the
(local) folder that contain the zip file. 
 
gtfs_extender.py does some useful pre-computations on the GTFS. 
It dumps the results into more csv files in the same folder. ilgtfs.ExtendedGTFS can read these files. 

### Route stories
... Route stories should be explained here ...

### full_stops.txt
Like GTFS stops.txt file, but with the following extra fields:

- nearest_train_station: stop_id for the nearest train station 
- train_station_distance: distance in meters from the train station,
- routes_here: short names (=signed names) of the routes stopping in the station

### kavrazif routes 





Status
======
Results
-------
1. The advertised schedule isn't realistic at all, the expected travel time is the same all day\week long.  E.g. un 2016-5-1 gtfs file, 
there are 7962 routes and 8144 route stories (there are 194 routes with two route stories.) **It would be interesting to find if any of these is binding the operators.**

2. [A new map of stops near stations. Gray points are the train stations; green are up to 300m away, yellow 300-500m, red 500-800m](http://arcg.is/25LMpxg)

3. [A table of the number of bus stopping at each train station, by hour](https://docs.google.com/spreadsheets/d/1aAlK9Tmp0VT0LmBvEaLj0yJ_21-UtNWyrYKBa9pjWxo/edit?usp=sharing)

4. [A map of all stops from which you can arrive to a train station, with planned travel time of up to 30 minutes. 
 Only includes stops with at least 5 buses stopping every day. Not including data for the busiest train stations](http://arcg.is/292ifCF]

5. Most of the Kav Razif line start\end at the train station. Exceptions: Bet Yehosha lines 11, 12, Binyamina lines 
   23, 24 and Kfar Saba lines 40 & 41


Tasks
-----
* ~~a table of train stations with the number of buses stopping near the station every hour of the day.~~
* ~~make a list of "kav razif" routes~~
* map routes that serve train stations: stop, train station it serves, time to\from train station, number of trips
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



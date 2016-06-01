
implementation
--------------
- ilgtfs - read israel gtfs file, and also extended gtfs which includes the files created by gtfs_extender
- gtfs_extender - functions to "extend" gtfs with some generated re-usable data 

extended gtfs files documentation
---------------------------------
### trip stories and full trips


### full stops


### kavrazif routes 


data files from open train project
----------------------------------

[gtfs archive (until 2015-07, railway only)](http://192.241.154.128/gtfs-data/)

[gtfs archive (2015-11 and later)](http://gtfs.otrain.org/static/archive/)

[actual train data](http://otrain.org/files/)



Results
-------
In 2016-5-1 gtfs file, 
There are 7962 routes and 8144 route stories. I.e. there are 194 routes with two route stories. 
I.e. - the advertised schedule isn't realistic at all, the expected travel time is the same all day\week long. 


Tasks
-----
* "extend gtfs" function + read extended gtfs  
* make a list of "kav razif" routes
* Merge bus route alternatives in train visits calculations. 

    from Levenshtein import distance
    edit_dist = distance("ah", "aho")

* For each train station, how many buses in the area & in which frequency
* map routes to show where you can get from the train station 

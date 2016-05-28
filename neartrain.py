import ilgtfs

gtfs_file = r'data/gtfs_2016_05_01/israel-public-transportation.zip'
g = ilgtfs.GTFS(gtfs_file)
g.find_stop_routes()
g.find_train_stations()
print("There are %d train stations" % len([stop for stop in g.stops.values() if stop.is_train_station]))
g.find_distance_from_train_station()
stops_near_train_stations = [stop for stop in g.stops.values() if stop.distance_from_train_station < 300]
print("There are %d stops near train stations " % len(stops_near_train_stations))
g.export_full_stops()


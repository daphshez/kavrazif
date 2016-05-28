import ilgtfs
import geo


def find_train_stations(g):
    g.find_train_stations()
    return [stop for stop in g.stops.values() if stop.is_train_station]


def find_stops_near_train_stations(g, train_stations, max_distance=500):
    g.find_distance_from_train_station()
    return [stop for stop in g.stops.values() if stop.distance_from_train_station < max_distance]


def find_routes_through_stops(g, stops):
    stop_ids = set(stop.stop_id for stop in stops)
    routes = set()
    for trip in g.trips:
        for trip_story_stop in g.trip_stories[trip.trip_story_id]:
            if trip_story_stop.stop_id in stop_ids:
                routes.add(trip.route)
    return routes


def print_stops(stops, filename):
    with open(filename, 'w', encoding='utf8') as f:
        f.write("stop_id,stop_code,stop_name,stop_desc,stop_lat,stop_lon\n")
        for stop in sorted(stops, key=lambda x: x.stop_id):
            f.write(",".join(str(x) for x in (stop.stop_id,
                                              stop.stop_code,
                                              stop.stop_name,
                                              stop.stop_desc,
                                              stop.stop_lat,
                                              stop.stop_lon)) + "\n")

if __name__ == "__main__":
    gtfs_file = r'data/gtfs_2016_05_01/israel-public-transportation.zip'
    g = ilgtfs.GTFS(gtfs_file)
    g.load_full_trips()
    g.load_stops()
    train_stations = find_train_stations(g)
    print("There are %d train stations" % len(train_stations))
    print_stops(train_stations, r'data/gtfs_2016_05_01/train_station_stops.txt')
    stop_near_train_stations = find_stops_near_train_stations(g, set(train_stations))
    print("There are %d stops near train stations " % len(stop_near_train_stations))
    print_stops(stop_near_train_stations, r'data/gtfs_2016_05_01/near_train_stops.txt')


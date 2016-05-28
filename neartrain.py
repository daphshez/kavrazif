import ilgtfs
import geo


def find_train_stations(g):
    train_agency = 2
    train_trips = [trip for trip in g.trips.values() if trip.route.agency.agency_id == train_agency]
    train_trip_story_ids = set(trip.trip_story_id for trip in train_trips)
    train_station_ids = set()
    for trip_story_id in train_trip_story_ids:
        train_station_ids.update(trip_story_stop.stop_id for trip_story_stop in g.trip_stories[trip_story_id])
    return [g.stops[stop_id] for stop_id in train_station_ids]


def find_stops_near_train_stations(g, train_stations, max_distance=500):
    train_station_points = [(stop, geo.GeoPoint(stop.stop_lat, stop.stop_lon)) for stop in train_stations]
    result = []
    for stop in g.stops.values():
        stop_point = geo.GeoPoint(stop.stop_lat, stop.stop_lon)
        for train_station, train_station_point in train_station_points:
            distance = train_station_point.distance_to(stop_point)
            if distance < max_distance:
                result.append((stop, train_station, distance))
                break
    return result


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
    print_stops([x[0] for x in stop_near_train_stations], r'data/gtfs_2016_05_01/near_train_stops.txt')


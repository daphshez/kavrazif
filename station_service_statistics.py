"""
 find the number of buses that serve each station.
"""

from ilgtfs import ExtendedGTFS
from collections import defaultdict, Counter
from datetime import date
from collections import namedtuple
import csv
from geo import GeoPoint

StationStop = namedtuple('StationStop', ['station_stop_id', 'story_stop_sequence'])

weekdays = {6, 0, 1, 2, 3}


def stops_distance(g, stop_id1, stop_id2):
    """Returns the geographical distance (in meters) between the two stops"""
    p1 = GeoPoint(g.stops[stop_id1].stop_lat, g.stops[stop_id1].stop_lon)
    p2 = GeoPoint(g.stops[stop_id2].stop_lat, g.stops[stop_id2].stop_lon)
    return p1.distance_to(p2)


def train_station_stops(g, route_story, max_station_distance=500):
    """ Returns a map from station_id (stop_id of a  train station), for each station the bus passes by to the id of the
    nearest bus stop. route_story is expected to be a route story of a bus route.
    """
    stops_near_station = [stop for stop in route_story.stops
                          if g.stops[stop.stop_id].train_station_distance <= max_station_distance]
    # set of train stations that the route story passes
    stations = {g.stops[stop.stop_id].nearest_train_station_id for stop in stops_near_station}
    res = {}
    for station in stations:
        # find all the trip stops near this train station
        stops_near_this_station = [stop for stop in stops_near_station
                                   if g.stops[stop.stop_id].nearest_train_station_id == station]
        # sort by the distance from the the train station
        sorted_stops = list(sorted(stops_near_this_station,
                                   key=lambda s: g.stops[s.stop_id].train_station_distance))
        res[station] = sorted_stops[0].stop_id
    return res


# returns:
#     1) a list of integers, time_to_station, where
#     time_to_station[i] is the time from route_story_stop[i] to the nearest train station the route passes
#     2) A list of stop_id, which are the nearest station_id to each stop (geographically)
# or None if the route_story doesn't pass by any station
def route_story_time_to_station(g, route_story):
    station_to_stop = train_station_stops(g, route_story)
    if len(station_to_stop) == 0:  # route doesn't stop near station
        return None, None

    # stop_to_station = {stop_id: station_id for (station_id, stop_id) in station_to_stop.items()}

    # find the nearest station (geographically) to each bus stop on the route
    if len(station_to_stop) == 1:
        stations = list(station_to_stop.keys()) * len(route_story.stops)
    else:
        stations = []
        for route_story_stop in route_story.stops:
            station_distances = sorted((stops_distance(g, station_id, route_story_stop.stop_id), station_id)
                                       for station_id in station_to_stop)
            stations.append(station_distances[0][1])

    to_station = []
    for route_story_stop, station_id in zip(route_story.stops, stations):
        # find the bus stops that are actually near the stations
        route_story_stops_at_station = [route_story_stop for route_story_stop in route_story.stops if
                                        route_story_stop.stop_id == station_to_stop[station_id]]
        assert len(route_story_stops_at_station) > 0
        # there's an edge case where the bus stops at this stop twice, so
        # len(route_story_stops_near_station) could be > 1
        # this happens on circular routes that go especially out of their way to a train station
        # the way I handle it here may not be appropriate for all uses
        at_station = [stop.arrival_offset for stop in route_story_stops_at_station]
        distance_to_station = sorted(((offset - route_story_stop.arrival_offset) for offset in at_station),
                                     key=lambda x: abs(x))
        to_station.append(distance_to_station[0])
    return to_station, stations


def route_story_weekly_trip(g, start_date, end_date, weekdays_only):
    """Returns a map from route_story_id, to the weekly trips of that route_story, between the specified dates"""
    res = defaultdict(lambda: 0)
    for trip in g.trips.values():
        if trip.service.end_date >= start_date and trip.service.start_date <= end_date:
            if weekdays_only:
                days = len(trip.service.days.intersection(weekdays))
            else:
                days = len(trip.service.days)
            res[trip.route_story.route_story_id] += days
    return res


# and which stops in the trip story are the stops near the trains station?
def by_train_trips(g, start_date, end_date, max_station_distance=500, ignore_stations=None):
    """Returns a map from trip to a list of StationStop objects"""

    def station_stops(trip):
        stops_near_station = [stop for stop in trip.route_story.stops
                              if g.stops[stop.stop_id].train_station_distance <= max_station_distance]
        # set of train stations that the trip passes
        stations = {g.stops[stop.stop_id].nearest_train_station_id for stop in stops_near_station}
        if ignore_stations is not None:
            stations.difference_update(ignore_stations)
        res = []
        for station in stations:
            # find all the trip stops near this train station
            stops_near_this_station = [stop for stop in stops_near_station
                                       if g.stops[stop.stop_id].nearest_train_station_id == station]
            # sort by the distance from the the train station
            sorted_stops = list(sorted(stops_near_this_station,
                                       key=lambda s: g.stops[s.stop_id].train_station_distance))
            # we want to only take the nearest stop to the train station
            # however there are some weird corner cases of circular bus routes that stop at the same stop
            # by a train station on both directions of the of on trip
            # we want to return both those stops
            res += [(station, stop) for stop in sorted_stops if stop.stop_id == sorted_stops[0].stop_id]
        return res

    print("Running by_train_trips")
    bus_trips = [trip for trip in g.trips.values() if trip.route.route_type == 3]
    print('  number of bus trips: %d' % len(bus_trips))
    bus_trips_in_dates = [trip for trip in bus_trips if
                          trip.service.end_date >= start_date and trip.service.start_date <= end_date]
    print('  number of bus trips in date range: %d' % len(bus_trips_in_dates))
    trips_and_stops = ((trip, station_stops(trip)) for trip in bus_trips_in_dates)

    return {trip: stops for (trip, stops) in trips_and_stops if len(stops) > 0}


# station_hourly_data Dict[int, Tuple[int, int]] - dictionary from station id to (hour, count)
def export_station_hourly_data(g, station_hourly_data, output_filename):
    print("Running export_station_hourly_data")
    with open(output_filename, 'w', encoding='utf8') as f:
        field_names = ['station_stop_id', 'station_name', 'daily_total'] + [('h%d' % h) for h in range(26)]
        f.write(','.join(field_names) + '\n')
        for station_id in station_hourly_data:
            station = g.stops[station_id]
            line = ','.join([str(station_id), station.stop_name] +
                            [str(sum(station_hourly_data[station_id]))] +
                            [str(count) for count in station_hourly_data[station_id]])
            f.write(line + '\n')


# station_to_hourly_counter: Dict[int, Counter[Tuple[int, int]]] -
#    for each station, a counter of (day, hour) pairs
# returns -  dictionary from station id to (hour, count)
def station_hourly_average_sun_to_thurs(counters):
    print("Running station_hourly_average_sun_to_thurs")
    result = {}
    for station in counters:
        result[station] = []
        for hour in range(26):
            hourly_average = sum(counters[station][(day, hour)] for day in weekdays) / 5
            result[station].append(hourly_average)
    return result


def bus_station_visits(g, start_date, end_date, max_distance_from_station=500):
    print("Running bus_station_visits")
    trips = by_train_trips(g, start_date, end_date, max_distance_from_station)
    print("  number of bus trips that pass by stations: %d" % len(trips))
    station_to_hourly_counter = defaultdict(lambda: Counter())
    for trip, stops_near_stations in trips.items():
        for station_id, route_story_stop in stops_near_stations:
            hour = (trip.start_time + route_story_stop.arrival_offset) // 3600
            for day in trip.service.days:
                station_to_hourly_counter[station_id][(day, hour)] += 1
    print("  done. found data for %d stations" % len(station_to_hourly_counter))
    return station_to_hourly_counter


def train_station_visits(g, start_date, end_date):
    print("Running train_station_visits")
    train_trips = (trip for trip in g.trips.values() if trip.route.route_type == 2)
    train_trips = [trip for trip in train_trips if
                   trip.service.end_date >= start_date and trip.service.start_date <= end_date]
    print("There are %s trips in the date span" % len(train_trips))
    print("Building hourly data dict")
    station_to_hourly_counter = defaultdict(lambda: Counter())
    for trip in train_trips:
        for stop in trip.route_story.stops:
            hour = (trip.start_time + stop.arrival_offset) // 3600
            for day in trip.service.days:
                station_to_hourly_counter[g.stops[stop.stop_id].nearest_train_station_id][(day, hour)] += 1
    print("  done. found data for %d stations" % len(station_to_hourly_counter))
    return station_to_hourly_counter


def export_bus_station_visits(g, station_to_hourly_counter, start_date, end_date):
    output_filename = g.at_path('hourly_bus_station_visit_sun_thur_%s_%s.txt' %
                                (start_date.strftime('%Y-%m-%d'), end_date.strftime('%Y-%m-%d')))
    export_station_hourly_data(g, station_hourly_average_sun_to_thurs(station_to_hourly_counter), output_filename)


def export_train_station_visits(g, station_to_hourly_counter, start_date, end_date):
    output_filename = g.at_path('hourly_train_arrivals_sun_thur_%s_%s.txt' %
                                (start_date.strftime('%Y-%m-%d'), end_date.strftime('%Y-%m-%d')))
    export_station_hourly_data(g, station_hourly_average_sun_to_thurs(station_to_hourly_counter), output_filename)


def stops_connected_to_stations_map(g, start_date, end_date, ignore_stations, station_offset_range, min_daily_visits=5):
    print("stops_connected_to_stations_map starting")
    ResultRecord = namedtuple('ResultRecord', ['station_id', 'weekly_visits', 'routes'])

    def build_stop_data():
        route_story_frequency = route_story_weekly_trip(g, start_date, end_date, weekdays_only=True)
        print("   len(g.route_stories)=%d" % len(g.route_stories))
        print("   len(route_story_frequency)=%d" % len(route_story_frequency))

        route_story_id_to_route = {route_story_id: route
                                   for route in g.routes.values()
                                   for route_story_id in route.route_story_ids}

        res = {}
        route_with_weekday_trips = 0
        route_going_through_station = 0
        for route_story_id, route_story in g.route_stories.items():
            if route_story_frequency[route_story_id] == 0:  # weekend only stories
                continue

            route_with_weekday_trips += 1

            offsets_to_station, stations = route_story_time_to_station(g, route_story)
            if offsets_to_station is None:  # story doesn't pass near station
                continue

            route_going_through_station += 1

            for route_story_stop, offset, station_id in zip(route_story.stops, offsets_to_station, stations):
                if offset < station_offset_range[0] or offset > station_offset_range[1]:  # stop to far from station
                    continue

                if route_story_stop.stop_id in res:
                    prev_record = res[route_story_stop.stop_id]
                    assert station_id == prev_record.station_id
                    prev_record.routes.add(route_story_id_to_route[route_story_id])
                    record = ResultRecord(station_id, prev_record.weekly_visits + route_story_frequency[route_story_id],
                                          prev_record.routes)
                else:
                    record = ResultRecord(station_id, route_story_frequency[route_story_id],
                                          {route_story_id_to_route[route_story_id]})
                res[(route_story_stop.stop_id, station_id)] = record

        print("   route_with_weekday_trips=%d" % route_with_weekday_trips)
        print("   route_going_through_station=%d" % route_going_through_station)
        print("   number of stops on those routes=%d" % len(res))
        return res

    def select_station(built_stop_data):
        tmp = defaultdict(lambda: set())
        for stop_id, station_id in built_stop_data.keys():
            tmp[stop_id].add(station_id)
        res = {}
        for stop_id in tmp.keys():
            station_distances = sorted((stops_distance(g, stop_id, station_id), station_id)
                                       for station_id in tmp[stop_id])
            res[stop_id] = built_stop_data[(stop_id, station_distances[0][1])]
        return res

    def prepare_for_export(for_export):
        print("   preparing for export, number of stops=%d" % len(for_export))
        res1 = {stop_id: record for (stop_id, record) in for_export.items() if record.station_id not in ignore_stations}
        print("   stops near non ignore stations=%d" % len(res1))
        res2 = {stop_id: record for (stop_id, record) in for_export.items() if record.weekly_visits >= 25}
        print("   stops with at least 25 weekly visits=%d" % len(res2))
        res = {stop_id: record for (stop_id, record) in res1.items() if stop_id in res2.keys()}
        print("   stops fulfilling both conditions=%d" % len(res))
        return res

    def export(file_name, for_export):
        print("   exporting")
        fields = ['station_id', 'station_name', 'stop_id', 'stop_name', 'line_numbers', 'daily_visits',
                  'latitude', 'longitude']
        with open(file_name, 'w', encoding='utf8') as f:
            writer = csv.DictWriter(f, fieldnames=fields, lineterminator='\n')
            writer.writeheader()
            for stop_id in for_export:
                station_id, weekly_visits, routes = for_export[stop_id]
                data = {'station_id': station_id,
                        'station_name': g.stops[station_id].stop_name,
                        'stop_id': stop_id,
                        'stop_name': g.stops[stop_id].stop_name,
                        'line_numbers': ' '.join(r.line_number for r in routes),
                        'daily_visits': weekly_visits / 5,
                        'latitude': g.stops[stop_id].stop_lat,
                        'longitude': g.stops[stop_id].stop_lon}
                writer.writerow(data)
        print("Done.")

    export(g.at_path('30_min_to_station.txt'), prepare_for_export(select_station(build_stop_data())))


if __name__ == '__main__':
    gtfs = ExtendedGTFS(r'data/gtfs/gtfs_2016_05_25')
    gtfs.load_stops()
    gtfs.load_trips()
    start = date(2016, 6, 1)
    end = date(2016, 6, 14)
    busiest_train_stations = {37358, 37312, 37350, 37388, 37292, 37376, 37378, 37318, 37386, 37380, 37348, 37360}
    # export_train_station_visits(gtfs, train_station_visits(gtfs, start, end), start, end)
    stops_connected_to_stations_map(gtfs, start, end, ignore_stations=busiest_train_stations,
                                    station_offset_range=(0, 30*60))

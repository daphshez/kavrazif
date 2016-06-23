"""
 find the number of buses that serve each station
"""

from ilgtfs import ExtendedGTFS
from collections import defaultdict, Counter
from datetime import date
from collections import namedtuple


StationStop = namedtuple('StationStop', ['station_stop_id', 'story_stop_sequence'])


# which trips pass by train station?
# and which stops in the trip story are the stops near the trains station?
def by_train_trips(g, start_date, end_date, max_station_distance=500):
    """Returns a map from trip to a list of StationStop objects"""

    def station_stops(trip):
        stops_near_station = [stop for stop in trip.route_story.stops
                              if g.stops[stop.stop_id].train_station_distance <= max_station_distance]
        # set of train stations that the trip passes
        stations = {g.stops[stop.stop_id].nearest_train_station_id for stop in stops_near_station}
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
            hourly_average = sum(counters[station][(day, hour)] for day in (6, 0, 1, 2, 3)) / 5
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


if __name__ == '__main__':
    gtfs = ExtendedGTFS(r'data/gtfs_2016_05_25')
    gtfs.load_stops()
    gtfs.load_trips()
    start = date(2016, 6, 1)
    end = date(2016, 6, 14)
    export_train_station_visits(gtfs, train_station_visits(gtfs, start, end), start, end)

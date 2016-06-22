"""
 find the number of buses that serve each station
"""

from ilgtfs import ExtendedGTFS
from collections import defaultdict, Counter
import os
from datetime import date


def group(iterable, key_func):
    res = defaultdict(lambda: [])
    for v in iterable:
        res[key_func(v)].append(v)
    return res


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


def bus_station_visits(g, output_folder, start_date, end_date, max_distance_from_station=500):
    def find_stops_near_station(route_story):
        stops_near_stations = (stop for stop in route_story.stops
                               if g.stops[stop.stop_id].train_station_distance < max_distance_from_station)
        # group by nearest train station
        grouped_by_train_station = group(stops_near_stations,
                                         lambda stop: g.stops[stop.stop_id].nearest_train_station_id)
        res = []
        for station_route_story_stops in grouped_by_train_station.values():
            # for each station sort by distance from station
            station_route_story_stops.sort(key=lambda s: g.stops[s.stop_id].train_station_distance)
            res += [s for s in station_route_story_stops if s.stop_id == station_route_story_stops[0].stop_id]
        return res

    def find_per_station_day_hour():
        print("Running find_per_station_day_hour")
        bus_trips = [trip for trip in g.trips.values() if trip.route.route_type == 3]
        print("  number of bus trips: %d" % len(bus_trips))
        bus_trips = [trip for trip in bus_trips if
                     trip.service.end_date >= start_date and trip.service.start_date <= end_date]
        print("  number of bus trips in date range %d" % len(bus_trips))
        station_to_hourly_counter = defaultdict(lambda: Counter())
        total_visits = 0
        for trip in bus_trips:
            stops_near_stations = find_stops_near_station(trip.route_story)
            for stop in stops_near_stations:
                hour = (trip.start_time + stop.arrival_offset) // 3600
                for day in trip.service.days:
                    station_to_hourly_counter[g.stops[stop.stop_id].nearest_train_station_id][(day, hour)] += 1
            total_visits += len(trip.service.days) * len(stops_near_stations)
        print("  done. found data for %d stations" % len(station_to_hourly_counter))
        print("  total number of buses stopping near train stations in a week %d" % total_visits)
        return station_to_hourly_counter

    output_filename = os.path.join(output_folder, 'hourly_bus_station_visit_sun_thur_%s_%s.txt' %
                                   (start_date.strftime('%Y-%m-%d'), end_date.strftime('%Y-%m-%d')))
    export_station_hourly_data(g, station_hourly_average_sun_to_thurs(find_per_station_day_hour()), output_filename)


def train_station_visits(g, output_folder, start_date, end_date):
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
    print("exporting")
    output_filename = os.path.join(output_folder, 'hourly_train_arrivals_sun_thur_%s_%s.txt' %
                                   (start_date.strftime('%Y-%m-%d'), end_date.strftime('%Y-%m-%d')))
    export_station_hourly_data(g, station_hourly_average_sun_to_thurs(station_to_hourly_counter), output_filename)
    print("   done.")


if __name__ == '__main__':
    gtfs = ExtendedGTFS(r'data/gtfs_2016_05_25/israel-public-transportation.zip')
    gtfs.load_stops()
    gtfs.load_trips()
    train_station_visits(gtfs, r'data/gtfs_2016_05_25/', date(2016, 6, 1), date(2016, 6, 14))

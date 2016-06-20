"""
 find the number of buses that serve each station
"""

from ilgtfs import ExtendedGTFS
from collections import defaultdict, Counter
import os
from datetime import date


def station_visits(g, output_folder, start_date, end_date, max_distance_from_station=500):
    def find_stops_near_station(route_story):
        stops_near_stations = (stop for stop in route_story.stops
                               if g.stops[stop.stop_id].train_station_distance < max_distance_from_station)
        # group by nearest train station (also include the distance from the station so it will be easier to pick)
        grouped_by_train_station = defaultdict(lambda: [])
        for route_story_stop in stops_near_stations:
            stop = g.stops[route_story_stop.stop_id]
            train_station = stop.nearest_train_station_id
            grouped_by_train_station[train_station].append((stop.train_station_distance,
                                                            route_story_stop.stop_sequence,     # does that make sense?
                                                            route_story_stop))
        # return the minimum distance stop for each station
        return [sorted(stops)[0][-1] for stops in grouped_by_train_station.values()]

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

    def station_hourly_average_sun_to_thurs(station_to_hourly_counter):
        print("Running station_hourly_average_sun_to_thurs")
        result = {}
        for station in station_to_hourly_counter:
            result[station] = []
            for hour in range(26):
                hourly_average = sum(station_to_hourly_counter[station][(day, hour)] for day in (6, 0, 1, 2, 3)) / 5
                result[station].append(hourly_average)
        return result

    def export_station_hourly_data(station_hourly_data):
        print("Running export_station_hourly_data")
        output_filename = os.path.join(output_folder, 'hourly_station_visit_sun_thur_%s_%s.txt' %
                                       (start_date.strftime('%Y-%m-%d'), end_date.strftime('%Y-%m-%d')))
        with open(output_filename, 'w', encoding='utf8') as f:
            field_names = ['station_stop_id', 'station_name', 'daily_total'] + [('h%d' % h) for h in range(26)]
            f.write(','.join(field_names) + '\n')
            for station_id in station_hourly_data:
                station = g.stops[station_id]
                line = ','.join([str(station_id), station.stop_name] +
                                [str(sum(station_hourly_data[station_id]))] +
                                [str(count) for count in station_hourly_data[station_id]])
                f.write(line + '\n')

    export_station_hourly_data(station_hourly_average_sun_to_thurs(find_per_station_day_hour()))


if __name__ == '__main__':
    gtfs = ExtendedGTFS(r'data/gtfs_2016_05_25/israel-public-transportation.zip')
    gtfs.load_stops()
    gtfs.load_trips()
    station_visits(gtfs, r'data/gtfs_2016_05_25/', date(2016, 6, 1), date(2016, 6, 14))

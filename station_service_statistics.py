from ilgtfs import ExtendedGTFS
from collections import defaultdict, Counter

max_distance_from_station = 500


# todo: handle start and end date
def station_visits(g, output_filename):
    """
    :param g: ilgtfs.ExtendedGTFS
    :param output_filename: str
    :return:
    """

    def find_per_station_day_hour():
        print("Running find_per_station_day_hour")
        bus_trips = (trip for trip in g.trips.values() if trip.route.route_type == 3)
        station_to_hourly_counter = defaultdict(lambda: Counter())
        total_visits = 0
        for trip in bus_trips:
            trip_story = trip.trip_story
            stops_near_stations = [stop for stop in trip_story
                                   if g.stops[stop.stop_id].distance_from_train_station < max_distance_from_station]
            for stop in stops_near_stations:
                hour = (trip.start_time + stop.arrival_offset) // 3600
                for day in trip.service.days:
                    station_to_hourly_counter[g.stops[stop.stop_id].nearest_train_station][(day, hour)] += 1
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
    station_visits(gtfs, r'data/gtfs_2016_05_25/hourly_station_visit_statistics_sun_thur.txt')

import zipfile
import csv
import io
import datetime
from collections import defaultdict, namedtuple

from ilgtfs import StopTime, TripStoryStop, ExtendedGTFS
import geo


# Trip stories are a list of stops with arrival and departure time as offset from the beginning of the trip
# Trip stories are build from stop times, but:
#   you can see which trips have the same story
#   the trip stories file is much smaller than the stop times file
# trip_to_trip_story  is in fact a 1-to-1 table to trip (could have added the fields to trips.txt)
def build_trip_stories(gtfs: ExtendedGTFS):
    # Returns dictionary from trip_id to a list of csv records
    def read_trip_id_to_stop_times():
        with zipfile.ZipFile(gtfs.filename) as z:
            with z.open('stop_times.txt') as f:
                reader = csv.DictReader(io.TextIOWrapper(f, 'utf8'))
                print("Reading stop_times file")
                trip_id_to_stop_times = {}
                j = 0
                for j, record in enumerate(reader):
                    trip_id_to_stop_times.setdefault(record['trip_id'], []).append(record)
                    if j % 500000 == 0:
                        print("  ", j, datetime.datetime.now())
                print("Total number of stop_times records read %d" % j)
                print("Total number of trips %d " % len(trip_id_to_stop_times))
        return trip_id_to_stop_times

    def read_trips_file():
        with zipfile.ZipFile(gtfs.filename) as z:
            with io.TextIOWrapper(z.open('trips.txt')) as f:
                header = f.readline().strip()
                assert header == "route_id,service_id,trip_id,direction_id,shape_id", header
                return ((line.split(',')[2], line.strip()) for line in f.readlines())

    trip_id_to_trip_story = read_trip_id_to_stop_times()

    bad_sequences = 0
    missing_trips = 0
    trip_story_to_id = {}

    with open(gtfs.trip_stories_filename(), 'w') as f1, open(gtfs.full_trips_filename(), 'w') as f2:
        f1.write("trip_story_id,arrival_offset,departure_offset,stop_id,pickup_type,drop_off_type\n")
        f2.write("route_id,service_id,trip_id,direction_id,shape_id,start_time_formatted,start_time,trip_story\n")
        for i, (trip_id, trip_line) in enumerate(read_trips_file()):
            if i % 10000 == 0:
                print("  %d trips written" % i, datetime.datetime.now())

            # check if trip_id had data in stop_times
            if trip_id not in trip_id_to_trip_story:
                missing_trips += 1
                f2.write("%s,%s,%s,%s\n" % (trip_line, "", "", ""))
                continue

            csv_records = trip_id_to_trip_story[trip_id]
            csv_records.sort(key=lambda r: int(r['stop_sequence']))
            # check that we have all the stops (I am assuming no duplicates)
            if csv_records[0]['stop_sequence'] != '1' or int(csv_records[-1]['stop_sequence']) != len(csv_records):
                bad_sequences += 1
                f2.write("%s,%s,%s,%s\n" % (trip_line, "", "", ""))
                continue

            # get the formatted start time from the first record; we will print it to the trips file
            start_time_formatted = csv_records[0]['arrival_time']
            # convert to ilgtfs.StopTime objects
            gtfs_stop_times = [StopTime.from_csv(record) for record in csv_records]
            # get the start time in seconds since the start of the day
            start_time = gtfs_stop_times[0].arrival_time
            # convert the StopTime object to TripStoryStop object; also convert to a tuple because it's hashable
            trip_story = tuple(TripStoryStop(record.arrival_time - start_time,
                                             record.departure_time - start_time,
                                             record.stop_id,
                                             record.pickup_type,
                                             record.drop_off_type) for record in gtfs_stop_times)
            # is it a new trip story? if yes, allocate an id and write to the trip stories file
            if trip_story not in trip_story_to_id:
                trip_story_id = len(trip_story_to_id) + 1
                trip_story_to_id[trip_story] = trip_story_id
                for trip_story_stop in trip_story:
                    f1.write("%d,%d,%d,%d,%s,%s\n" % (trip_story_id, trip_story_stop.arrival_offset,
                                                      trip_story_stop.departure_offset,
                                                      trip_story_stop.stop_id,
                                                      trip_story_stop.pickup_type,
                                                      trip_story_stop.drop_off_type))
            # write to the file that maps trips to trip story ids
            f2.write("%s,%s,%s,%s\n" % (trip_line, start_time_formatted, start_time, trip_story_to_id[trip_story]))

        print("Total number of trip stories %d" % len(trip_story_to_id))
        print("Total number of trip story points %d" % sum(len(story) for story in trip_story_to_id))
        print("Bad sequences %d" % bad_sequences)
        print("Trips without stop times %d" % missing_trips)


def build_full_stops(gtfs):
    if gtfs.trips is None:
        gtfs.load_trips()

    if gtfs.stops is None:
        gtfs.load_basic_stops()

    def find_train_stations():
        print("Finding train stations")
        train_trips = (trip for trip in gtfs.trips.values() if trip.route.route_type == 2)
        train_trip_story_ids = set(trip.trip_story_id for trip in train_trips)
        train_station_stops = set()
        for trip_story_id in train_trip_story_ids:
            for trip_story_stop in gtfs.trip_stories[trip_story_id]:
                train_station_stops.add(trip_story_stop.stop_id)
        print("%d train stations found" % len(train_station_stops))
        return train_station_stops

    def find_distance_from_train_station(train_stations):
        train_stations_stops = (stop for stop in gtfs.stops.values() if stop.stop_id in train_stations)
        train_station_points = [(stop.stop_id, geo.GeoPoint(stop.stop_lat, stop.stop_lon))
                                for stop in train_stations_stops]

        print("finding distance from train stations")
        result = {}
        for stop in gtfs.stops.values():
            if stop.stop_id in train_stations:
                result[stop.stop_id] = (0, stop.stop_id)
            else:
                stop_point = geo.GeoPoint(stop.stop_lat, stop.stop_lon)
                min_distance, nearest_station = None, None
                for train_station, train_station_point in train_station_points:
                    distance = train_station_point.distance_to(stop_point)
                    if min_distance is None or distance < min_distance:
                        nearest_station = train_station
                        min_distance = distance
                result[stop.stop_id] = (min_distance, nearest_station)
        return result

    def find_stop_routes():
        print("Finding routes for stops")
        result = defaultdict(lambda: set())
        for trip in gtfs.trips.values():
            for trip_story_stop in trip.trip_story:
                result[trip_story_stop.stop_id].add(trip.route)
        return result

    def export_full_stops(train_station_distance, stop_routes):
        with open(gtfs.full_stops_filename(), 'w', encoding='utf8') as outf:
            outf.write('stop_id,stop_code,stop_name,stop_desc,stop_lat,stop_lon,location_type,parent_station,zone_id,' +
                       'nearest_train_station,train_station_distance,routes_here\n')
            for stop in gtfs.stops.values():
                line = ','.join([
                    str(stop.stop_id),
                    stop.stop_code,
                    stop.stop_name,
                    stop.stop_desc,
                    str(stop.stop_lat),
                    str(stop.stop_lon),
                    stop.location_type,
                    stop.parent_station,
                    stop.zone_id,
                    str(train_station_distance[stop.stop_id][1]),
                    str(int(train_station_distance[stop.stop_id][0])),
                    ' '.join(route.line_number for route in stop_routes[stop.stop_id])
                ])
                outf.write(line + '\n')

    export_full_stops(find_distance_from_train_station(find_train_stations()), find_stop_routes())


def extend_routes(gtfs: ExtendedGTFS):
    print("Extending routes")
    gtfs.load_basic_routes()
    gtfs.load_trips()

    def find_route_trip_stories():
        result = defaultdict(lambda: set())
        for trip in gtfs.trips.values():
            result[trip.route].add(trip.trip_story_id)
        print("Trip stories found for %d routes" % len(result))
        return result

    def find_days_of_week():
        result = defaultdict(lambda: set())
        for trip in gtfs.trips.values():
            result[trip.route].update(trip.service.days)
        return result

    def find_date_range():
        result = {}
        for trip in gtfs.trips.values():
            if trip.route in result:
                current = result[trip.route]
                result[trip.route] = (min(trip.service.start_date, current[0]),
                                      max(trip.service.end_date, current[1]))
            else:
                result[trip.route] = (trip.service.start_date, trip.service.end_date)
        return result

    def export(trip_stories, date_ranges, days):
        fields = ["route_id", "agency_id", "route_short_name", "route_long_name", "route_desc", "route_type",
                  "trip_stories_number", "trip_stories",
                  "start_date", "end_date",
                  "number_of_days", "days"]
        with open(gtfs.at_path(ExtendedGTFS.full_routes_filename), 'w', encoding='utf8') as f:
            f.write(','.join(fields) + '\n')
            for route in gtfs.routes.values():
                date_range = date_ranges.get(route, None)
                values = [route.route_id, route.agency_id, route.line_number, route.route_long_name, route.route_desc,
                          route.route_type,
                          len(trip_stories[route]),
                          ' '.join(str(story) for story in trip_stories[route]),
                          date_range[0].strftime('%Y-%m-%d') if date_range is not None else '',
                          date_range[1].strftime('%Y-%m-%d') if date_range is not None else '',
                          len(days[route]),
                          ' '.join(str(day) for day in days[route])]
                str_values = [str(v) for v in values]
                f.write(','.join(str_values) + '\n')

    export(find_route_trip_stories(), find_date_range(), find_days_of_week())


def find_kavrazif_routes(gtfs: ExtendedGTFS, max_distance_from_train_station=500):
    print("stating find_kavrazif_routes")
    gtfs.load_trip_stories()
    gtfs.load_routes()
    gtfs.load_stops()

    KavRazif = namedtuple('KavRazif', 'id line_number station_id')
    Result = namedtuple('Result', 'route,trip_story_id,trip_story_stop,kavrazif_record')

    def stop_object(trip_story_stop):
        return gtfs.stops[trip_story_stop.stop_id]

    def load_kavrazif_records():
        with open('data/kavrazif_lines.txt', encoding='utf8') as f:
            return [KavRazif(record['kavrazif_id'], record['line_number'].strip(), int(record['station_id']))
                    for record in csv.DictReader(f)]

    def log_unmatched_kavrazif(kavrazif_records, matched_records):
        missing_records = [record for record in kavrazif_records if record not in matched_records]
        if len(missing_records) > 0:
            print("  %d kavrazif records weren't updated, missing records: %s" %
                  (len(missing_records), missing_records))

    def trip_story_stops_near_stations(trip_story):
        stops_by_nearest_station_id = defaultdict(lambda: [])
        for stop in trip_story:
            if stop_object(stop).train_station_distance < max_distance_from_train_station:
                nearest_station = stop_object(stop).nearest_train_station_id
                stops_by_nearest_station_id[nearest_station].append(stop)
        station_to_nearest_trip_story_stop = []
        for station_id in stops_by_nearest_station_id:
            nearest_stop = min(stops_by_nearest_station_id[station_id],
                               key=lambda s: stop_object(s).train_station_distance)
            station_to_nearest_trip_story_stop.append((nearest_stop, station_id))
        return station_to_nearest_trip_story_stop

    def load_route_id_and_station_id(kavrazif_records):
        result = []
        # line numbers
        line_numbers = set(record.line_number for record in kavrazif_records)
        # routes with the correct line number
        routes = [route for route in gtfs.routes.values() if route.line_number in line_numbers]
        print("  there are %d routes with a line number match" % len(routes))
        for route in routes:
            # find train stations that have route with this name nearby as a kavrazif route
            possible_train_stations = {record.station_id: record for record in kavrazif_records
                                       if record.line_number == route.line_number}
            for trip_story_id in route.trip_stories:
                trip_story = gtfs.trip_stories[trip_story_id]
                for trip_story_stop, station_id in trip_story_stops_near_stations(trip_story):
                    if station_id in possible_train_stations:
                        r = Result(route, trip_story_id, trip_story_stop, possible_train_stations[station_id])
                        result.append(r)
        print("  found %d route_id and train_station pairs" % len(result))
        return result

    def export2():
        fields = ['route_id', 'route_desc', 'agency_id', 'agency_name', 'route_short_name', 'route_long_name',
                  'route_type', 'kavrazif_id', 'train_station_id', 'train_station_name', 'stop_sequence',
                  'stop_sequence_from_end', 'trip_story_id']

        data = load_route_id_and_station_id(load_kavrazif_records())
        with open(gtfs.at_path('kavrazif_routes.txt'), 'w', encoding='utf8') as f:
            writer = csv.DictWriter(f, fieldnames=fields, lineterminator='\n')
            writer.writeheader()
            for r in data:
                route = r.route
                station = gtfs.stops[stop_object(r.trip_story_stop).nearest_train_station_id]
                sequence_from_end = len(gtfs.trip_stories[r.trip_story_id]) - r.trip_story_stop.stop_sequence + 1
                record = {
                    'route_id': route.route_id,
                    'route_desc': route.route_desc,
                    'agency_id': route.agency.agency_id,
                    'agency_name': route.agency.agency_name,
                    'route_short_name': route.line_number,
                    'route_long_name': route.route_long_name,
                    'route_type': route.route_type,
                    'kavrazif_id': r.kavrazif_record.id,
                    'train_station_id': r.kavrazif_record.station_id,
                    'train_station_name': station.stop_name,
                    'stop_sequence': r.trip_story_stop.stop_sequence,
                    'stop_sequence_from_end': sequence_from_end,
                    'trip_story_id': r.trip_story_id
                }
                writer.writerow(record)

    export2()


if __name__ == '__main__':
    find_kavrazif_routes(ExtendedGTFS('data/gtfs_2016_05_25/israel-public-transportation.zip'))

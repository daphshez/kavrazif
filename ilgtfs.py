import csv
import zipfile
import io
import datetime
import os
from collections import namedtuple
import geo

route_types = {0: 'LightRailway', 2: 'IsraelRail', 3: 'Bus', 4: 'Monish'}


class Agency:
    def __init__(self, agency_id, agency_name):
        self.agency_id = agency_id
        self.agency_name = agency_name

    @classmethod
    def from_csv(cls, csv_record):
        return cls(int(csv_record['agency_id']), csv_record['agency_name'])


# route_id,agency_id,route_short_name,route_long_name,route_desc,route_type,route_color
class Route:
    def __init__(self, route_id, agency, route_short_name, route_long_name, route_desc, route_type):
        self.route_type = route_type
        self.route_desc = route_desc
        self.route_long_name = route_long_name
        self.route_id = route_id
        self.agency = agency
        self.route_short_name = route_short_name

    def __repr__(self):
        return "<Route %d>" % self.route_id

    @classmethod
    def from_csv(cls, csv_record, agencies):
        agency_id = int(csv_record['agency_id'])
        return cls(int(csv_record['route_id']),
                   agencies[agency_id],
                   csv_record['route_short_name'], csv_record['route_long_name'],
                   csv_record['route_desc'], int(csv_record['route_type']))


class Trip:
    def __init__(self, route, service, trip_id, direction_id, shape):
        self.route_id = route
        self.service = service
        self.trip_id = trip_id
        self.direction_id = direction_id
        self.shape = shape
        self.stop_times_ids = None
        self.stop_times = None

    @classmethod
    def from_csv(cls, csv_record, routes, services, shapes):
        route_id = int(csv_record['route_id'])
        route = routes.get(route_id, route_id)

        service_id = int(csv_record['service_id'])
        service = services.get(service_id, service_id)

        shape_id = int(csv_record['shape_id']) if csv_record['shape_id'] != '' else -1
        shape = shapes.get(shape_id, shape_id)

        return cls(route,
                   service,
                   csv_record['trip_id'],
                   int(csv_record['direction_id']),
                   shape)


class FullTrip:
    def __init__(self, route, service, trip_id, direction_id, trip_story_id, trip_story, start_time):
        self.route = route
        self.service = service
        self.trip_id = trip_id
        self.direction_id = direction_id
        self.trip_story_id = trip_story_id
        self.trip_story = trip_story
        self.start_time = start_time

    @classmethod
    def from_csv(cls, csv_record, routes, services, trip_stories):
        route_id = int(csv_record['route_id'])
        route = routes[route_id]

        service_id = int(csv_record['service_id'])
        service = services[service_id]

        trip_story_id = int(csv_record['trip_story'])
        trip_story = trip_stories[trip_story_id]

        return cls(route,
                   service,
                   csv_record['trip_id'],
                   int(csv_record['direction_id']),
                   trip_story_id,
                   trip_story,
                   int(csv_record['start_time']))


class Service:
    weekday_names = dict(zip('monday tuesday wednesday thursday friday saturday sunday'.split(), range(7)))

    def __init__(self, service_id, days, start_date, end_date):
        self.end_date = end_date
        self.start_date = start_date
        self.days = days
        self.service_id = service_id

    @classmethod
    def from_csv(cls, csv_record):
        service_id = int(csv_record['service_id'])
        days = {Service.weekday_names[day] for day in Service.weekday_names.keys() if csv_record[day] == '1'}
        start_date = datetime.datetime.strptime(csv_record['start_date'], "%Y%m%d").date()
        end_date = datetime.datetime.strptime(csv_record['end_date'], "%Y%m%d").date()
        return cls(service_id, days, start_date, end_date)


class StopTime:
    # trip_id,arrival_time,departure_time,stop_id,stop_sequence,pickup_type,drop_off_type
    def __init__(self, arrival_time, departure_time, stop_id, stop_sequence, pickup_type, drop_off_type):
        self.drop_off_type = drop_off_type
        self.pickup_type = pickup_type
        self.stop_sequence = stop_sequence
        self.stop_id = stop_id
        self.departure_time = departure_time
        self.arrival_time = arrival_time

    @classmethod
    def from_csv(cls, csv_record):
        def parse_timestamp(timestamp):
            """Returns second since start of day"""
            # We need to manually parse because there's hours >= 24; but ain't Python doing it beautifully?
            (hour, minute, second) = (int(f) for f in timestamp.split(':'))
            return hour * 60 * 60 + minute * 60 + second

        arrival_time = parse_timestamp(csv_record['arrival_time'])
        departure_time = parse_timestamp(csv_record['departure_time'])
        stop_id = int(csv_record['stop_id'])
        stop_sequence = int(csv_record['stop_sequence'])
        pickup_type = csv_record['pickup_type']
        drop_off_type = csv_record['drop_off_type']
        return cls(arrival_time, departure_time, stop_id, stop_sequence, pickup_type, drop_off_type)


class Stop:
    def __init__(self, stop_id, stop_code, stop_name, stop_desc, stop_lat, stop_lon, location_type, parent_station,
                 zone_id):
        self.stop_id = stop_id
        self.stop_code = stop_code
        self.stop_name = stop_name
        self.stop_desc = stop_desc
        self.stop_lat = stop_lat
        self.stop_lon = stop_lon
        self.location_type = location_type
        self.parent_station = parent_station
        self.zone_id = zone_id
        self.is_train_station = None
        self.nearest_train_station = None
        self.distance_from_train_station = None
        self.routes_stopping_here = set()

    @classmethod
    def from_csv(cls, csv_record):
        stop_id = int(csv_record['stop_id'])
        field_names = "stop_code,stop_name,stop_desc,stop_lat,stop_lon,location_type,parent_station,zone_id".split(',')
        fields = [csv_record[field] for field in field_names]
        return cls(stop_id, *fields)


class Shape:
    def __init__(self, shape_id):
        self.shape_id = shape_id
        self.coordinates = {}

    def add_coordinate(self, point, sequence):
        self.coordinates[sequence] = point

    # shape_id,shape_pt_lat,shape_pt_lon,shape_pt_sequence
    @classmethod
    def from_csv(cls, csv_record, current_shapes):
        shape_id = int(csv_record['shape_id'])
        point = (float(csv_record['shape_pt_lat']), float(csv_record['shape_pt_lon']))
        sequence = int(csv_record['shape_pt_sequence'])
        shape = current_shapes.setdefault(shape_id, Shape(shape_id))
        shape.add_coordinate(point, sequence)


class TripStoryStop(namedtuple('_TripStoryStop', "arrival_offset,departure_offset,stop_id,pickup_type,drop_off_type")):
    @classmethod
    def from_csv(cls, csv_record):
        trip_story_id = int(csv_record['trip_story_id'])
        field_names = "arrival_offset,departure_offset,stop_id,pickup_type,drop_off_type".split(',')
        fields = [csv_record[field] for field in field_names]
        fields = [int(field) if field != '' else 0 for field in fields]
        return trip_story_id, cls(*fields)


def read_stop_times(reader, trips):
    print("  reading records from file")
    records_by_trip_id = {}
    for i, record in enumerate(reader):
        records_by_trip_id.setdefault(record['trip_id'], []).append(StopTime.from_csv(record))
        if i % 100000 == 0:
            print(datetime.datetime.now())

    print("  %d records read for %d trips" %
          (sum(len(x) for x in records_by_trip_id.values()), len(records_by_trip_id)))

    stop_times_to_trips = {}
    for trip_id, records in records_by_trip_id.items():
        records.sort(key=lambda stop_time: stop_time.stop_sequence)
        if records[0].stop_sequence != 1 or records[-1].stop_sequence != len(records):
            print("Bad stop time sequence %s" % records)
            continue
        stop_times_to_trips.setdefault(tuple(records), []).append(trip_id)
    print("There are %d stop time sequences" % len(stop_times_to_trips))

    for i, (stop_times, stop_time_trip_ids) in enumerate(stop_times_to_trips.items()):
        for trip_id in stop_time_trip_ids:
            trips[trip_id].stop_times_id = i
            trips[trip_id].stop_times = stop_times


class GTFS:
    def __init__(self, filename):
        self.filename = filename
        self.agencies = None
        self.routes = None
        self.shapes = None
        self.services = None
        self.trips = None
        self.stops = None
        self.trip_stories = None
        self.train_stations_found = False

    def load_routes(self):
        with zipfile.ZipFile(self.filename) as z:
            print("Loading agencies")
            with z.open('agency.txt') as f:
                reader = csv.DictReader(io.TextIOWrapper(f, 'utf8'))
                self.agencies = {agency.agency_id: agency for agency in (Agency.from_csv(record) for record in reader)}
            print("%d agencies loaded" % len(self.agencies))

            print("Loading routes")
            with z.open('routes.txt') as f:
                reader = csv.DictReader(io.TextIOWrapper(f, 'utf8'))
                self.routes = {route.route_id: route for route in (Route.from_csv(record, self.agencies)
                                                                   for record in reader)}
            print("%d routes loaded" % len(self.routes))

    def load_shapes(self):
        self.shapes = {}
        with zipfile.ZipFile(self.filename) as z:
            print("Loading shapes")
            with z.open('shapes.txt') as f:
                reader = csv.DictReader(io.TextIOWrapper(f, 'utf8'))
                for record in reader:
                    Shape.from_csv(record, self.shapes)
            print("%d shapes loaded" % len(self.shapes))

    def load_services(self):
        with zipfile.ZipFile(self.filename) as z:
            print("Loading services")
            with z.open('calendar.txt') as f:
                reader = csv.DictReader(io.TextIOWrapper(f, 'utf8'))
                self.services = {service.service_id: service for service in
                                 (Service.from_csv(record) for record in reader)}
            print("%d services loaded" % len(self.services))

    def load_trips(self):
        if self.services is None:
            self.load_services()

        with zipfile.ZipFile(self.filename) as z:
            print("Loading trips")
            with z.open('trips.txt') as f:
                reader = csv.DictReader(io.TextIOWrapper(f, 'utf8'))
                trips = {trip.trip_id: trip for trip in (Trip.from_csv(record,
                                                                       self.routes,
                                                                       self.services,
                                                                       self.shapes) for record in reader)}
            print("%d trips loaded" % len(trips))

    def load_stops(self):
        with zipfile.ZipFile(self.filename) as z:
            print("Loading stops")
            with z.open('stops.txt') as f:
                reader = csv.DictReader(io.TextIOWrapper(f, 'utf8'))
                self.stops = {stop.stop_id: stop for stop in (Stop.from_csv(record) for record in reader)}
            print("%d stops loaded" % len(self.stops))

    def load_stop_times(self):
        if self.trips is None:
            self.load_trips()
        with zipfile.ZipFile(self.filename) as z:
            print("Loading stop times")
            with z.open('stop_times.txt') as f:
                read_stop_times(csv.DictReader(io.TextIOWrapper(f, 'utf8')), self.trips)

    def load_full_trips(self):
        if self.services is None:
            self.load_services()

        if self.routes is None:
            self.load_routes()

        print("Loading trip stories")
        self.trip_stories = {}
        with open(self.trip_stories_filename(), encoding='utf8') as f:
            for record in csv.DictReader(f):
                trip_story_id, trip_story_stop = TripStoryStop.from_csv(record)
                self.trip_stories.setdefault(trip_story_id, []).append(trip_story_stop)
        print("%d trip_stories loaded" % len(self.trip_stories))

        print("Loading full trips")
        with open(self.full_trips_filename(), encoding='utf8') as f:
            reader = csv.DictReader(f)
            self.trips = {trip.trip_id: trip for trip in (FullTrip.from_csv(record,
                                                                            self.routes, self.services,
                                                                            self.trip_stories)
                                                          for record in reader)}
        print("%d full trips loaded" % len(self.trips))

    def trip_stories_filename(self):
        return os.path.join(self.filename, os.pardir, 'trip_stories.txt')

    def full_trips_filename(self):
        return os.path.join(self.filename, os.pardir, 'full_trips.txt')

    def find_train_stations(self):
        if self.trips is None:
            self.load_full_trips()

        if self.stops is None:
            self.load_stops()

        print("Finding train stations")
        train_trips = (trip for trip in self.trips.values() if trip.route.route_type == 2)
        train_trip_story_ids = set(trip.trip_story_id for trip in train_trips)
        for stop in self.stops.values():
            stop.is_train_station = False
        for trip_story_id in train_trip_story_ids:
            for trip_story_stop in self.trip_stories[trip_story_id]:
                self.stops[trip_story_stop.stop_id].is_train_station = True
        self.train_stations_found = True

    def find_distance_from_train_station(self):
        if not self.train_stations_found:
            self.find_train_stations()
        train_station_points = [(stop, geo.GeoPoint(stop.stop_lat, stop.stop_lon)) for stop in self.stops.values()
                                if stop.is_train_station]

        print("finding distance from train stations")
        for stop in self.stops.values():
            if stop.is_train_station:
                stop.distance_from_train_station = 0
                stop.nearest_train_station = stop
                continue

            stop_point = geo.GeoPoint(stop.stop_lat, stop.stop_lon)
            for train_station, train_station_point in train_station_points:
                distance = train_station_point.distance_to(stop_point)
                if stop.distance_from_train_station is None or distance < stop.distance_from_train_station:
                    stop.nearest_train_station = train_station
                    stop.distance_from_train_station = distance

    def find_stop_routes(self):
        if self.stops is None:
            self.load_stops()

        if self.trips is None:
            self.load_full_trips()

        print("Finding routes for stops")
        for trip in self.trips.values():
            for trip_story_stop in trip.trip_story:
                self.stops[trip_story_stop.stop_id].routes_stopping_here.add(trip.route)

    def export_full_stops(self):
        full_stops_filename = os.path.join(self.filename, os.pardir, 'full_stops.txt')
        with open(full_stops_filename, 'w', encoding='utf8') as outf:
            outf.write('stop_id,stop_code,stop_name,stop_desc,stop_lat,stop_lon,location_type,parent_station,zone_id,' +
                       'nearest_train_station,train_station_distance,routes_here\n')
            for stop in self.stops.values():
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
                    str(stop.nearest_train_station.stop_id),
                    str(int(stop.distance_from_train_station)),
                    ' '.join(route.route_short_name for route in stop.routes_stopping_here)
                ])
                outf.write(line + '\n')


if __name__ == '__main__':
    g = GTFS('data/gtfs_2016_05_01/israel-public-transportation.zip')
    g.find_train_stations()

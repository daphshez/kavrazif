import csv
import zipfile
import io
import datetime
import os
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import Column, ForeignKey, Integer, String, Date, Boolean, Float

route_types = {0: 'LightRailway', 2: 'IsraelRail', 3: 'Bus', 4: 'Monish'}

Base = declarative_base()


class Agency(Base):
    __tablename__ = 'agency'
    agency_id = Column(Integer, primary_key=True)
    agency_name = Column(String(100), nullable=False)

    def __init__(self, agency_id, agency_name):
        self.agency_id = agency_id
        self.agency_name = agency_name

    @classmethod
    def from_csv(cls, csv_record):
        return cls(int(csv_record['agency_id']), csv_record['agency_name'])


class Route(Base):
    __tablename__ = 'route'
    route_id = Column(Integer, primary_key=True)
    agency_id = Column(Integer, ForeignKey('agency.agency_id'))
    route_short_name = Column(String(10))
    route_long_name = Column(String(50))
    route_desc = Column(String(50))
    route_type = Column(Integer)

    def __init__(self, route_id, agency, route_short_name, route_long_name, route_desc, route_type):
        self.route_id = route_id
        self.agency = agency
        self.agency_id = agency.agency_id
        self.route_short_name = route_short_name
        self.route_long_name = route_long_name
        self.route_desc = route_desc
        self.route_type = route_type

    def __repr__(self):
        return "<Route %d>" % self.route_id

    def __eq__(self, other):
        return self.route_id == other.route_id

    def __hash__(self):
        return hash(self.route_id)

    @classmethod
    def from_csv(cls, csv_record, agencies):
        agency_id = int(csv_record['agency_id'])
        return cls(int(csv_record['route_id']),
                   agencies[agency_id],
                   csv_record['route_short_name'], csv_record['route_long_name'],
                   csv_record['route_desc'], int(csv_record['route_type']))


class Trip:
    def __init__(self, route, service, trip_id, direction_id, shape_id):
        self.route = route
        self.service = service
        self.trip_id = trip_id
        self.direction_id = direction_id
        self.shape_id = shape_id
        self.stop_times_ids = None
        self.stop_times = None

    @classmethod
    def from_csv(cls, csv_record, routes, services, shapes):
        route = routes[int(csv_record['route_id'])]
        service = services.get[int(csv_record['service_id'])]
        return cls(route,
                   service,
                   csv_record['trip_id'],
                   int(csv_record['direction_id']),
                   int(csv_record['shape_id']) if csv_record['shape_id'] != '' else -1)


class FullTrip(Trip):
    def __init__(self, route, service, trip_id, direction_id, shape, trip_story_id, trip_story, start_time):
        super().__init__(route, service, trip_id, direction_id, shape)
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
                   int(csv_record['shape_id']) if csv_record['shape_id'] != '' else -1,
                   trip_story_id,
                   trip_story,
                   int(csv_record['start_time']))


class Service(Base):
    __tablename__ = 'service'
    service_id = Column(Integer, primary_key=True)
    start_date = Column(Date)
    end_date = Column(Date)
    # todo: handle weekedays

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


class Stop(Base):
    __tablename__ = 'stop'
    stop_id = Column(Integer, primary_key=True)
    stop_code = Column(String)
    stop_name = Column(String)
    stop_desc = Column(String)
    stop_lat = Column(Float)
    stop_lon = Column(Float)
    location_type = Column(String)
    parent_station = Column(String)
    zone_id = Column(String)

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

    def __eq__(self, other):
        return self.stop_id == other.stop_id

    def __hash__(self):
        return hash(self.stop_id)

    @classmethod
    def from_csv(cls, csv_record):
        stop_id = int(csv_record['stop_id'])
        field_names = "stop_code,stop_name,stop_desc,stop_lat,stop_lon,location_type,parent_station,zone_id".split(',')
        fields = [csv_record[field] for field in field_names]
        return cls(stop_id, *fields)


class FullStop(Stop):
    def __init__(self, stop_id, stop_code, stop_name, stop_desc, stop_lat, stop_lon, location_type, parent_station,
                 zone_id, nearest_train_station, distance_from_train_station, routes_stopping_here):
        super().__init__(stop_id, stop_code, stop_name, stop_desc, stop_lat, stop_lon, location_type, parent_station,
                         zone_id)
        self.nearest_train_station = nearest_train_station
        self.distance_from_train_station = distance_from_train_station
        self.routes_stopping_here = routes_stopping_here

    @classmethod
    def from_csv(cls, csv_record):
        stop_id = int(csv_record['stop_id'])
        field_names = "stop_code,stop_name,stop_desc,stop_lat,stop_lon,location_type,parent_station,zone_id".split(',')
        fields = [csv_record[field] for field in field_names]
        fields += [int(csv_record['nearest_train_station']), int(csv_record['train_station_distance'])]
        fields += [csv_record['routes_here'].split(' ')]
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


class TripStoryStop:
    def __init__(self, arrival_offset, departure_offset, stop_id, pickup_type, drop_off_type):
        self.arrival_offset = arrival_offset
        self.departure_offset = departure_offset
        self.stop_id = stop_id
        self.pickup_type = pickup_type
        self.drop_off_type = drop_off_type

    def __hash__(self):
        return hash(self.as_tuple())

    def __eq__(self, other):
        return self.as_tuple() == other.as_tuple()

    def as_tuple(self):
        return self.arrival_offset, self.departure_offset, self.stop_id, self.pickup_type, self.drop_off_type

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
        print("Loading stop times. This will be verrrrry slow.")
        if self.trips is None:
            self.load_trips()
        with zipfile.ZipFile(self.filename) as z:
            print("Loading stop times")
            with z.open('stop_times.txt') as f:
                read_stop_times(csv.DictReader(io.TextIOWrapper(f, 'utf8')), self.trips)

    def to_sqlite(self, filename):
        engine = create_engine('sqlite:///' + filename)
        Base.metadata.drop_all(engine)
        Base.metadata.create_all(engine)
        Session = sessionmaker(bind=engine)
        s = Session()
        if self.agencies:
            s.add_all(self.agencies.values())
            s.commit()
        if self.routes:
            s.add_all(self.routes.values())
            s.commit()
        if self.services:
            s.add_all(self.services.values())
            s.commit()
        if self.trips:
            s.add_all(self.trips.values())
            s.commit()
        if self.stops:
            s.add_all(self.stops.values())
            s.commit()
        if self.trip_stories:
            for trip_story in self.trip_stories.values():
                s.add_all(trip_story)
            s.commit()


class ExtendedGTFS(GTFS):
    def __init__(self, filename):
        super().__init__(filename)

    def at_path(self, filename):
        return os.path.join(self.filename, os.pardir, filename)

    def trip_stories_filename(self):
        return self.at_path('trip_stories.txt')

    def full_trips_filename(self):
        return self.at_path('full_trips.txt')

    def full_stops_filename(self):
        return os.path.join(self.filename, os.pardir, 'full_stops.txt')

    def load_trips(self):
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

    def load_basic_stops(self):
        super().load_stops()

    def load_extended_stops(self):
        with open(self.full_stops_filename(), encoding='utf8') as f:
            print("Loading stops")
            reader = csv.DictReader(f)
            self.stops = {stop.stop_id: stop for stop in (FullStop.from_csv(record) for record in reader)}
            print("%d stops loaded" % len(self.stops))

    def load_stops(self):
        self.load_extended_stops()

    @property
    def train_stations(self):
        return [stop for stop in self.stops.values() if stop.is_train_station]

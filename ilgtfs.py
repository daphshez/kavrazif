import csv
import zipfile
import io
import datetime


# route_id,agency_id,route_short_name,route_long_name,route_desc,route_type,route_color
class Route:
    def __init__(self, route_id):
        self.route_id = route_id

    def __repr__(self):
        return "<Route %d>" % self.route_id

    @classmethod
    def from_csv(cls, csv_record):
        return cls(int(csv_record['route_id']))


class Agency:
    def __init__(self, agency_id, agency_name):
        self.agency_id = agency_id
        self.agency_name = agency_name

    @classmethod
    # agency_id,agency_name,agency_url,agency_timezone,agency_lang,agency_phone,agency_fare_url
    def from_csv(cls, csv_record):
        return cls(int(csv_record['agency_id']), int(csv_record['agency_name']))



class Trip:
    def __init__(self, route, service, trip_id, direction_id, shape_id):
        self.route_id = route
        self.service = service
        self.trip_id = trip_id
        self.direction_id = direction_id
        self.shape_id = shape_id

    @classmethod
    def from_csv(cls, csv_record, routes, services):
        route_id = int(csv_record['route_id'])
        if route_id in routes:
            route = routes[route_id]
        else:
            print("Error: unknown route_id %s for trip record %s" % (route_id, csv_record))
            route = None

        service_id = int(csv_record['service_id'])
        if service_id in services:
            service = services[service_id]
        else:
            print("Error: unknown service_id %s for trip record %s" % (route_id, csv_record))
            service = None

        return cls(route,
                   service,
                   int(csv_record['trip_id']),
                   int(csv_record['direction_id']),
                   int(csv_record['shape_id']) if csv_record['shape_id'] != '' else None)


# service_id,sunday,monday,tuesday,wednesday,thursday,friday,saturday,start_date,end_date
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
    pass


class Stop:
    pass


class Shape:
    pass


class GTFS:
    def __init__(self, routes, trips, services):
        self.routes = routes
        self.trips = trips
        self.services = services


def load_gtfs(filename, load_trips=True):
    with zipfile.ZipFile(filename) as z:
        with z.open('routes.txt') as f:
            reader = csv.DictReader(io.TextIOWrapper(f, 'utf8'))
            routes = {route.route_id: route for route in (Route.from_csv(record) for record in reader)}
        if load_trips:
            with z.open('calendar.txt') as f:
                reader = csv.DictReader(io.TextIOWrapper(f, 'utf8'))
                services = {service.service_id: service for service in (Service.from_csv(record) for record in reader)}
            with z.open('trips.txt') as f:
                reader = csv.DictReader(io.TextIOWrapper(f, 'utf8'))
                trips = {trip.trip_id: trip for trip in (Trip.from_csv(record, routes, services) for record in reader)}
        else:
            services = None
            trips = None

    return GTFS(routes, trips, services)

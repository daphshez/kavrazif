import csv
from collections import namedtuple
import datetime
from ilgtfs import GTFS
from csv import DictWriter

default_day = 6
minimum_seconds_to_bus = 0

VisitsAtStop = namedtuple('StoppingAtStop', ['day', 'arrival', 'departure', 'route', 'stop_id'])

TrainVisit = namedtuple('TrainVisit', ['train_visit', 'last_bus_before', 'first_bus_after', 'bus_route_id'])


# find all events of any train\bus stopping at any of the stops in stop_ids between start and end date
# returns a list of VisitsAtStop objects
def visits_at_stop(g, stop_ids, start_date, end_date):
    trip_story_to_stops = {}
    # find trip stories that go through target stops
    for trip_story_id, trip_story in g.trip_stories.items():
        for stop_sequence, trip_story_stop in enumerate(trip_story):
            if trip_story_stop.stop_id in stop_ids:
                trip_story_to_stops.setdefault(trip_story_id, []).append(trip_story_stop)

    result = []
    for trip in g.trips.values():
        if trip.trip_story_id not in trip_story_to_stops:
            continue
        if trip.service.end_date < start_date or trip.service.start_date > end_date:
            continue

        for day in trip.service.days:
            for trip_story_stop in trip_story_to_stops[trip.trip_story_id]:
                arrival = trip.start_time + trip_story_stop.arrival_offset
                departure = trip.start_time + trip_story_stop.departure_offset
                result.append(VisitsAtStop(day, arrival, departure, trip.route, trip_story_stop.stop_id))
    return result


# find the last visit in bus_visit before train_visit
def arrival_before(train_visit, bus_visits):
    visits_before = [visit for visit in bus_visits if visit.arrival < train_visit.arrival - minimum_seconds_to_bus]
    return visits_before[-1] if len(visits_before) > 0 else None


# find the first visit in bus_visit after train_visit
def departures_after(train_visit, bus_visits):
    visits_after = (visit for visit in bus_visits if visit.departure > train_visit.arrival + minimum_seconds_to_bus)
    return next(visits_after, None)


# returns a list of TrainVisit: for each train visit, and for each of the bus routes stopping near the train station
# TrainVisit contains the train visit, the last bus visit on that route before the train arrival, and the first
# bus visit after the train's arrival
def train_arrival_to_bus_visit(g, visits, day=default_day):
    # receives a list of VisitsAtStop (created by visits_at_stop)
    # creates a map from (train station) -> (route -> list of visits at the route)
    def station_to_route_to_visits():
        stations_to_route_to_visits = {}
        for visit in visits:
            if visit.route.route_type == 3 and visit.day == day:  # buses only
                stop = g.stops[visit.stop_id]
                station_routes = stations_to_route_to_visits.setdefault(stop.nearest_train_station.stop_id, {})
                station_routes.setdefault(visit.route.route_id, []).append(visit)

        for d in stations_to_route_to_visits.values():
            for l in d.values():
                l.sort(key=lambda v: (v.day, v.arrival))

        return stations_to_route_to_visits

    s2r2bs = station_to_route_to_visits()
    result = []
    for train_visit in (v for v in visits if v.route.route_type == 2 and v.day == day):
        for bus_route_id, bus_visits in s2r2bs[train_visit.stop_id].items():
            result.append(TrainVisit(train_visit,
                                     arrival_before(train_visit, bus_visits),
                                     departures_after(train_visit, bus_visits),
                                     bus_route_id))
    return result


# receives a list of TrainVisit and returns only the visits that are for train_station_stop_id
def filter_train_visits_by_train_station(train_visits, train_station_stop_id):
    return [v for v in train_visits if v.train_visit.stop_id == train_station_stop_id]


# receives a list of TrainVisit and returns only the visits that are with (train_station, bus_name) in
# train_station_and_bus_name
def filter_train_visits_by_train_station_and_bus_short_name(g, train_visits, train_station_and_bus_name):
    return [v for v in train_visits if
            (v.train_visit.stop_id, g.routes[v.bus_route_id].route_short_name) in train_station_and_bus_name]


# export a list of TrainVisit objects
def export_train_and_bus(filename, g, visits):
    def format_time(t):
        return '%02d:%02d:%02d' % (t / 3600, t % 3600 / 60, t % 60)

    writer = DictWriter(open(filename, 'w', encoding='utf8'),
                        fieldnames=['day', 'arrival', 'departure', 'train_route_id', 'train_station_id',
                                    'train_station_name', 'train_route_name',
                                    'bus_route_id', 'bus_route_name', 'bus_route_long_name',
                                    'bus_route_description', 'agency_id',
                                    'last_bus_arrival', 'first_bus_departure',
                                    'formatted_train_time', 'formatted_last_arrival', 'formatted_first_departure'],
                        lineterminator='\n')
    writer.writeheader()
    for train, bus_before, bus_after, bus_route_id in visits:
        stop = g.stops[train.stop_id]
        bus_route = g.routes[bus_route_id]
        v = {'day': train.day,
             'arrival': train.arrival,
             'departure': train.departure,
             'train_route_id': train.route.route_id,
             'train_station_id': train.stop_id,
             'train_station_name': stop.stop_name,
             'train_route_name': train.route.route_long_name,
             'bus_route_id': bus_route.route_id,
             'bus_route_name': bus_route.route_short_name,
             'bus_route_long_name': bus_route.route_long_name,
             'bus_route_description': bus_route.route_desc,
             'agency_id': bus_route.agency_id,
             'last_bus_arrival': bus_before.arrival if bus_before is not None else "",
             'first_bus_departure': bus_after.departure if bus_after is not None else "",
             'formatted_train_time': format_time(train.arrival),
             'formatted_last_arrival': format_time(bus_before.arrival) if bus_before is not None else '',
             'formatted_first_departure': format_time(bus_after.departure) if bus_after is not None else ''}
        writer.writerow(v)


# export a list of TrainVisit objects, and also individual station files
def export_all_train_visits(g, train_visits):
    export_train_and_bus('data/gtfs_2016_05_01/train_visits.txt', g, train_visits)
    for train_station in g.train_stations:
        export_train_and_bus('data/gtfs_2016_05_01/stations_0/train_visits_%d_%s.txt' %
                             (train_station.stop_id, g.stops[train_station.stop_id].stop_name),
                             g,
                             filter_train_visits_by_train_station(train_visits, train_station.stop_id))


# export only stations \ bus lines in the kavrazif configuration file
def export_kavrazif_train_visits(g, train_visits):
    with open('data/kavrazif_lines_south.txt', 'r', encoding='utf8') as f:
        reader = csv.DictReader(f)
        train_station_and_bus_name = set((int(r['station_id']), r['route_short_name']) for r in reader)
        print("There are %d train station and bus pairs to read" % len(train_station_and_bus_name))
        train_visits = filter_train_visits_by_train_station_and_bus_short_name(g, train_visits,
                                                                               train_station_and_bus_name)
        print("%d train visits left after filtering" % len(train_visits))
        export_train_and_bus('data/gtfs_2016_05_01/kavrazif_train_visits.txt', g, train_visits)


def main():
    g = GTFS('data/gtfs_2016_05_01/israel-public-transportation.zip')
    g.find_distance_from_train_station()
    stop_ids = set(stop.stop_id for stop in g.stops.values() if stop.distance_from_train_station < 300)
    visits = visits_at_stop(g, stop_ids, datetime.date(2016, 5, 2), datetime.date(2016, 5, 8))
    print("There are %d visits" % len(visits))
    train_visits = train_arrival_to_bus_visit(g, visits)
    print("There are %d train visits" % len(train_visits))
    export_kavrazif_train_visits(g, train_visits)


if __name__ == '__main__':
    main()

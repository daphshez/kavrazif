import zipfile
import csv
import io
import datetime
from ilgtfs import StopTime, GTFS, TripStoryStop

# Trip stories are a list of stops with arrival and departure time as offset from the beginning of the trip
# Trip stories are build from stop times, but:
#   you can see which trips have the same story
#   the trip stories file is much smaller than the stop times file
# trip_to_trip_story  is in fact a 1-to-1 table to trip (could have added the fields to trips.txt)


# Returns dictionary from trip_id to a list of csv records
def read_trip_id_to_stop_times(gtfs_filename):
    with zipfile.ZipFile(gtfs_filename) as z:
        with z.open('stop_times.txt') as f:
            reader = csv.DictReader(io.TextIOWrapper(f, 'utf8'))
            print("Reading stop_times file")
            trip_id_to_stop_times = {}
            i = 0
            for i, record in enumerate(reader):
                trip_id_to_stop_times.setdefault(record['trip_id'], []).append(record)
                if i % 500000 == 0:
                    print("  ", i, datetime.datetime.now())
            print("Total number of stop_times records read %d" % i)
            print("Total number of trips %d " % len(trip_id_to_stop_times))
    return trip_id_to_stop_times


# Converts a ilgtfs.StopTime object to a TripStoryStop object
def stop_time_to_trip_story_stop(stop_time, start_time):
    return TripStoryStop(stop_time.arrival_time - start_time,
                         stop_time.departure_time - start_time,
                         stop_time.stop_id,
                         stop_time.pickup_type,
                         stop_time.drop_off_type)


def read_trips_file(gtfs_filename):
    with zipfile.ZipFile(gtfs_filename) as z:
        with io.TextIOWrapper(z.open('trips.txt')) as f:
            header = f.readline().strip()
            assert header == "route_id,service_id,trip_id,direction_id,shape_id", header
            return ((line.split(',')[2], line.strip()) for line in f.readlines())


def build_trip_stories(gtfs_filename):
    gtfs = GTFS(gtfs_filename)
    with open(gtfs.trip_stories_filename(), 'w') as f1, open(gtfs.full_trips_filename(), 'w') as f2:
        f1.write("trip_story_id,arrival_offset,departure_offset,stop_id,pickup_type,drop_off_type\n")
        f2.write("route_id,service_id,trip_id,direction_id,shape_id,start_time_formatted,start_time,trip_story\n")
        trip_id_to_trip_story = read_trip_id_to_stop_times(gtfs_filename)
        bad_sequences = 0
        missing_trips = 0
        trip_story_to_id = {}
        for i, (trip_id, trip_line) in enumerate(read_trips_file(gtfs_filename)):
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
            trip_story = tuple(stop_time_to_trip_story_stop(record, start_time) for record in gtfs_stop_times)
            # is it a new trip story? if yes, allocate an id and write to the trip stories file
            if trip_story not in trip_story_to_id:
                trip_story_id = len(trip_story_to_id)
                trip_story_to_id[trip_story] = trip_story_id
                for trip_story_stop in trip_story:
                    f1.write("%d,%d,%d,%d,%s,%s\n" % (trip_story_id, trip_story_stop.arrival_offset,
                                                      trip_story_stop.departure_offset,
                                                      trip_story_stop.stop_id,
                                                      trip_story_stop.pickup_type,
                                                      trip_story_stop.drop_off_type))
            # write to the file that maps trips to trip story ids
            trip_story_id = trip_story_to_id[trip_story]
            f2.write("%s,%s,%s,%s\n" % (trip_id, start_time_formatted, start_time, trip_story_id))

        print("Total number of trip stories %d" % len(trip_story_to_id))
        print("Total number of trip story points %d" % sum(len(story) for story in trip_story_to_id))
        print("Bad sequences %d" % bad_sequences)
        print("Trips without stop times %d" % missing_trips)


if __name__ == '__main__':
    build_trip_stories(gtfs_filename='data/gtfs_2016_05_01/israel-public-transportation.zip')

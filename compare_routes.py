from ilgtfs import ExtendedGTFS, FullRoute
from Levenshtein import distance, editops


def compare_routes(g: ExtendedGTFS, route1_id: int, route2_id: int):
    def route_str(route_id: int):
        route = g.routes[route_id]      # type: FullRoute
        assert len(route.trip_stories) == 1
        trip_story_id = route.trip_stories[0]
        return ''.join(chr(stop.stop_id) for stop in g.trip_stories[trip_story_id])

    s1, s2 = route_str(route1_id), route_str(route2_id)
    return distance(s1, s2), editops(s1, s2)

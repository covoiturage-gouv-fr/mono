import pytest
import requests

from pipelines.helpers.osrm import Pair, Route, fetch_route, parse_route, route_url

PAIR = Pair(2.308475, 48.850452, 2.35, 48.85)


def ok_payload(nodes, coords, distance=3969.2, duration=657.6):
    return {
        "code": "Ok",
        "routes": [{
            "distance": distance,
            "duration": duration,
            "geometry": {"type": "LineString", "coordinates": coords},
            "legs": [{"annotation": {"nodes": nodes}}],
        }],
    }


class FakeResponse:
    def __init__(self, status_code, payload):
        self.status_code = status_code
        self._payload = payload

    def json(self):
        return self._payload


class FakeSession:
    def __init__(self, responses):
        self.responses = list(responses)
        self.calls = []

    def get(self, url, params=None, timeout=None):
        self.calls.append((url, params))
        r = self.responses.pop(0)
        if isinstance(r, Exception):
            raise r
        return r


@pytest.fixture(autouse=True)
def no_sleep(monkeypatch):
    monkeypatch.setattr("time.sleep", lambda s: None)


def test_route_url_puts_lng_before_lat():
    assert route_url("http://osrm", PAIR) == (
        "http://osrm/route/v1/driving/2.308475,48.850452;2.35,48.85"
    )


def test_route_url_ignores_trailing_slash():
    assert route_url("http://osrm/", PAIR).startswith("http://osrm/route/")


def test_parse_route_returns_nodes_coords_distance_duration():
    route = parse_route(ok_payload([10, 11, 12], [[2.3, 48.8], [2.31, 48.81], [2.32, 48.82]]))

    assert route == Route(
        nodes=[10, 11, 12],
        coords=[(2.3, 48.8), (2.31, 48.81), (2.32, 48.82)],
        distance=3969.2,
        duration=657.6,
    )


def test_parse_route_rejects_nodes_coords_length_mismatch():
    with pytest.raises(ValueError, match="nodes"):
        parse_route(ok_payload([10, 11], [[2.3, 48.8], [2.31, 48.81], [2.32, 48.82]]))


@pytest.mark.parametrize("code", ["NoRoute", "NoSegment"])
def test_parse_route_returns_none_when_osrm_finds_no_route(code):
    assert parse_route({"code": code, "message": "x"}) is None


def test_parse_route_raises_on_unexpected_code():
    with pytest.raises(ValueError, match="InvalidQuery"):
        parse_route({"code": "InvalidQuery", "message": "x"})


def test_fetch_route_asks_for_full_geometry_and_nodes():
    session = FakeSession([FakeResponse(200, ok_payload([1, 2], [[0, 0], [1, 1]]))])

    fetch_route(session, "http://osrm", PAIR)

    _, params = session.calls[0]
    assert params == {
        "overview": "full",
        "geometries": "geojson",
        "annotations": "nodes",
        "steps": "false",
    }


def test_fetch_route_no_route_is_an_http_400_not_an_error():
    session = FakeSession([FakeResponse(400, {"code": "NoRoute", "message": "x"})])

    assert fetch_route(session, "http://osrm", PAIR) is None
    assert len(session.calls) == 1


@pytest.mark.parametrize("transient", [
    requests.ConnectionError("closed"),
    requests.Timeout("slow"),
    FakeResponse(429, {"code": "TooBig"}),
    FakeResponse(503, {}),
])
def test_fetch_route_retries_transient_failures(transient):
    session = FakeSession([transient, FakeResponse(200, ok_payload([1, 2], [[0, 0], [1, 1]]))])

    route = fetch_route(session, "http://osrm", PAIR)

    assert route.nodes == [1, 2]
    assert len(session.calls) == 2


def test_fetch_route_gives_up_after_attempts():
    session = FakeSession([requests.ConnectionError("closed")] * 3)

    with pytest.raises(requests.ConnectionError):
        fetch_route(session, "http://osrm", PAIR, attempts=3)

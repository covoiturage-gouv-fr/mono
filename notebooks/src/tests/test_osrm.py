import unittest

import pandas as pd
import requests

from helpers.osrm import (
    add_osrm_columns,
    assert_error_rate,
    count_osrm_status,
    fetch_route,
)


class FakeResponse:
    def __init__(self, payload=None, status_code=200, text="<html>503</html>"):
        self.payload = payload
        self.status_code = status_code
        self.text = text

    def json(self):
        if self.payload is None:
            raise requests.exceptions.JSONDecodeError("Expecting value", self.text, 0)
        return self.payload


class FakeSession:
    def __init__(self, handler):
        self.handler = handler
        self.calls = []

    def get(self, url, **kwargs):
        self.calls.append((url, kwargs))
        return self.handler(url, **kwargs)


def ok_payload():
    return {"code": "Ok", "routes": [
        {"duration": 600.0, "distance": 12000.0},
        {"duration": 700.0, "distance": 9000.0},
    ]}


def raise_timeout(url, **kwargs):
    raise requests.exceptions.ReadTimeout("timeout on " + url)


def fetch(session):
    return fetch_route(session, "http://osrm", 2.35, 48.85, 4.83, 45.76)


class TestFetchRoute(unittest.TestCase):

    def test_should_parse_ok_with_alternatives(self):
        result = fetch(FakeSession(lambda url, **kw: FakeResponse(ok_payload())))
        self.assertEqual(result, {
            "osrm_status": "ok",
            "osrm_duration": 600.0,
            "osrm_distance": 12000.0,
            "osrm_min_distance": 9000.0,
        })

    def test_should_build_url_and_pass_timeout(self):
        session = FakeSession(lambda url, **kw: FakeResponse(ok_payload()))
        fetch(session)
        url, kwargs = session.calls[0]
        self.assertEqual(
            url,
            "http://osrm/route/v1/car/2.35,48.85;4.83,45.76?alternatives=true&overview=false",
        )
        self.assertEqual(kwargs["timeout"], 10)

    def test_should_return_no_route_on_http_400(self):
        session = FakeSession(lambda url, **kw: FakeResponse({"code": "NoRoute"}, 400))
        result = fetch(session)
        self.assertEqual(result["osrm_status"], "no_route")
        self.assertTrue(pd.isna(result["osrm_distance"]))

    def test_should_return_no_route_on_no_segment(self):
        session = FakeSession(lambda url, **kw: FakeResponse({"code": "NoSegment"}, 400))
        self.assertEqual(fetch(session)["osrm_status"], "no_route")

    def test_should_return_error_on_html_503(self):
        session = FakeSession(lambda url, **kw: FakeResponse(None, 503))
        result = fetch(session)
        self.assertEqual(result["osrm_status"], "error")
        self.assertTrue(pd.isna(result["osrm_duration"]))
        self.assertTrue(pd.isna(result["osrm_min_distance"]))

    def test_should_return_error_on_timeout(self):
        self.assertEqual(fetch(FakeSession(raise_timeout))["osrm_status"], "error")

    def test_should_return_error_on_other_code_or_missing_routes(self):
        for payload in ({"code": "InvalidQuery"}, {"code": "Ok"}, {"code": "Ok", "routes": []}):
            with self.subTest(payload=payload):
                session = FakeSession(lambda url, **kw: FakeResponse(payload))
                self.assertEqual(fetch(session)["osrm_status"], "error")

    def test_should_not_log_url_or_coordinates(self):
        with self.assertLogs(level="WARNING") as logs:
            fetch(FakeSession(raise_timeout))
        output = "\n".join(logs.output)
        self.assertIn("ReadTimeout", output)
        for secret in ("2.35", "48.85", "http", "/route"):
            self.assertNotIn(secret, output)


class TestAddOsrmColumns(unittest.TestCase):

    def test_should_keep_index_and_order(self):
        df = pd.DataFrame({
            "start_x": [1.0, 2.0, 3.0], "start_y": [0.0, 0.0, 0.0],
            "end_x": [0.0, 0.0, 0.0], "end_y": [0.0, 0.0, 0.0],
        }, index=[10, 20, 30])

        def handler(url, **kw):
            start_x = float(url.split("/car/")[1].split(",")[0])
            return FakeResponse({"code": "Ok", "routes": [{"duration": start_x, "distance": start_x * 10}]})

        result = add_osrm_columns(df, "http://osrm", FakeSession(handler))
        self.assertEqual(list(result.index), [10, 20, 30])
        self.assertEqual(result.osrm_duration.tolist(), [1.0, 2.0, 3.0])
        self.assertEqual(result.osrm_min_distance.tolist(), [10.0, 20.0, 30.0])
        self.assertEqual(set(result.osrm_status), {"ok"})

    def test_should_handle_empty_dataframe(self):
        df = pd.DataFrame({c: [] for c in ["start_x", "start_y", "end_x", "end_y"]})
        result = add_osrm_columns(df, "http://osrm", FakeSession(lambda url, **kw: None))
        self.assertTrue(result.empty)
        for column in ["osrm_status", "osrm_duration", "osrm_distance", "osrm_min_distance"]:
            self.assertIn(column, result.columns)


def trips_df(start_xs):
    n = len(start_xs)
    return pd.DataFrame({"start_x": start_xs, "start_y": [0.0] * n, "end_x": [0.0] * n, "end_y": [0.0] * n})


def fail_when_negative(url, **kw):
    start_x = float(url.split("/car/")[1].split(",")[0])
    if start_x < 0:
        raise requests.exceptions.ConnectionError("down")
    return FakeResponse(ok_payload())


class TestCircuitBreaker(unittest.TestCase):

    def test_should_stop_calling_osrm_after_consecutive_errors(self):
        session = FakeSession(raise_timeout)
        result = add_osrm_columns(trips_df([1.0] * 10), "http://osrm", session, workers=1, max_consecutive_errors=3)
        self.assertEqual(len(session.calls), 3)
        self.assertEqual(set(result.osrm_status), {"error"})
        self.assertTrue(result.osrm_duration.isna().all())
        self.assertEqual(len(result), 10)

    def test_should_reset_count_on_success(self):
        session = FakeSession(fail_when_negative)
        df = trips_df([-1.0, -1.0, 1.0, -1.0, -1.0, 1.0])
        result = add_osrm_columns(df, "http://osrm", session, workers=1, max_consecutive_errors=3)
        self.assertEqual(len(session.calls), 6)
        self.assertEqual(result.osrm_status.tolist(), ["error", "error", "ok", "error", "error", "ok"])

    def test_should_reset_count_on_no_route(self):
        def handler(url, **kw):
            start_x = float(url.split("/car/")[1].split(",")[0])
            if start_x == 0:
                return FakeResponse({"code": "NoRoute"}, status_code=400)
            raise requests.exceptions.ConnectionError("down")

        session = FakeSession(handler)
        df = trips_df([1.0, 1.0, 0.0, 1.0, 1.0])
        add_osrm_columns(df, "http://osrm", session, workers=1, max_consecutive_errors=3)
        self.assertEqual(len(session.calls), 5)

    def test_should_log_circuit_open_once(self):
        session = FakeSession(raise_timeout)
        with self.assertLogs(level="WARNING") as logs:
            add_osrm_columns(trips_df([1.0] * 10), "http://osrm", session, workers=1, max_consecutive_errors=3)
        opened = [m for m in logs.output if "circuit open" in m]
        self.assertEqual(len(opened), 1)
        self.assertNotIn("http://osrm", opened[0])

    def test_should_bound_calls_with_several_workers(self):
        session = FakeSession(raise_timeout)
        result = add_osrm_columns(trips_df([1.0] * 50), "http://osrm", session, workers=4, max_consecutive_errors=5)
        self.assertLessEqual(len(session.calls), 5 + 4)
        self.assertEqual(set(result.osrm_status), {"error"})


class TestErrorRate(unittest.TestCase):

    def test_should_count_status(self):
        df = pd.DataFrame({"osrm_status": ["ok", "ok", "error", "no_route"]})
        self.assertEqual(count_osrm_status(df), {"ok": 2, "no_route": 1, "error": 1})

    def test_should_pass_at_exactly_threshold(self):
        assert_error_rate({"ok": 95, "no_route": 0, "error": 5})

    def test_should_raise_above_threshold(self):
        with self.assertRaises(RuntimeError) as ctx:
            assert_error_rate({"ok": 94, "no_route": 0, "error": 6})
        self.assertIn("error", str(ctx.exception))

    def test_should_noop_when_total_is_zero(self):
        assert_error_rate({"ok": 0, "no_route": 0, "error": 0})


if __name__ == "__main__":
    unittest.main()

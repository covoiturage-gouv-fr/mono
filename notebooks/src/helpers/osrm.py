import logging
import threading
from concurrent.futures import ThreadPoolExecutor

import pandas as pd
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

OSRM_COLUMNS = ["osrm_status", "osrm_duration", "osrm_distance", "osrm_min_distance"]
NO_ROUTE_CODES = ("NoRoute", "NoSegment")


def make_session():
    retry = Retry(total=3, backoff_factor=0.5, status_forcelist=[502, 503, 504], allowed_methods=["GET"])
    adapter = HTTPAdapter(max_retries=retry, pool_connections=8, pool_maxsize=8)
    session = requests.Session()
    session.mount("http://", adapter)
    session.mount("https://", adapter)
    return session


def _result(status, duration=float("nan"), distance=float("nan"), min_distance=float("nan")):
    return {
        "osrm_status": status,
        "osrm_duration": duration,
        "osrm_distance": distance,
        "osrm_min_distance": min_distance,
    }


def _error(reason):
    # Never log the URL or exception message: they contain trip coordinates.
    logging.warning("Error on osrm call: %s", reason)
    return _result("error")


def fetch_route(session, osrm_url, start_x, start_y, end_x, end_y, timeout=10):
    url = f"{osrm_url}/route/v1/car/{start_x},{start_y};{end_x},{end_y}?alternatives=true&overview=false"
    try:
        response = session.get(url, timeout=timeout)
        # OSRM answers NoRoute with HTTP 400 and a JSON body, so parse whatever the status.
        body = response.json()
    except (requests.RequestException, ValueError) as e:
        return _error(type(e).__name__)

    code = body.get("code") if isinstance(body, dict) else None
    if code in NO_ROUTE_CODES:
        return _result("no_route")
    routes = body.get("routes") if code == "Ok" else None
    if not routes:
        return _error(code or f"HTTP {response.status_code}")

    return _result(
        "ok",
        routes[0]["duration"],
        routes[0]["distance"],
        min(route["distance"] for route in routes),
    )


def add_osrm_columns(df, osrm_url, session, workers=4, max_consecutive_errors=20):
    rows = list(df[["start_x", "start_y", "end_x", "end_y"]].itertuples(index=False, name=None))
    lock = threading.Lock()
    consecutive_errors = 0

    def fetch(row):
        nonlocal consecutive_errors
        with lock:
            if consecutive_errors >= max_consecutive_errors:
                return _result("error")
        result = fetch_route(session, osrm_url, *row)
        with lock:
            consecutive_errors = consecutive_errors + 1 if result["osrm_status"] == "error" else 0
            if consecutive_errors == max_consecutive_errors:
                logging.warning("osrm circuit open after %d consecutive errors, skipping remaining trips", consecutive_errors)
        return result

    with ThreadPoolExecutor(max_workers=workers) as executor:
        results = list(executor.map(fetch, rows))
    osrm = pd.DataFrame(results, index=df.index, columns=OSRM_COLUMNS)
    return pd.concat([df, osrm], axis=1)


def count_osrm_status(df):
    counts = df["osrm_status"].value_counts()
    return {status: int(counts.get(status, 0)) for status in ("ok", "no_route", "error")}


def assert_error_rate(counts, threshold=0.05):
    total = sum(counts.values())
    if total and counts["error"] / total > threshold:
        raise RuntimeError(f"Too many osrm errors: {counts}")

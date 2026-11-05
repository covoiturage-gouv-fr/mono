from unittest.mock import MagicMock, patch

import pytest
import requests

from pipelines.helpers.api_client import ApiClient


def _client():
    return ApiClient("https://api.test", "ak", "sk")


@patch("pipelines.helpers.api_client.requests")
def test_claim_returns_none_on_204(rq):
    rq.post.side_effect = [
        MagicMock(status_code=200, json=lambda: {"access_token": "t"}),  # token
        MagicMock(status_code=204),                                       # claim
    ]
    assert _client().claim(["operator"]) is None


@patch("pipelines.helpers.api_client.requests")
def test_claim_returns_payload_on_200(rq):
    rq.post.side_effect = [
        MagicMock(status_code=200, json=lambda: {"access_token": "t"}),
        MagicMock(status_code=200, json=lambda: {"uuid": "u", "target": "operator", "params": {}}),
    ]
    assert _client().claim(["operator"])["uuid"] == "u"


# The mono API mounts apiRoute handlers under a /:api_version/ prefix, so calls
# must target /v3/... — hitting the bare root returns 404.
@patch("pipelines.helpers.api_client.requests")
def test_auth_targets_versioned_path(rq):
    rq.post.return_value = MagicMock(status_code=201, json=lambda: {"access_token": "t"})
    _client()._auth()
    assert rq.post.call_args.args[0] == "https://api.test/v3/auth/access_token"


@patch("pipelines.helpers.api_client.requests")
def test_claim_targets_versioned_path(rq):
    rq.post.side_effect = [
        MagicMock(status_code=201, json=lambda: {"access_token": "t"}),
        MagicMock(status_code=204),
    ]
    _client().claim(["operator"])
    assert rq.post.call_args_list[1].args[0] == "https://api.test/v3/exports/claim"


# Empty queue: the API returns 200 with an empty body (null result -> res.end()),
# not 204, so claim must treat an empty body as "no task" rather than r.json().
@patch("pipelines.helpers.api_client.requests")
def test_claim_returns_none_on_empty_200_body(rq):
    empty = MagicMock(status_code=200, content=b"")
    empty.json.side_effect = ValueError("Expecting value: line 1 column 1 (char 0)")
    rq.post.side_effect = [
        MagicMock(status_code=201, json=lambda: {"access_token": "t"}),  # token
        empty,                                                           # empty claim
    ]
    assert _client().claim(["operator"]) is None


def _http_error(status):
    r = MagicMock(status_code=status, content=b"{}")
    r.raise_for_status.side_effect = requests.HTTPError(str(status))
    return r


# Dex tokens expire after 5 min: a 401 mid-run must fetch a fresh token and
# replay once, not fail silently.
@patch("pipelines.helpers.api_client.requests")
def test_reauthenticates_once_on_401(rq):
    rq.post.side_effect = [
        MagicMock(status_code=201, json=lambda: {"access_token": "old"}),
        MagicMock(status_code=401, content=b"{}"),
        MagicMock(status_code=201, json=lambda: {"access_token": "new"}),
        MagicMock(status_code=200, content=b""),
    ]
    _client().complete("u", 42)
    assert rq.post.call_args_list[2].args[0] == "https://api.test/v3/auth/access_token"
    assert rq.post.call_args_list[3].kwargs["headers"] == {"Authorization": "Bearer new"}


@patch("pipelines.helpers.api_client.requests")
def test_raises_when_401_persists_after_reauth(rq):
    rq.post.side_effect = [
        MagicMock(status_code=201, json=lambda: {"access_token": "t"}),
        MagicMock(status_code=401, content=b"{}"),
        MagicMock(status_code=201, json=lambda: {"access_token": "t2"}),
        _http_error(401),
    ]
    with pytest.raises(requests.HTTPError):
        _client().claim(["operator"])


@patch("pipelines.helpers.api_client.requests")
def test_complete_raises_on_4xx(rq):
    rq.post.side_effect = [
        MagicMock(status_code=201, json=lambda: {"access_token": "t"}),
        _http_error(404),
    ]
    with pytest.raises(requests.HTTPError):
        _client().complete("u", 42)

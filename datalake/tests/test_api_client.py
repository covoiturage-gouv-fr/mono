from unittest.mock import MagicMock, patch

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

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

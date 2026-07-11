import os
import tempfile

from pipelines.helpers.datagouv_client import DataGouvClient


class FakeResponse:
    def __init__(self, payload):
        self._payload = payload

    def raise_for_status(self):
        pass

    def json(self):
        return self._payload


class FakeSession:
    """Enregistre les appels et renvoie des réponses scriptées."""

    def __init__(self, dataset_payload, upload_payload=None, put_payload=None):
        self.dataset_payload = dataset_payload
        self.upload_payload = upload_payload or {"id": "new", "title": "x"}
        self.put_payload = put_payload or {"id": "new"}
        self.calls = []

    def get(self, url, headers=None, timeout=None):
        self.calls.append(("GET", url, headers))
        return FakeResponse(self.dataset_payload)

    def post(self, url, headers=None, files=None, timeout=None):
        self.calls.append(("POST", url, headers, list(files.keys())))
        return FakeResponse(self.upload_payload)

    def put(self, url, headers=None, json=None, timeout=None):
        self.calls.append(("PUT", url, headers, json))
        return FakeResponse(self.put_payload)


def _tmpfile(name):
    d = tempfile.mkdtemp()
    p = os.path.join(d, name)
    with open(p, "w") as f:
        f.write("a;b\n1;2\n")
    return p


def test_auth_header_on_every_call():
    s = FakeSession({"resources": []})
    c = DataGouvClient("https://x/api/1", "SECRET", "ds", session=s)
    c.get_dataset()
    assert s.calls[0][2]["X-API-KEY"] == "SECRET"


def test_upload_new_resource_uses_dataset_upload_url():
    s = FakeSession({"resources": []})
    c = DataGouvClient("https://x/api/1", "k", "ds", session=s)
    c.upload(_tmpfile("2026-06.csv"))
    post = [call for call in s.calls if call[0] == "POST"][0]
    assert post[1].endswith("/datasets/ds/upload/")
    assert post[3] == ["file"]  # multipart, pas de Content-Type json
    assert "Content-Type" not in s.calls[-1][2]


def test_upload_existing_title_replaces_resource():
    s = FakeSession({"resources": [{"id": "R1", "title": "2026-06.csv"}]})
    c = DataGouvClient("https://x/api/1", "k", "ds", session=s)
    c.upload(_tmpfile("2026-06.csv"))
    post = [call for call in s.calls if call[0] == "POST"][0]
    assert post[1].endswith("/datasets/ds/resources/R1/upload/")


def test_set_metadata_puts_description():
    s = FakeSession({"resources": []})
    c = DataGouvClient("https://x/api/1", "k", "ds", session=s)
    c.set_metadata({"id": "R9", "title": "t"}, "ma description")
    put = [call for call in s.calls if call[0] == "PUT"][0]
    assert put[1].endswith("/datasets/ds/resources/R9")
    assert put[3] == {"title": "t", "description": "ma description"}

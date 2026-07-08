from pathlib import Path
from unittest.mock import MagicMock, patch

from pipelines.cmd.export_worker import process_one


def test_process_one_empty_returns_false():
    api = MagicMock()
    api.claim.return_value = None
    assert process_one(api, MagicMock(), MagicMock(), "export") is False


@patch("pipelines.cmd.export_worker.stream_csv")
def test_process_one_success_flow(stream_csv, tmp_path, monkeypatch):
    # Le worker ne doit écrire aucun fichier temporaire dans le répertoire courant
    # (régression : ./{uuid}.csv échouait sur le workdir en lecture seule du pod).
    monkeypatch.chdir(tmp_path)
    captured = {}

    def _fake_stream(conn, inner, csv_path):
        captured["csv_path"] = csv_path
        with open(csv_path, "w") as f:
            f.write("journey_id\n1\n")
    stream_csv.side_effect = _fake_stream

    api = MagicMock()
    api.claim.return_value = {"uuid": "u1", "target": "operator",
                             "params": {"start_at": "2026-01-01T00:00:00+0100",
                                        "end_at": "2026-02-01T00:00:00+0100",
                                        "operator_id": [1], "geo_selector": None}}
    s3 = MagicMock()
    ok = process_one(api, MagicMock(), s3, "export")
    assert ok is True
    # temp file lived outside cwd, and the tempdir was cleaned up on exit
    assert Path(captured["csv_path"]).parent != tmp_path
    assert not Path(captured["csv_path"]).exists()
    assert list(tmp_path.iterdir()) == []
    s3.upload_file.assert_called_once()
    assert s3.upload_file.call_args[0][2] == "u1.csv.zip"
    api.complete.assert_called_once()
    assert api.complete.call_args[0][0] == "u1"


@patch("pipelines.cmd.export_worker.stream_csv", side_effect=RuntimeError("boom"))
def test_process_one_failure_calls_fail(stream_csv, tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    api = MagicMock()
    api.claim.return_value = {"uuid": "u2", "target": "operator",
                             "params": {"start_at": "2026-01-01T00:00:00+0100",
                                        "end_at": "2026-02-01T00:00:00+0100",
                                        "operator_id": [1], "geo_selector": None}}
    assert process_one(api, MagicMock(), MagicMock(), "export") is True
    api.fail.assert_called_once()
    assert api.fail.call_args[0][0] == "u2"

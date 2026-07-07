from unittest.mock import MagicMock

from pipelines.helpers.s3 import s3_upload


def test_s3_upload_calls_put_with_key(tmp_path):
    f = tmp_path / "x.csv.zip"
    f.write_bytes(b"zipdata")
    client = MagicMock()
    s3_upload("export", "abc.csv.zip", str(f), client=client)
    client.upload_file.assert_called_once_with(str(f), "export", "abc.csv.zip")

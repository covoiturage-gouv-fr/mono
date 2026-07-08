import hashlib

import pytest

from pipelines.helpers.checksum import hash_file, verify_checksum, verify_size


def test_verify_size_passes_on_match(tmp_path):
    p = tmp_path / "f.bin"
    p.write_bytes(b"12345")
    verify_size(str(p), 5, "f")  # ne lève pas


def test_verify_size_raises_on_mismatch(tmp_path):
    p = tmp_path / "f.bin"
    p.write_bytes(b"12345")
    with pytest.raises(RuntimeError, match="Taille"):
        verify_size(str(p), 999, "f")


def test_hash_file_matches_hashlib(tmp_path):
    p = tmp_path / "f.bin"
    p.write_bytes(b"covoiturage")
    assert hash_file(str(p), "sha256") == hashlib.sha256(b"covoiturage").hexdigest()
    assert hash_file(str(p), "sha1") == hashlib.sha1(b"covoiturage").hexdigest()


def test_verify_checksum_passes_on_match(tmp_path):
    p = tmp_path / "f.bin"
    p.write_bytes(b"data")
    verify_checksum(str(p), "sha256:" + hashlib.sha256(b"data").hexdigest(), "f")
    verify_checksum(str(p), "sha1:" + hashlib.sha1(b"data").hexdigest(), "f")  # algo tiré du préfixe


def test_verify_checksum_raises_on_mismatch(tmp_path):
    p = tmp_path / "f.bin"
    p.write_bytes(b"data")
    with pytest.raises(RuntimeError, match="Checksum"):
        verify_checksum(str(p), "sha256:" + hashlib.sha256(b"AUTRE").hexdigest(), "f")


def test_verify_checksum_raises_on_malformed(tmp_path):
    p = tmp_path / "f.bin"
    p.write_bytes(b"data")
    with pytest.raises(RuntimeError, match="mal formé"):
        verify_checksum(str(p), "deadbeef", "f")  # pas de préfixe d'algo

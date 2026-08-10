import hashlib

import pytest

from streetradar.provider import ProviderPaths, data_root
from streetradar.storage import file_md5


class TestProviderPaths:
    def test_layout_is_derived_from_one_root(self, tmp_path):
        paths = ProviderPaths.for_provider("naver", root=tmp_path)

        assert paths.raw == tmp_path / "naver" / "raw"
        assert paths.processed == tmp_path / "naver" / "processed"
        assert paths.matched == tmp_path / "naver" / "matched"
        assert paths.tile == tmp_path / "naver" / "tile"

    def test_pmtiles_defaults_to_the_provider_name(self, tmp_path):
        paths = ProviderPaths.for_provider("naver", root=tmp_path)
        assert paths.pmtiles().name == "naver.pmtiles"

    def test_pmtiles_accepts_an_explicit_filename(self, tmp_path):
        paths = ProviderPaths.for_provider("ja", root=tmp_path)
        assert paths.pmtiles("ja360.pmtiles").name == "ja360.pmtiles"

    def test_ensure_creates_everything_and_is_idempotent(self, tmp_path):
        paths = ProviderPaths.for_provider("mapy", root=tmp_path).ensure()
        paths.ensure()

        for directory in (paths.raw, paths.processed, paths.matched, paths.tile):
            assert directory.is_dir()

    def test_data_root_honours_the_override(self, tmp_path, monkeypatch):
        monkeypatch.setenv("STREETRADAR_DATA_DIR", str(tmp_path))
        assert data_root() == tmp_path.resolve()


class TestFileMd5:
    def test_matches_hashlib(self, tmp_path):
        payload = b"streetradar" * 5000
        target = tmp_path / "blob.bin"
        target.write_bytes(payload)

        assert file_md5(target) == hashlib.md5(payload).hexdigest()

    def test_chunking_does_not_change_the_digest(self, tmp_path):
        """PMTiles archives run to several GB, so the file is read in chunks.
        The chunk size must not affect the result."""
        payload = b"x" * 300_000
        target = tmp_path / "blob.bin"
        target.write_bytes(payload)

        assert file_md5(target, chunk_size=1024) == file_md5(target, chunk_size=1_000_000)

    def test_empty_file(self, tmp_path):
        target = tmp_path / "empty.bin"
        target.write_bytes(b"")
        assert file_md5(target) == hashlib.md5(b"").hexdigest()

    def test_missing_file_raises(self, tmp_path):
        with pytest.raises(OSError):
            file_md5(tmp_path / "nope.bin")

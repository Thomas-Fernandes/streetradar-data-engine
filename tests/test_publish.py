"""The publish gate and the versioned handoff.

Driven against the shared in-memory bucket, with a synthetic PMTiles file — the
magic bytes plus padding — and hand-built `TilesetStats`, so nothing here needs
tippecanoe, pmtiles or a network.
"""

from __future__ import annotations

import json
from pathlib import Path

import pytest
from fakes import make_client

from streetradar.publish import (
    LATEST_KEY,
    PublishError,
    PublishResult,
    archive_key,
    publish,
    verify_tiles,
)
from streetradar.r2 import R2Client
from streetradar.tileset import TilesetSpec, TilesetStats, ZoomStats

SPEC = TilesetSpec(layer="coverage", min_zoom=0, max_zoom=16)


def make_stats(*, zooms: range | list[int] = range(0, 17), max_tile: int = 8_000) -> TilesetStats:
    by_zoom = tuple(
        ZoomStats(zoom=z, tiles=3, bytes=300, max_tile_bytes=(max_tile if z == 8 else 100))
        for z in zooms
    )
    return TilesetStats(
        tiles=sum(z.tiles for z in by_zoom),
        bytes=sum(z.bytes for z in by_zoom),
        by_zoom=by_zoom,
    )


def make_archive(directory: Path, name: str = "ja.pmtiles", *, body: bytes | None = None) -> Path:
    archive = directory / name
    archive.write_bytes(body if body is not None else b"PMTiles\x03" + b"\x00" * 200)
    return archive


def pub(
    r2: R2Client,
    archive: Path,
    *,
    provider: str = "ja",
    scope: str = "is",
    run_id: str,
    stats: TilesetStats | None = None,
    published_at: str = "2026-08-11T09:00:00+00:00",
) -> PublishResult:
    return publish(
        r2,
        provider=provider,
        scope=scope,
        run_id=run_id,
        archive=archive,
        stats=stats if stats is not None else make_stats(),
        spec=SPEC,
        published_at=published_at,
    )


class TestVerifyGate:
    def test_a_good_archive_passes(self, tmp_path: Path) -> None:
        report = verify_tiles(make_archive(tmp_path), make_stats(), SPEC)
        assert report.ok and report.problems == ()

    def test_a_missing_archive_fails(self, tmp_path: Path) -> None:
        report = verify_tiles(tmp_path / "gone.pmtiles", make_stats(), SPEC)
        assert not report.ok and "missing" in report.problems[0]

    def test_an_empty_archive_fails(self, tmp_path: Path) -> None:
        report = verify_tiles(make_archive(tmp_path, body=b""), make_stats(), SPEC)
        assert not report.ok and any("empty" in p for p in report.problems)

    def test_a_non_pmtiles_file_fails_on_magic(self, tmp_path: Path) -> None:
        report = verify_tiles(make_archive(tmp_path, body=b"not a tileset"), make_stats(), SPEC)
        assert not report.ok and any("magic" in p for p in report.problems)

    def test_zero_tiles_fails(self, tmp_path: Path) -> None:
        stats = TilesetStats(tiles=0, bytes=0, by_zoom=())
        report = verify_tiles(make_archive(tmp_path), stats, SPEC)
        assert not report.ok and any("no tiles" in p for p in report.problems)

    def test_an_oversized_tile_fails(self, tmp_path: Path) -> None:
        report = verify_tiles(make_archive(tmp_path), make_stats(max_tile=600_000), SPEC)
        assert not report.ok and any("largest tile" in p for p in report.problems)

    def test_a_zoom_above_the_spec_fails(self, tmp_path: Path) -> None:
        report = verify_tiles(make_archive(tmp_path), make_stats(zooms=range(0, 18)), SPEC)
        assert not report.ok and any("above the spec" in p for p in report.problems)


class TestPublish:
    def test_it_uploads_under_a_versioned_key_and_points_latest_at_it(self, tmp_path: Path) -> None:
        r2, fake = make_client()
        result = pub(r2, make_archive(tmp_path), run_id="20260811-0900")

        key = archive_key("ja", "is", "20260811-0900")
        assert result.key == key
        assert key in fake.store
        assert result.url == f"https://tiles.streetradar.app/{key}"
        assert result.previous_key is None and result.pruned == ()

        latest = json.loads(fake.store[LATEST_KEY]["data"])
        current = latest["providers"]["ja"]["is"]["current"]
        assert current["run_id"] == "20260811-0900"
        assert current["url"].endswith(f"{key}")
        assert current["max_zoom"] == 16
        assert latest["providers"]["ja"]["is"]["previous"] is None

    def test_a_second_run_keeps_both_generations(self, tmp_path: Path) -> None:
        r2, fake = make_client()
        pub(r2, make_archive(tmp_path, "a.pmtiles"), run_id="run-1")
        result = pub(r2, make_archive(tmp_path, "b.pmtiles"), run_id="run-2")

        assert result.previous_key == archive_key("ja", "is", "run-1")
        assert result.pruned == ()
        archives = {k for k in fake.store if k.endswith(".pmtiles")}
        assert archives == {archive_key("ja", "is", "run-1"), archive_key("ja", "is", "run-2")}

    def test_a_third_run_prunes_the_oldest(self, tmp_path: Path) -> None:
        r2, fake = make_client()
        result = None
        for i in range(3):
            result = pub(r2, make_archive(tmp_path, f"{i}.pmtiles"), run_id=f"run-{i}")
        assert result is not None
        assert result.pruned == (archive_key("ja", "is", "run-0"),)
        archives = {k for k in fake.store if k.endswith(".pmtiles")}
        assert archives == {archive_key("ja", "is", "run-1"), archive_key("ja", "is", "run-2")}

    def test_it_refuses_to_overwrite_a_live_key(self, tmp_path: Path) -> None:
        r2, _ = make_client()
        pub(r2, make_archive(tmp_path, "a.pmtiles"), run_id="same")
        with pytest.raises(PublishError, match="refusing to overwrite"):
            pub(r2, make_archive(tmp_path, "b.pmtiles"), run_id="same")

    def test_a_failing_gate_blocks_the_publish_entirely(self, tmp_path: Path) -> None:
        r2, fake = make_client()
        with pytest.raises(PublishError, match="failed the gate"):
            pub(r2, make_archive(tmp_path, body=b""), run_id="bad")  # empty: fails the gate
        assert fake.store == {}  # nothing uploaded, no pointer written

    def test_two_scopes_of_one_provider_coexist_in_latest(self, tmp_path: Path) -> None:
        r2, fake = make_client()
        for scope in ("lu", "fr"):
            pub(
                r2,
                make_archive(tmp_path, f"{scope}.pmtiles"),
                provider="google",
                scope=scope,
                run_id="r1",
            )
        latest = json.loads(fake.store[LATEST_KEY]["data"])
        assert set(latest["providers"]["google"]) == {"lu", "fr"}

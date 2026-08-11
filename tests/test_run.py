"""The run loop, from a driven collector to a published archive.

The collector is faked — a real `PartWriter[LineBatch]` writing real parquet into
the run directory, exactly as Já's pyramid descent does — so the loop is exercised
without touching a provider. Two layers: unit tests that stub tiling and
publishing to pin the *order* (parts durable, then snapshot, then tile, then
publish, then discard), and one integration test that runs the real tippecanoe
and pmtiles binaries end to end into the in-memory bucket.
"""

from __future__ import annotations

import json
import shutil
from collections.abc import AsyncIterator, Iterable
from pathlib import Path

import pytest
from fakes import make_client

from streetradar import run as run_module
from streetradar.lines import LineBatch
from streetradar.publish import PublishResult, archive_key
from streetradar.run import run_job
from streetradar.store import PartWriter
from streetradar.tileset import (
    TilesetSpec,
    TilesetStats,
    ZoomStats,
    line_features,
    read_line_parts,
)

needs_tippecanoe = pytest.mark.skipif(
    shutil.which("tippecanoe") is None, reason="tippecanoe is not installed"
)
needs_pmtiles = pytest.mark.skipif(
    shutil.which("pmtiles") is None, reason="the pmtiles CLI is not installed"
)

SPEC = TilesetSpec(layer="coverage", min_zoom=0, max_zoom=16)
# A real Reykjavík-ish z16 tile, so the lines project to valid Icelandic WGS84.
BASE_X, BASE_Y = 28776, 17432


async def fake_ja_lines(
    parts_dir: Path, *, tiles: int = 6, rows_per_part: int = 1_000_000
) -> AsyncIterator[Path]:
    """A stand-in for Já's collector: real LineBatch parquet, yielded per part."""
    writer = PartWriter(
        parts_dir,
        LineBatch(provider="ja", run_id="run", zoom=16, extent=4096),
        rows_per_part=rows_per_part,
    )
    for i in range(tiles):
        writer.batch.add(
            tile_x=BASE_X + i,
            tile_y=BASE_Y,
            lines=[[(500.0, 500.0), (1500.0, 1500.0), (2500.0, 2500.0)]],
        )
        if writer.full():
            part = writer.flush()
            if part is not None:
                yield part
    part = writer.flush()
    if part is not None:
        yield part


async def no_lines(parts_dir: Path) -> AsyncIterator[Path]:
    """A collector that finds no coverage."""
    return
    yield  # pragma: no cover — makes this an async generator


def ja_features(directory: Path) -> Iterable[bytes]:
    return line_features(read_line_parts(directory))


class TestOrdering:
    def _stub_tiling(self, monkeypatch: pytest.MonkeyPatch, events: list[str]) -> None:
        def fake_build(features: Iterable[bytes], dest: Path, spec: TilesetSpec) -> TilesetStats:
            for _ in features:  # drain, as the real builder does
                pass
            events.append("tile")
            dest.write_bytes(b"PMTiles\x03" + b"\x00" * 50)
            return TilesetStats(tiles=3, bytes=300, by_zoom=(ZoomStats(16, 3, 300, 100),))

        def fake_publish(r2: object, **kw: object) -> PublishResult:
            events.append("publish")
            return PublishResult(key="k", url="u", previous_key=None, pruned=())

        monkeypatch.setattr(run_module, "build_tileset", fake_build)
        monkeypatch.setattr(run_module, "publish", fake_publish)

    async def test_every_part_is_snapshotted_before_tiling(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        events: list[str] = []
        self._stub_tiling(monkeypatch, events)
        parts_dir = tmp_path / "parts"
        parts_dir.mkdir()

        result = await run_job(
            make_client()[0],
            provider="ja",
            scope="is",
            run_id="r",
            parts=fake_ja_lines(parts_dir, tiles=6, rows_per_part=2),
            parts_dir=parts_dir,
            features=lambda _d: iter([b""]),
            spec=SPEC,
            archive_dir=tmp_path / "archive",
            published_at="2026-08-11T09:00:00+00:00",
            snapshot=lambda: events.append("snap"),
        )
        # Three parts (six tiles, two per part), each snapshotted, then one tile
        # and one publish — and every snapshot lands before tiling begins.
        assert result.parts == 3
        assert events == ["snap", "snap", "snap", "tile", "publish"]

    async def test_a_published_run_discards_its_local_files(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        self._stub_tiling(monkeypatch, [])
        parts_dir = tmp_path / "parts"
        parts_dir.mkdir()
        archive_dir = tmp_path / "archive"

        await run_job(
            make_client()[0],
            provider="ja",
            scope="is",
            run_id="r",
            parts=fake_ja_lines(parts_dir, tiles=4, rows_per_part=1_000_000),
            parts_dir=parts_dir,
            features=lambda _d: iter([b""]),
            spec=SPEC,
            archive_dir=archive_dir,
            published_at="2026-08-11T09:00:00+00:00",
        )
        assert not list(parts_dir.glob("*.parquet"))
        assert not list(archive_dir.glob("*.pmtiles"))

    async def test_an_empty_country_publishes_nothing(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        called = []
        monkeypatch.setattr(run_module, "build_tileset", lambda *a, **k: called.append("built"))
        parts_dir = tmp_path / "parts"
        parts_dir.mkdir()
        r2, fake = make_client()

        result = await run_job(
            r2,
            provider="ja",
            scope="is",
            run_id="r",
            parts=no_lines(parts_dir),
            parts_dir=parts_dir,
            features=ja_features,
            spec=SPEC,
            archive_dir=tmp_path / "archive",
            published_at="2026-08-11T09:00:00+00:00",
        )
        assert result.published is None
        assert result.note == "no coverage collected"
        assert called == [] and fake.store == {}


@needs_tippecanoe
@needs_pmtiles
class TestFullChain:
    async def test_it_tiles_and_publishes_a_real_archive(self, tmp_path: Path) -> None:
        r2, fake = make_client()
        parts_dir = tmp_path / "parts"
        parts_dir.mkdir()
        archive_dir = tmp_path / "archive"

        result = await run_job(
            r2,
            provider="ja",
            scope="is",
            run_id="20260811",
            parts=fake_ja_lines(parts_dir, tiles=6),
            parts_dir=parts_dir,
            features=ja_features,
            spec=SPEC,
            archive_dir=archive_dir,
            published_at="2026-08-11T09:00:00+00:00",
        )

        assert result.published is not None
        assert result.stats is not None and result.stats.tiles > 0
        key = archive_key("ja", "is", "20260811")
        assert key in fake.store
        assert fake.store[key]["data"][:7] == b"PMTiles"

        latest = json.loads(fake.store["latest.json"]["data"])
        assert latest["providers"]["ja"]["is"]["current"]["run_id"] == "20260811"

        # The country's working set is gone once the archive is on R2.
        assert not list(parts_dir.glob("*.parquet"))
        assert not list(archive_dir.glob("*.pmtiles"))

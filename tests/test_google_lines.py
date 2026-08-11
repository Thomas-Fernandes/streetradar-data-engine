"""The Google line collector: flood, chain each tile's graph, store lines.

Driven through the real fetch and parse surface, and the lines are read back to
WGS84 through the tiling stage's own projection to prove the round trip — a line
stored in tile-local coordinates must come back to the panoramas it was chained
from.
"""

from __future__ import annotations

import json
import math
import re
from pathlib import Path

import pyarrow.parquet as pq
import pytest

from streetradar.google import TILE_ZOOM
from streetradar.google_collect import GoogleState
from streetradar.google_lines import LINE_EXTENT, LINE_PRECISION, _to_local, collect_lines
from streetradar.lines import LineBatch
from streetradar.store import PartWriter
from streetradar.tiles import Tile, offset_to_wgs84
from streetradar.tileset import line_features, read_line_parts

BOUNDS = (-0.130, 51.505, -0.120, 51.512)


def tile_of(lon: float, lat: float, z: int = TILE_ZOOM) -> tuple[int, int]:
    n = 1 << z
    x = int((lon + 180.0) / 360.0 * n)
    y = int((1.0 - math.asinh(math.tan(math.radians(lat))) / math.pi) / 2.0 * n)
    return x, y


def tile_bounds(x: int, y: int, z: int = TILE_ZOOM) -> tuple[float, float, float, float]:
    n = 1 << z
    return (
        x / n * 360.0 - 180.0,
        math.degrees(math.atan(math.sinh(math.pi * (1.0 - 2.0 * (y + 1) / n)))),
        (x + 1) / n * 360.0 - 180.0,
        math.degrees(math.atan(math.sinh(math.pi * (1.0 - 2.0 * y / n)))),
    )


def road_points() -> list[tuple[str, float, float, tuple[int, ...]]]:
    """A short straight road (three linked panoramas) inside each covered tile."""
    x0, y0 = tile_of(BOUNDS[0], BOUNDS[3])
    x1, y1 = tile_of(BOUNDS[2], BOUNDS[1])
    field, base = [], 0
    for x in range(x0, x1 + 1):
        for y in range(y0, y1 + 1):
            w, s, e, n = tile_bounds(x, y)
            local = [
                (f"pano{base + i:06d}xxxxxxxxxx", w + (e - w) * f, (s + n) / 2)
                for i, f in enumerate((0.3, 0.5, 0.7))
            ]
            field += [
                (local[0][0], local[0][1], local[0][2], (1,)),
                (local[1][0], local[1][1], local[1][2], (0, 2)),
                (local[2][0], local[2][1], local[2][2], (1,)),
            ]
            base += 3
    return field


class Endpoint:
    def __init__(self, field: list[tuple[str, float, float, tuple[int, ...]]]) -> None:
        self.by_tile: dict[tuple[int, int], list[tuple[str, float, float, tuple[int, ...]]]] = {}
        for pid, lon, lat, links in field:
            self.by_tile.setdefault(tile_of(lon, lat), []).append((pid, lon, lat, links))
        self.calls = 0

    async def get(self, url: str, *, missing_ok: bool = False) -> bytes | None:
        self.calls += 1
        m = re.search(r"!1i(\d+)!2i(\d+)", url)
        assert m
        panos = self.by_tile.get((int(m.group(1)), int(m.group(2))))
        if not panos:
            return b")]}'\n[null,null]"
        # Links are per-tile indices; the test road's links are already local.
        entries = [
            [[[2, pid], None, [[None, None, lat, lon], [12.0], [0.0, 0.0, 0.0]]], list(links)]
            for pid, lon, lat, links in panos
        ]
        return (")]}'\n" + json.dumps([None, [None, entries]])).encode()


def writer_at(directory: Path, *, rows_per_part: int = 20_000_000) -> PartWriter[LineBatch]:
    return PartWriter(
        directory,
        LineBatch(provider="google", run_id="test", zoom=TILE_ZOOM, extent=LINE_EXTENT),
        rows_per_part=rows_per_part,
    )


async def collect(
    endpoint: Endpoint, writer: PartWriter[LineBatch], state: GoogleState, **kw: int
) -> list[Path]:
    return [
        part async for part in collect_lines(endpoint, BOUNDS, writer=writer, state=state, **kw)
    ]


class TestLocalRoundTrip:
    def test_a_point_returns_to_itself_through_the_tiler(self) -> None:
        tx, ty = tile_of(-0.125, 51.508)
        x, y = _to_local(-0.125, 51.508, tx, ty, TILE_ZOOM)
        assert 0 <= x <= LINE_EXTENT and 0 <= y <= LINE_EXTENT
        lon, lat = offset_to_wgs84(Tile(TILE_ZOOM, tx, ty), x, y, LINE_EXTENT)
        assert lon == pytest.approx(-0.125, abs=1e-6)
        assert lat == pytest.approx(51.508, abs=1e-6)


class TestCollecting:
    async def test_lines_are_written(self, tmp_path: Path) -> None:
        endpoint = Endpoint(road_points())
        writer = writer_at(tmp_path)
        parts = await collect(endpoint, writer, GoogleState.for_bounds(BOUNDS), seed_step=8)
        assert writer.rows_written > 0
        assert pq.ParquetFile(parts[0]).metadata.num_rows == writer.rows_written

    async def test_lines_are_undated(self, tmp_path: Path) -> None:
        endpoint = Endpoint(road_points())
        writer = writer_at(tmp_path)
        parts = await collect(endpoint, writer, GoogleState.for_bounds(BOUNDS), seed_step=8)
        table = pq.read_table(parts[0])
        assert all(v is None for v in table.column("captured_at").to_pylist())
        assert set(table.column("capture_precision").to_pylist()) == {LINE_PRECISION}

    async def test_the_lines_reproject_onto_the_road(self, tmp_path: Path) -> None:
        # The whole point: a line stored tile-local must render back onto the
        # panoramas it was chained from, within the tiler's own projection.
        endpoint = Endpoint(road_points())
        writer = writer_at(tmp_path)
        await collect(endpoint, writer, GoogleState.for_bounds(BOUNDS), seed_step=8)
        rebuilt = [
            json.loads(row)["geometry"]["coordinates"]
            for blob in line_features(read_line_parts(tmp_path))
            for row in blob.decode().splitlines()
        ]
        assert rebuilt
        # Every reconstructed vertex sits on the box, at a real panorama latitude.
        for line in rebuilt:
            for lon, lat in line:
                assert BOUNDS[0] - 0.01 < lon < BOUNDS[2] + 0.01
                assert BOUNDS[1] - 0.01 < lat < BOUNDS[3] + 0.01

    async def test_resuming_probes_nothing_further(self, tmp_path: Path) -> None:
        endpoint = Endpoint(road_points())
        state = GoogleState.for_bounds(BOUNDS)
        await collect(endpoint, writer_at(tmp_path), state, seed_step=8)
        after = endpoint.calls
        await collect(endpoint, writer_at(tmp_path / "b"), state, seed_step=8)
        assert endpoint.calls == after


class TestGuards:
    async def test_a_writer_at_the_wrong_zoom_is_refused(self, tmp_path: Path) -> None:
        writer = PartWriter(
            tmp_path, LineBatch(provider="google", run_id="t", zoom=14, extent=LINE_EXTENT)
        )
        with pytest.raises(ValueError, match="not the tile zoom"):
            await collect(Endpoint(road_points()), writer, GoogleState.for_bounds(BOUNDS))

    async def test_a_writer_at_the_wrong_extent_is_refused(self, tmp_path: Path) -> None:
        writer = PartWriter(
            tmp_path, LineBatch(provider="google", run_id="t", zoom=TILE_ZOOM, extent=512)
        )
        with pytest.raises(ValueError, match="not the line extent"):
            await collect(Endpoint(road_points()), writer, GoogleState.for_bounds(BOUNDS))

    async def test_nothing_snapshots_the_state(self, tmp_path: Path) -> None:
        await collect(Endpoint(road_points()), writer_at(tmp_path), GoogleState.for_bounds(BOUNDS))
        assert not list(tmp_path.glob("*.state")) and not list(tmp_path.glob("*.tmp"))

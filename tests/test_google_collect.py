"""The Google geometry collector: flood z17 tiles, clip, dedup, resume.

Driven through the real fetch and parse surface: the synthetic endpoint answers a
tile URL with a response in Google's exact shape, so tile_url and parse_tile run
for real and only the point field is invented.
"""

from __future__ import annotations

import json
import math
import re
from collections import defaultdict
from pathlib import Path

import pyarrow.parquet as pq
import pytest

from streetradar.google import TILE_ZOOM
from streetradar.google_collect import GEOMETRY_PRECISION, GoogleState, collect_geometry
from streetradar.model import ObservationBatch
from streetradar.store import PartWriter
from streetradar.tiles import Tile

# A small box near London spanning a handful of z17 tiles.
BOUNDS = (-0.130, 51.505, -0.120, 51.512)  # west, south, east, north


def tile_of(lon: float, lat: float, z: int = TILE_ZOOM) -> tuple[int, int]:
    n = 1 << z
    x = int((lon + 180.0) / 360.0 * n)
    y = int((1.0 - math.asinh(math.tan(math.radians(lat))) / math.pi) / 2.0 * n)
    return x, y


def tile_bounds(x: int, y: int, z: int = TILE_ZOOM) -> tuple[float, float, float, float]:
    n = 1 << z
    west = x / n * 360.0 - 180.0
    east = (x + 1) / n * 360.0 - 180.0
    north = math.degrees(math.atan(math.sinh(math.pi * (1.0 - 2.0 * y / n))))
    south = math.degrees(math.atan(math.sinh(math.pi * (1.0 - 2.0 * (y + 1) / n))))
    return west, south, east, north


def dense_points() -> list[tuple[str, float, float]]:
    """Two panoramas in every z17 tile of the box, so the flood must expand."""
    x0, y0 = tile_of(BOUNDS[0], BOUNDS[3])
    x1, y1 = tile_of(BOUNDS[2], BOUNDS[1])
    points = []
    k = 0
    for x in range(x0, x1 + 1):
        for y in range(y0, y1 + 1):
            w, s, e, n = tile_bounds(x, y)
            for f in (0.35, 0.65):
                k += 1
                points.append((f"pano{k:06d}xxxxxxxxxxxx", w + (e - w) * f, s + (n - s) * f))
    return points


class Endpoint:
    """A synthetic photometa tactile endpoint over a fixed point field."""

    def __init__(self, points: list[tuple[str, float, float]]) -> None:
        self.by_tile: dict[tuple[int, int], list[tuple[str, float, float]]] = defaultdict(list)
        for pid, lon, lat in points:
            self.by_tile[tile_of(lon, lat)].append((pid, lon, lat))
        self.calls = 0

    async def get(self, url: str, *, missing_ok: bool = False) -> bytes | None:
        self.calls += 1
        m = re.search(r"!1i(\d+)!2i(\d+)", url)
        assert m
        x, y = int(m.group(1)), int(m.group(2))
        panos = self.by_tile.get((x, y))
        if not panos:
            return b")]}'\n[null,null]"  # a no-coverage tile
        entries = [
            [[[2, pid], None, [[None, None, lat, lon], [12.0], [0.0, 0.0, 0.0]]], [0]]
            for pid, lon, lat in panos
        ]
        return (")]}'\n" + json.dumps([None, [None, entries]])).encode()


def writer_at(directory: Path, *, rows_per_part: int = 20_000_000) -> PartWriter[ObservationBatch]:
    return PartWriter(
        directory,
        ObservationBatch(provider="google", run_id="test", capture_precision=GEOMETRY_PRECISION),
        rows_per_part=rows_per_part,
    )


async def collect(
    endpoint: Endpoint, writer: PartWriter[ObservationBatch], state: GoogleState, **kw: int
) -> list[Path]:
    return [
        part async for part in collect_geometry(endpoint, BOUNDS, writer=writer, state=state, **kw)
    ]


class TestGoogleState:
    def test_a_fresh_state_is_empty_at_zoom_17(self) -> None:
        state = GoogleState.for_bounds(BOUNDS)
        assert state.zoom == TILE_ZOOM
        assert state.probed == 0 and state.panoramas == 0

    def test_it_survives_a_round_trip(self, tmp_path: Path) -> None:
        state = GoogleState.for_bounds(BOUNDS)
        x, y = tile_of(-0.125, 51.508)
        state.grid.add(Tile(TILE_ZOOM, x, y))
        state.panoramas, state.covered = 123, 45
        path = tmp_path / "g.state"
        state.save(path)
        back = GoogleState.load(path)
        assert back.probed == 1 and Tile(TILE_ZOOM, x, y) in back.grid
        assert (back.panoramas, back.covered) == (123, 45)

    def test_a_foreign_file_is_refused(self, tmp_path: Path) -> None:
        (tmp_path / "x").write_bytes(b"nope")
        with pytest.raises(ValueError, match="not a Google sweep snapshot"):
            GoogleState.load(tmp_path / "x")


class TestCollecting:
    async def test_the_flood_reaches_every_panorama(self, tmp_path: Path) -> None:
        # One seed, contiguous coverage: the flood must expand to the whole box.
        endpoint = Endpoint(dense_points())
        writer = writer_at(tmp_path)
        parts = await collect(endpoint, writer, GoogleState.for_bounds(BOUNDS), seed_step=8)
        assert writer.rows_written == len(dense_points())
        table = pq.read_table(parts[0])
        assert len(table.column("pano_id").to_pylist()) == len(dense_points())

    async def test_panoramas_are_written_undated(self, tmp_path: Path) -> None:
        # A tile carries no date; the geometry pass must not invent one.
        endpoint = Endpoint(dense_points())
        writer = writer_at(tmp_path)
        parts = await collect(endpoint, writer, GoogleState.for_bounds(BOUNDS), seed_step=8)
        table = pq.read_table(parts[0])
        assert all(v is None for v in table.column("captured_at").to_pylist())
        assert set(table.column("capture_precision").to_pylist()) == {GEOMETRY_PRECISION}

    async def test_rows_carry_a_zoom_17_tile(self, tmp_path: Path) -> None:
        endpoint = Endpoint(dense_points())
        writer = writer_at(tmp_path)
        parts = await collect(endpoint, writer, GoogleState.for_bounds(BOUNDS), seed_step=8)
        table = pq.read_table(parts[0])
        span = 1 << TILE_ZOOM
        assert all(0 < x < span for x in table.column("tile_x").to_pylist())

    async def test_coverage_outside_the_box_is_not_collected(self, tmp_path: Path) -> None:
        # A panorama one tile east of the box must not be reached: the flood is
        # clipped, which is what stops a country run spilling into its neighbours.
        x1, _ = tile_of(BOUNDS[2], BOUNDS[1])
        w, s, e, n = tile_bounds(x1 + 3, tile_of(BOUNDS[0], BOUNDS[3])[1])
        outside = dense_points() + [("neighbourxxxxxxxxxxxxx", (w + e) / 2, (s + n) / 2)]
        endpoint = Endpoint(outside)
        writer = writer_at(tmp_path)
        parts = await collect(endpoint, writer, GoogleState.for_bounds(BOUNDS), seed_step=8)
        ids = {i for p in parts for i in pq.read_table(p).column("pano_id").to_pylist()}
        from streetradar.google_collect import _pano_key

        assert _pano_key("neighbourxxxxxxxxxxxxx") not in ids

    async def test_an_empty_country_writes_nothing(self, tmp_path: Path) -> None:
        endpoint = Endpoint([])
        writer = writer_at(tmp_path)
        parts = await collect(endpoint, writer, GoogleState.for_bounds(BOUNDS), seed_step=8)
        assert parts == []
        assert writer.rows_written == 0
        assert endpoint.calls > 0  # the seed grid was still probed

    async def test_resuming_probes_no_finished_tile(self, tmp_path: Path) -> None:
        endpoint = Endpoint(dense_points())
        state = GoogleState.for_bounds(BOUNDS)
        await collect(endpoint, writer_at(tmp_path), state, seed_step=8)
        after = endpoint.calls
        await collect(endpoint, writer_at(tmp_path / "b"), state, seed_step=8)
        assert endpoint.calls == after

    async def test_a_limit_bounds_the_probing(self, tmp_path: Path) -> None:
        endpoint = Endpoint(dense_points())
        state = GoogleState.for_bounds(BOUNDS)
        await collect(endpoint, writer_at(tmp_path), state, seed_step=8, limit=5)
        assert state.probed <= 5 + 64  # a batch may overshoot the limit

    async def test_a_part_is_written_once_it_fills(self, tmp_path: Path) -> None:
        endpoint = Endpoint(dense_points())
        writer = writer_at(tmp_path, rows_per_part=20)
        parts = await collect(endpoint, writer, GoogleState.for_bounds(BOUNDS), seed_step=8)
        assert len(parts) > 1
        assert writer.pending == 0


class TestOrderingRule:
    async def test_nothing_snapshots_the_state(self, tmp_path: Path) -> None:
        await collect(Endpoint(dense_points()), writer_at(tmp_path), GoogleState.for_bounds(BOUNDS))
        assert not list(tmp_path.glob("*.state"))
        assert not list(tmp_path.glob("*.tmp"))

    async def test_a_dated_writer_is_refused(self, tmp_path: Path) -> None:
        # The geometry pass has no dates; a writer promising month precision would
        # mislabel every panorama as dated.
        writer = PartWriter(
            tmp_path, ObservationBatch(provider="google", run_id="t", capture_precision="month")
        )
        with pytest.raises(ValueError, match="must be 'unknown'"):
            await collect(Endpoint(dense_points()), writer, GoogleState.for_bounds(BOUNDS))

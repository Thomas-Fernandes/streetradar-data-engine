"""The Google dates pass: flood, sample one panorama per cell, date it.

The synthetic endpoint serves both real shapes — the tactile tile and the by-id
metadata — so tile_url, parse_tile, metadata_url and parse_metadata all run for
real, and only the point field with its dates and history is invented.
"""

from __future__ import annotations

import datetime
import json
import math
import re
from collections import defaultdict
from pathlib import Path

import pyarrow.parquet as pq
import pytest

from streetradar.google import TILE_ZOOM
from streetradar.google_dates import SAMPLE_ZOOM, DatesState, collect_dates
from streetradar.google_meta import METADATA_PRECISION
from streetradar.model import ObservationBatch
from streetradar.store import PartWriter
from streetradar.tiles import Tile

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


HISTORY = [([2019, 9]), ([2016, 6])]  # two past layers on every point


def dense_points() -> list[tuple[str, float, float]]:
    x0, y0 = tile_of(BOUNDS[0], BOUNDS[3])
    x1, y1 = tile_of(BOUNDS[2], BOUNDS[1])
    points, k = [], 0
    for x in range(x0, x1 + 1):
        for y in range(y0, y1 + 1):
            w, s, e, n = tile_bounds(x, y)
            k += 1
            points.append((f"pano{k:06d}xxxxxxxxxxxx", w + (e - w) * 0.5, s + (n - s) * 0.5))
    return points


def encode_tile(panos: list[tuple[str, float, float]]) -> bytes:
    entries = [
        [[[2, pid], None, [[None, None, lat, lon], [12.0], [0.0, 0.0, 0.0]]], [0]]
        for pid, lon, lat in panos
    ]
    return (")]}'\n" + json.dumps([None, [None, entries]])).encode()


def encode_metadata(pid: str, lon: float, lat: float, date: list[int]) -> bytes:
    # Two historical layers, shifted slightly, each dated from HISTORY.
    connected, dated = [], []
    for i, hdate in enumerate(HISTORY):
        connected.append([[None, f"{pid[:8]}_h{i}"], None, [[None, None, lat + 1e-5, lon + 1e-5]]])
        dated.append([i, hdate])
    node = [None, [[None, None, lat, lon]], None, [connected], None, None, None, None, dated]
    msg = [None, [None, pid], None, None, None, [node], [None] * 7 + [date]]
    return (")]}'\n" + json.dumps([[], [msg]])).encode()


class Endpoint:
    """Serves the tactile tile and the by-id metadata for a fixed point field."""

    def __init__(self, points: list[tuple[str, float, float]]) -> None:
        self.by_tile: dict[tuple[int, int], list[tuple[str, float, float]]] = defaultdict(list)
        self.pos: dict[str, tuple[float, float]] = {}
        for pid, lon, lat in points:
            self.by_tile[tile_of(lon, lat)].append((pid, lon, lat))
            self.pos[pid] = (lon, lat)
        self.tile_calls = 0
        self.meta_calls = 0

    async def get(self, url: str, *, missing_ok: bool = False) -> bytes | None:
        if "photometa/v1?" in url:  # metadata by id
            self.meta_calls += 1
            pid = re.search(r"!1e2!2s([^!]+)!", url).group(1)  # type: ignore[union-attr]
            lon, lat = self.pos[pid]
            return encode_metadata(pid, lon, lat, [2022, 3])
        # tile
        self.tile_calls += 1
        m = re.search(r"!1i(\d+)!2i(\d+)", url)
        assert m
        panos = self.by_tile.get((int(m.group(1)), int(m.group(2))))
        return encode_tile(panos) if panos else b")]}'\n[null,null]"


def writer_at(directory: Path, *, rows_per_part: int = 20_000_000) -> PartWriter[ObservationBatch]:
    return PartWriter(
        directory,
        ObservationBatch(provider="google", run_id="test", capture_precision=METADATA_PRECISION),
        rows_per_part=rows_per_part,
    )


async def collect(
    endpoint: Endpoint, writer: PartWriter[ObservationBatch], state: DatesState, **kw: int
) -> list[Path]:
    return [
        part async for part in collect_dates(endpoint, BOUNDS, writer=writer, state=state, **kw)
    ]


class TestDatesState:
    def test_a_fresh_state_holds_two_empty_grids(self) -> None:
        state = DatesState.for_bounds(BOUNDS)
        assert state.tile_zoom == TILE_ZOOM and state.sample_zoom == SAMPLE_ZOOM
        assert len(state.tiles) == 0 and len(state.cells) == 0

    def test_it_survives_a_round_trip(self, tmp_path: Path) -> None:
        state = DatesState.for_bounds(BOUNDS)
        state.tiles.add(Tile(TILE_ZOOM, *tile_of(-0.125, 51.508)))
        state.cells.add(Tile(SAMPLE_ZOOM, *tile_of(-0.125, 51.508, SAMPLE_ZOOM)))
        state.dated, state.metadata_calls = 55, 12
        path = tmp_path / "d.state"
        state.save(path)
        back = DatesState.load(path)
        assert len(back.tiles) == 1 and len(back.cells) == 1
        assert (back.dated, back.metadata_calls) == (55, 12)

    def test_a_foreign_file_is_refused(self, tmp_path: Path) -> None:
        (tmp_path / "x").write_bytes(b"no")
        with pytest.raises(ValueError, match="not a Google dates snapshot"):
            DatesState.load(tmp_path / "x")


class TestCollecting:
    async def test_it_writes_dated_observations(self, tmp_path: Path) -> None:
        endpoint = Endpoint(dense_points())
        writer = writer_at(tmp_path)
        parts = await collect(endpoint, writer, DatesState.for_bounds(BOUNDS), seed_step=8)
        table = pq.read_table(parts[0])
        assert all(v is not None for v in table.column("captured_at").to_pylist())
        assert set(table.column("capture_precision").to_pylist()) == {METADATA_PRECISION}

    async def test_the_current_date_and_the_history_are_written(self, tmp_path: Path) -> None:
        endpoint = Endpoint(dense_points())
        writer = writer_at(tmp_path)
        parts = await collect(endpoint, writer, DatesState.for_bounds(BOUNDS), seed_step=8)
        years = {
            datetime.datetime.fromtimestamp(v.timestamp(), datetime.UTC).year
            if hasattr(v, "timestamp")
            else v.year
            for v in pq.read_table(parts[0]).column("captured_at").to_pylist()
        }
        # 2022 current, 2019 and 2016 from the historical stack.
        assert {2022, 2019, 2016} <= years

    async def test_only_one_metadata_call_per_sample_cell(self, tmp_path: Path) -> None:
        # Many tiles share a z15 cell, so the metadata calls must be far fewer
        # than the tiles probed — that is the sampling.
        endpoint = Endpoint(dense_points())
        state = DatesState.for_bounds(BOUNDS)
        await collect(endpoint, writer_at(tmp_path), state, seed_step=8)
        assert 0 < state.metadata_calls < endpoint.tile_calls
        assert state.metadata_calls == len(state.cells)

    async def test_an_empty_country_dates_nothing(self, tmp_path: Path) -> None:
        endpoint = Endpoint([])
        writer = writer_at(tmp_path)
        assert await collect(endpoint, writer, DatesState.for_bounds(BOUNDS), seed_step=8) == []
        assert endpoint.meta_calls == 0

    async def test_resuming_makes_no_further_calls(self, tmp_path: Path) -> None:
        endpoint = Endpoint(dense_points())
        state = DatesState.for_bounds(BOUNDS)
        await collect(endpoint, writer_at(tmp_path), state, seed_step=8)
        after_tiles, after_meta = endpoint.tile_calls, endpoint.meta_calls
        await collect(endpoint, writer_at(tmp_path / "b"), state, seed_step=8)
        assert (endpoint.tile_calls, endpoint.meta_calls) == (after_tiles, after_meta)


class TestOrderingRule:
    async def test_nothing_snapshots_the_state(self, tmp_path: Path) -> None:
        await collect(Endpoint(dense_points()), writer_at(tmp_path), DatesState.for_bounds(BOUNDS))
        assert not list(tmp_path.glob("*.state")) and not list(tmp_path.glob("*.tmp"))

    async def test_an_undated_writer_is_refused(self, tmp_path: Path) -> None:
        writer = PartWriter(
            tmp_path, ObservationBatch(provider="google", run_id="t", capture_precision="unknown")
        )
        with pytest.raises(ValueError, match="must be 'month'"):
            await collect(Endpoint(dense_points()), writer, DatesState.for_bounds(BOUNDS))

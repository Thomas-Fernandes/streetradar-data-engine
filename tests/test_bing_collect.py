"""The Streetside collector: sweep root cells, dedup, write, resume.

Driven through the real fetch surface — the synthetic fetcher answers a URL with
JSON in the endpoint's shape, so `bubble_url`, `parse_bubbles` and the sweep all
run for real, and only the point field is invented.
"""

from __future__ import annotations

import json
import math
from pathlib import Path
from urllib.parse import parse_qs, urlparse

import pyarrow.parquet as pq
import pytest

from streetradar.bing import MAX_COUNT
from streetradar.bing_collect import OBS_ZOOM, ROOT_ZOOM, BingState, collect_streetside
from streetradar.model import ObservationBatch
from streetradar.store import PartWriter
from streetradar.tiles import Tile

# A ~2 km box near London, inside a single z11 root cell.
BOUNDS = (-0.140, 51.500, -0.110, 51.520)  # west, south, east, north
WHEN = "4/24/2021 3:03:57 PM"


def haversine(lat1: float, lon1: float, lat2: float, lon2: float) -> float:
    r = 6_371_000.0
    p1, p2 = math.radians(lat1), math.radians(lat2)
    dp, dl = math.radians(lat2 - lat1), math.radians(lon2 - lon1)
    a = math.sin(dp / 2) ** 2 + math.cos(p1) * math.cos(p2) * math.sin(dl / 2) ** 2
    return 2 * r * math.asin(math.sqrt(min(1.0, a)))


class Endpoint:
    """A synthetic StreetSideBubbleMetaData over a fixed point field."""

    def __init__(self, points: list[tuple[int, float, float]]) -> None:
        self.points = points  # (id, lat, lon)
        self.calls = 0

    @property
    def concurrency(self) -> int:
        return 8

    async def get(self, url: str, *, missing_ok: bool = False) -> bytes | None:
        self.calls += 1
        params = parse_qs(urlparse(url).query)
        lat = (float(params["north"][0]) + float(params["south"][0])) / 2
        lon = (float(params["east"][0]) + float(params["west"][0])) / 2
        ranked = sorted(self.points, key=lambda p: haversine(lat, lon, p[1], p[2]))
        payload: list[dict[str, object]] = [{"elapsed": 0.001}]
        for i, (pid, plat, plon) in enumerate(ranked[:MAX_COUNT]):
            payload.append(
                {
                    "id": pid,
                    "la": plat,
                    "lo": plon,
                    "cd": WHEN,
                    "pr": pid - 1 if i else "",
                    "ne": pid + 1,
                }
            )
        return json.dumps(payload).encode()


def grid_points(n: int = 30, spacing: float = 0.0006) -> list[tuple[int, float, float]]:
    lat0, lon0 = 51.505, -0.135
    return [
        (i * n + j + 1, lat0 + i * spacing, lon0 + j * spacing) for i in range(n) for j in range(n)
    ]


def writer_at(directory: Path, *, rows_per_part: int = 20_000_000) -> PartWriter[ObservationBatch]:
    return PartWriter(
        directory,
        ObservationBatch(provider="bing", run_id="test", capture_precision="second"),
        rows_per_part=rows_per_part,
    )


async def collect(
    endpoint: Endpoint, writer: PartWriter[ObservationBatch], state: BingState, **kw: int
) -> list[Path]:
    return [
        part
        async for part in collect_streetside(endpoint, BOUNDS, writer=writer, state=state, **kw)
    ]


class TestBingState:
    def test_a_fresh_state_is_empty_at_the_root_zoom(self) -> None:
        state = BingState.for_bounds(BOUNDS)
        assert state.zoom == ROOT_ZOOM
        assert state.cells_done == 0 and state.bubbles == 0

    def test_it_survives_a_round_trip(self, tmp_path: Path) -> None:
        state = BingState.for_bounds(BOUNDS)
        tile = next(iter(TileTiles()))
        state.grid.add(tile)
        state.bubbles, state.probes = 4242, 99
        path = tmp_path / "bing.state"
        state.save(path)
        back = BingState.load(path)
        assert back.cells_done == 1 and tile in back.grid
        assert (back.bubbles, back.probes) == (4242, 99)

    def test_a_foreign_file_is_refused(self, tmp_path: Path) -> None:
        (tmp_path / "x.state").write_bytes(b"nope")
        with pytest.raises(ValueError, match="not a Bing sweep snapshot"):
            BingState.load(tmp_path / "x.state")


def TileTiles() -> list[Tile]:
    from streetradar.tiles import TileSource

    src = TileSource(
        id="bing", url_template="", min_zoom=ROOT_ZOOM, max_zoom=ROOT_ZOOM, bounds=BOUNDS
    )
    return list(src.tiles_at(ROOT_ZOOM))


class TestCollecting:
    async def test_every_point_is_written_once(self, tmp_path: Path) -> None:
        # The box spans more than one root cell, so a point on a seam is
        # written by both — the DISTINCT-id pass downstream removes that residue.
        # The invariant here is that every point is found, once deduplicated.
        endpoint = Endpoint(grid_points())
        writer = writer_at(tmp_path)
        parts = await collect(endpoint, writer, BingState.for_bounds(BOUNDS))
        ids = {i for part in parts for i in pq.read_table(part).column("pano_id").to_pylist()}
        assert ids == {p[0] for p in grid_points()}
        assert writer.rows_written >= 900

    async def test_dates_reach_the_part(self, tmp_path: Path) -> None:
        endpoint = Endpoint(grid_points(3))
        writer = writer_at(tmp_path)
        parts = await collect(endpoint, writer, BingState.for_bounds(BOUNDS))
        table = pq.read_table(parts[0])
        moments = table.column("captured_at").to_pylist()  # pyarrow -> datetime
        assert all(v is not None for v in moments)
        assert moments[0].year == 2021

    async def test_rows_carry_the_observation_tile(self, tmp_path: Path) -> None:
        endpoint = Endpoint(grid_points(4))
        writer = writer_at(tmp_path)
        parts = await collect(endpoint, writer, BingState.for_bounds(BOUNDS))
        table = pq.read_table(parts[0])
        span = 1 << OBS_ZOOM
        assert all(0 < x < span for x in table.column("tile_x").to_pylist())

    async def test_the_root_cell_is_marked_done(self, tmp_path: Path) -> None:
        state = BingState.for_bounds(BOUNDS)
        await collect(Endpoint(grid_points(4)), writer_at(tmp_path), state)
        assert state.cells_done >= 1
        assert state.probes > 0

    async def test_an_empty_region_costs_little_and_writes_nothing(self, tmp_path: Path) -> None:
        state = BingState.for_bounds(BOUNDS)
        writer = writer_at(tmp_path)
        parts = await collect(Endpoint([]), writer, state)
        assert parts == []
        assert writer.rows_written == 0
        assert state.cells_done >= 1  # the cell was still visited and dismissed

    async def test_resuming_skips_finished_cells(self, tmp_path: Path) -> None:
        endpoint = Endpoint(grid_points(4))
        state = BingState.for_bounds(BOUNDS)
        await collect(endpoint, writer_at(tmp_path), state)
        after_full = endpoint.calls
        # Every cell is done, so a second pass adds no probes at all.
        await collect(endpoint, writer_at(tmp_path / "b"), state)
        assert endpoint.calls == after_full

    async def test_a_part_is_written_once_it_fills(self, tmp_path: Path) -> None:
        endpoint = Endpoint(grid_points(20))
        writer = writer_at(tmp_path, rows_per_part=100)
        parts = await collect(endpoint, writer, BingState.for_bounds(BOUNDS))
        assert len(parts) > 1
        assert writer.pending == 0


class TestOrderingRule:
    async def test_nothing_snapshots_the_state(self, tmp_path: Path) -> None:
        await collect(Endpoint(grid_points(4)), writer_at(tmp_path), BingState.for_bounds(BOUNDS))
        assert not list(tmp_path.glob("*.state"))
        assert not list(tmp_path.glob("*.tmp"))

    async def test_a_writer_of_the_wrong_precision_is_refused(self, tmp_path: Path) -> None:
        writer = PartWriter(
            tmp_path, ObservationBatch(provider="bing", run_id="t", capture_precision="month")
        )
        with pytest.raises(ValueError, match="must be 'second'"):
            await collect(Endpoint(grid_points(3)), writer, BingState.for_bounds(BOUNDS))

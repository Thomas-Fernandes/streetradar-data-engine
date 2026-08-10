"""Flood fill: expansion, exits, bounded probes, and resuming.

The flood is driven against a stub fetcher holding a synthetic "road": a chain of
covered tiles. That is enough to exercise the properties that matter — that it
follows exits rather than all four neighbours, that a limit is honoured, and that
stopping and resuming visits exactly the same tiles as running straight through.
"""

from __future__ import annotations

import math
from collections.abc import Collection, Mapping

import pytest

from streetradar.flood import (
    DIRECTIONS,
    FloodState,
    ProbedGrid,
    exits_from_points,
    harvest_flood,
)
from streetradar.tiles import Tile, TileSource

ZOOM = 17


def source(bounds: tuple[float, float, float, float] = (5.7, 49.4, 6.6, 50.2)) -> TileSource:
    return TileSource(
        id="test",
        url_template="https://example.invalid/{z}/{x}/{y}",
        min_zoom=ZOOM,
        max_zoom=ZOOM,
        bounds=bounds,
    )


class StubFetcher:
    """Serves a body for tiles in `covered`, nothing elsewhere."""

    def __init__(self, covered: Mapping[Tile, bytes], concurrency: int = 8) -> None:
        self._covered = covered
        self._concurrency = concurrency
        self.requested: list[Tile] = []

    @property
    def concurrency(self) -> int:
        return self._concurrency

    async def fetch_tile(self, url: str, headers: Mapping[str, str]) -> bytes | None:
        z, x, y = (int(p) for p in url.rstrip("/").split("/")[-3:])
        tile = Tile(z, x, y)
        self.requested.append(tile)
        return self._covered.get(tile)


def horizontal_road(start: Tile, length: int) -> dict[Tile, bytes]:
    """A chain of covered tiles running east, each exiting west and east."""
    return {Tile(start.z, start.x + i, start.y): b"WE" for i in range(length)}


def exits_from_body(body: bytes, tile: Tile) -> Collection[str]:
    """Test doubles encode their exits directly in the body."""
    return [chr(b) for b in body]


class TestProbedGrid:
    def test_records_and_reports_membership(self) -> None:
        grid = ProbedGrid(x0=10, y0=20, width=5, height=5)
        tile = Tile(ZOOM, 12, 22)
        assert tile not in grid
        assert grid.add(tile) is True
        assert tile in grid
        assert len(grid) == 1

    def test_adding_twice_is_reported_as_already_present(self) -> None:
        grid = ProbedGrid(x0=0, y0=0, width=4, height=4)
        tile = Tile(ZOOM, 1, 1)
        assert grid.add(tile) is True
        assert grid.add(tile) is False
        assert len(grid) == 1

    def test_outside_the_region_counts_as_probed(self) -> None:
        # This is what stops a flood wandering off the grid instead of
        # expanding forever into tiles it can never record.
        grid = ProbedGrid(x0=0, y0=0, width=4, height=4)
        assert Tile(ZOOM, 99, 99) in grid
        assert grid.add(Tile(ZOOM, 99, 99)) is False
        assert len(grid) == 0

    def test_bitmap_is_one_bit_per_tile(self) -> None:
        # The whole point: a set of tuples for Australia is 4 GB, this is 125 MB.
        grid = ProbedGrid(x0=0, y0=0, width=8000, height=8000)
        assert len(grid._bits) == 8000 * 8000 // 8  # noqa: SLF001

    def test_rejects_an_empty_region(self) -> None:
        with pytest.raises(ValueError, match="positive extent"):
            ProbedGrid(x0=0, y0=0, width=0, height=10)


class TestExitsFromPoints:
    def test_a_point_near_the_west_edge_exits_west(self) -> None:
        tile = Tile(14, 7195, 4355)
        west, south, east, north = tile.bounds
        lon = west + (east - west) * 0.01
        lat = (south + north) / 2
        assert exits_from_points([(lon, lat)], tile) == {"W"}

    def test_a_point_in_the_middle_exits_nowhere(self) -> None:
        tile = Tile(14, 7195, 4355)
        west, south, east, north = tile.bounds
        centre = ((west + east) / 2, (south + north) / 2)
        assert exits_from_points([centre], tile) == set()

    def test_north_and_south_are_not_inverted(self) -> None:
        # The y axis is the classic way to get this wrong: a flood that expands
        # north where the road went south finds nothing and stops.
        tile = Tile(14, 7195, 4355)
        west, south, east, north = tile.bounds
        mid_lon = (west + east) / 2
        assert exits_from_points([(mid_lon, north - (north - south) * 0.01)], tile) == {"N"}
        assert exits_from_points([(mid_lon, south + (north - south) * 0.01)], tile) == {"S"}

    def test_a_wider_margin_finds_more_exits(self) -> None:
        tile = Tile(14, 7195, 4355)
        west, south, east, north = tile.bounds
        point = (west + (east - west) * 0.12, (south + north) / 2)
        assert exits_from_points([point], tile, margin=0.05) == set()
        assert exits_from_points([point], tile, margin=0.20) == {"W"}

    def test_a_corner_point_exits_both_ways(self) -> None:
        tile = Tile(14, 7195, 4355)
        west, south, east, north = tile.bounds
        corner = (west + (east - west) * 0.01, north - (north - south) * 0.01)
        assert exits_from_points([corner], tile) == {"W", "N"}


class TestHarvestFlood:
    async def test_follows_a_road_from_one_seed(self) -> None:
        src = source()
        start = next(src.tiles_at(ZOOM))
        covered = horizontal_road(start, 10)
        stub = StubFetcher(covered)

        found = [
            r
            async for r in harvest_flood(stub, src, ZOOM, seeds=[start], exits=exits_from_body)
            if r.body
        ]

        assert {r.tile for r in found} == set(covered)

    async def test_probes_only_the_declared_exits(self) -> None:
        # A road running east-west must not cost probes to the north and south of
        # every tile. That difference is 63,709 requests against 36,610 on the
        # real Luxembourg export.
        src = source()
        start = next(src.tiles_at(ZOOM))
        covered = horizontal_road(start, 10)
        stub = StubFetcher(covered)

        _ = [r async for r in harvest_flood(stub, src, ZOOM, seeds=[start], exits=exits_from_body)]

        assert all(t.y == start.y for t in stub.requested)
        # ten covered tiles plus one dead end at each end
        assert len(stub.requested) == 12

    async def test_a_tile_is_never_probed_twice(self) -> None:
        src = source()
        start = next(src.tiles_at(ZOOM))
        stub = StubFetcher(horizontal_road(start, 20))
        _ = [r async for r in harvest_flood(stub, src, ZOOM, seeds=[start], exits=exits_from_body)]
        assert len(stub.requested) == len(set(stub.requested))

    async def test_disconnected_coverage_needs_its_own_seed(self) -> None:
        # The property that makes seeding the whole question: Greece has 83
        # components and one seed recovers 76% of it.
        src = source()
        first = next(src.tiles_at(ZOOM))
        island = Tile(ZOOM, first.x + 60, first.y + 60)
        covered = {**horizontal_road(first, 5), **horizontal_road(island, 5)}
        stub = StubFetcher(covered)

        one = {
            r.tile
            async for r in harvest_flood(stub, src, ZOOM, seeds=[first], exits=exits_from_body)
            if r.body
        }
        assert len(one) == 5

        stub2 = StubFetcher(covered)
        both = {
            r.tile
            async for r in harvest_flood(
                stub2, src, ZOOM, seeds=[first, island], exits=exits_from_body
            )
            if r.body
        }
        assert both == set(covered)

    async def test_limit_bounds_the_number_of_requests(self) -> None:
        src = source()
        start = next(src.tiles_at(ZOOM))
        stub = StubFetcher(horizontal_road(start, 100))
        _ = [
            r
            async for r in harvest_flood(
                stub, src, ZOOM, seeds=[start], exits=exits_from_body, limit=7
            )
        ]
        assert len(stub.requested) == 7

    async def test_an_unknown_exit_direction_is_refused(self) -> None:
        src = source()
        start = next(src.tiles_at(ZOOM))
        stub = StubFetcher({start: b"X"})
        with pytest.raises(ValueError, match="unknown exit direction"):
            _ = [
                r
                async for r in harvest_flood(stub, src, ZOOM, seeds=[start], exits=exits_from_body)
            ]


class TestResume:
    async def test_stopping_and_resuming_covers_the_same_tiles(self) -> None:
        src = source()
        start = next(src.tiles_at(ZOOM))
        covered = horizontal_road(start, 40)

        straight = StubFetcher(covered)
        whole = {
            r.tile
            async for r in harvest_flood(straight, src, ZOOM, seeds=[start], exits=exits_from_body)
            if r.body
        }

        piecewise = StubFetcher(covered)
        state = FloodState(grid=ProbedGrid.for_bounds(src.bounds, src, ZOOM), zoom=ZOOM)
        seen: set[Tile] = set()
        seeds: list[Tile] = [start]
        for _ in range(20):
            batch = [
                r
                async for r in harvest_flood(
                    piecewise, src, ZOOM, seeds=seeds, exits=exits_from_body, state=state, limit=5
                )
            ]
            seeds = []
            seen |= {r.tile for r in batch if r.body}
            if not batch:
                break

        assert seen == whole
        assert len(piecewise.requested) == len(straight.requested)

    async def test_a_snapshot_round_trips(self, tmp_path) -> None:  # type: ignore[no-untyped-def]
        src = source()
        start = next(src.tiles_at(ZOOM))
        stub = StubFetcher(horizontal_road(start, 30))
        state = FloodState(grid=ProbedGrid.for_bounds(src.bounds, src, ZOOM), zoom=ZOOM)
        _ = [
            r
            async for r in harvest_flood(
                stub, src, ZOOM, seeds=[start], exits=exits_from_body, state=state, limit=6
            )
        ]

        path = tmp_path / "flood.state"
        state.save(path)
        restored = FloodState.load(path)

        assert restored.probed == state.probed
        assert restored.hits == state.hits
        assert restored.zoom == state.zoom
        assert restored.frontier == state.frontier
        assert all(t in restored.grid for t in stub.requested)

    async def test_resuming_from_a_snapshot_finishes_the_job(self, tmp_path) -> None:  # type: ignore[no-untyped-def]
        src = source()
        start = next(src.tiles_at(ZOOM))
        covered = horizontal_road(start, 25)

        first = StubFetcher(covered)
        state = FloodState(grid=ProbedGrid.for_bounds(src.bounds, src, ZOOM), zoom=ZOOM)
        found = {
            r.tile
            async for r in harvest_flood(
                first, src, ZOOM, seeds=[start], exits=exits_from_body, state=state, limit=8
            )
            if r.body
        }
        path = tmp_path / "flood.state"
        state.save(path)

        # a crash here, then a fresh process
        resumed = FloodState.load(path)
        second = StubFetcher(covered)
        found |= {
            r.tile
            async for r in harvest_flood(
                second, src, ZOOM, seeds=[], exits=exits_from_body, state=resumed
            )
            if r.body
        }

        assert found == set(covered)
        assert not set(first.requested) & set(second.requested)

    async def test_a_state_built_for_another_zoom_is_refused(self) -> None:
        src = source()
        state = FloodState(grid=ProbedGrid(0, 0, 10, 10), zoom=14)
        stub = StubFetcher({})
        with pytest.raises(ValueError, match="built for zoom 14"):
            _ = [
                r
                async for r in harvest_flood(
                    stub, src, ZOOM, seeds=[], exits=exits_from_body, state=state
                )
            ]

    def test_a_snapshot_of_something_else_is_refused(self, tmp_path) -> None:  # type: ignore[no-untyped-def]
        path = tmp_path / "not-a-snapshot"
        path.write_bytes(b"nope, this is something else entirely")
        with pytest.raises(ValueError, match="not a flood snapshot"):
            FloodState.load(path)


class TestDirections:
    def test_only_the_four_edges_are_expanded(self) -> None:
        # Diagonals cost 33-52% more requests and their recall advantage vanishes
        # once seeding is adequate, so they are deliberately absent.
        assert set(DIRECTIONS) == {"W", "E", "N", "S"}
        assert all(abs(dx) + abs(dy) == 1 for dx, dy in DIRECTIONS.values())

    def test_offsets_agree_with_the_tile_grid(self) -> None:
        tile = Tile(14, 100, 100)
        west = Tile(14, tile.x + DIRECTIONS["W"][0], tile.y + DIRECTIONS["W"][1])
        north = Tile(14, tile.x + DIRECTIONS["N"][0], tile.y + DIRECTIONS["N"][1])
        assert west.bounds[0] < tile.bounds[0]
        assert north.bounds[3] > tile.bounds[3]
        assert math.isclose(west.bounds[2], tile.bounds[0])

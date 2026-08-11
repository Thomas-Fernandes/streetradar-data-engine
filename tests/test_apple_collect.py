"""Wiring the flood, the decoder and the part writer together.

Driven against a stub fetcher serving real encoded protobuf, so the decode under
test is the real one. What matters here is not that the flood expands — that is
`test_flood.py` — but the three joins: that a tile is decoded once rather than
twice, that panoramas reach the writer with their tile coordinates intact, and
that nothing snapshots the resume state on the caller's behalf.
"""

from __future__ import annotations

from collections.abc import Mapping
from pathlib import Path

import pyarrow.parquet as pq
import pytest

from streetradar.apple import (
    APPLE_ZOOM,
    CAPTURE_PRECISION,
    apple_source,
    collect_coverage,
    flood_state,
)
from streetradar.flood import FloodState
from streetradar.proto import apple_coverage_pb2 as pb
from streetradar.store import PartWriter
from streetradar.tiles import Tile, TileSource

BOUNDS = (5.7, 49.4, 6.6, 50.2)
TOKEN = "test-token"
FULL = 255 * 64  # the tile's own coordinate span, in sixty-fourths of a pixel


def source() -> TileSource:
    return apple_source(BOUNDS, token=TOKEN)


def body(tile: Tile, positions: list[tuple[float, float]], *, when: int = 1554163200000) -> bytes:
    """A coverage tile holding panoramas at the given fractions of the tile.

    `(0.0, 0.0)` is the north-west corner. The y flip is applied here so the
    tests can be written in map terms rather than wire terms.
    """
    message = pb.GroundMetadataTile()
    message.tile_coordinate.x, message.tile_coordinate.y = tile.x, tile.y
    message.tile_coordinate.z = tile.z
    build = message.build_table.add()
    build.build_id = 1203880071
    build.coverage_type = pb.GroundMetadataTile.GroundDataBuild.CAR
    for i, (fx, fy) in enumerate(positions):
        pano = message.pano.add()
        pano.panoid = 6418618400553645007 + i
        pano.timestamp = when + i * 1000
        pano.build_table_idx = 0
        pano.tile_position.x = int(fx * FULL)
        pano.tile_position.y = int((1.0 - fy) * FULL)
    return message.SerializeToString()


def road(start: Tile, length: int) -> dict[Tile, bytes]:
    """Covered tiles running east, each with panoramas at its west and east edges."""
    return {
        Tile(start.z, start.x + i, start.y): body(
            Tile(start.z, start.x + i, start.y), [(0.01, 0.5), (0.5, 0.5), (0.99, 0.5)]
        )
        for i in range(length)
    }


class StubFetcher:
    """Serves bodies by the tile named in Apple's headers, not in the URL."""

    def __init__(self, covered: Mapping[Tile, bytes]) -> None:
        self._covered = covered
        self.requested: list[Tile] = []

    @property
    def concurrency(self) -> int:
        return 8

    async def fetch_tile(self, url: str, headers: Mapping[str, str]) -> bytes | None:
        tile = Tile(
            int(headers["maps-tile-z"]), int(headers["maps-tile-x"]), int(headers["maps-tile-y"])
        )
        self.requested.append(tile)
        return self._covered.get(tile)


def writer_at(directory: Path, *, rows_per_part: int = 20_000_000) -> PartWriter:
    return PartWriter(
        directory,
        provider="apple",
        run_id="test",
        capture_precision=CAPTURE_PRECISION,
        rows_per_part=rows_per_part,
    )


async def collect(
    covered: Mapping[Tile, bytes], writer: PartWriter, seed: Tile
) -> tuple[list[Path], StubFetcher, FloodState]:
    fetcher = StubFetcher(covered)
    src = source()
    state = flood_state(src)
    parts = [
        part
        async for part in collect_coverage(fetcher, src, seeds=[seed], writer=writer, state=state)
    ]
    return parts, fetcher, state


class TestCollecting:
    async def test_panoramas_reach_the_writer(self, tmp_path: Path) -> None:
        seed = Tile(APPLE_ZOOM, 67756, 44565)
        writer = writer_at(tmp_path)
        parts, _, _ = await collect(road(seed, 4), writer, seed)
        assert writer.rows_written == 12  # 4 tiles x 3 panoramas
        assert len(parts) == 1
        assert pq.ParquetFile(parts[0]).metadata.num_rows == 12

    async def test_rows_carry_the_tile_they_came_from(self, tmp_path: Path) -> None:
        # The sort key the whole storage decision rests on. Losing it here would
        # cost 5x in file size and be invisible until someone measured.
        seed = Tile(APPLE_ZOOM, 67756, 44565)
        writer = writer_at(tmp_path)
        parts, _, _ = await collect(road(seed, 3), writer, seed)
        table = pq.read_table(parts[0])
        assert set(table.column("tile_x").to_pylist()) == {67756, 67757, 67758}
        assert set(table.column("tile_y").to_pylist()) == {44565}

    async def test_capture_times_and_builds_survive(self, tmp_path: Path) -> None:
        seed = Tile(APPLE_ZOOM, 67756, 44565)
        writer = writer_at(tmp_path)
        parts, _, _ = await collect({seed: body(seed, [(0.5, 0.5)])}, writer, seed)
        table = pq.read_table(parts[0])
        assert table.column("captured_at").to_pylist()[0] is not None
        assert table.column("build_id").to_pylist() == [1203880071]
        assert table.column("coverage_type").to_pylist() == ["car"]

    async def test_uncovered_tiles_contribute_nothing(self, tmp_path: Path) -> None:
        seed = Tile(APPLE_ZOOM, 67756, 44565)
        writer = writer_at(tmp_path)
        parts, fetcher, _ = await collect({}, writer, seed)
        assert parts == []
        assert writer.rows_written == 0
        assert fetcher.requested == [seed]

    async def test_a_part_is_written_once_it_fills(self, tmp_path: Path) -> None:
        seed = Tile(APPLE_ZOOM, 67756, 44565)
        writer = writer_at(tmp_path, rows_per_part=3)
        parts, _, _ = await collect(road(seed, 4), writer, seed)
        assert len(parts) == 4
        assert all(p.exists() for p in parts)
        assert writer.pending == 0


class TestDecodingOnce:
    async def test_each_tile_is_parsed_a_single_time(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        # Decoding for the exits and again for the panoramas would halve
        # throughput, from 2,544 tiles/s to below what the fetcher supplies.
        import streetradar.apple as apple

        calls: list[Tile] = []
        real = apple.decode_coverage_tile

        def counted(payload: bytes | None, tile: Tile) -> apple.CoverageTile:
            calls.append(tile)
            return real(payload, tile)

        monkeypatch.setattr(apple, "decode_coverage_tile", counted)
        seed = Tile(APPLE_ZOOM, 67756, 44565)
        await collect(road(seed, 5), writer_at(tmp_path), seed)
        assert len(calls) == len(set(calls)) == 5


class TestFloodIntegration:
    async def test_expansion_follows_the_exits_the_panoramas_imply(self, tmp_path: Path) -> None:
        # Panoramas hug the west and east edges, so the flood must run along the
        # road and probe only one tile past each end, not spread north and south.
        seed = Tile(APPLE_ZOOM, 67756, 44565)
        _, fetcher, _ = await collect(road(seed, 4), writer_at(tmp_path), seed)
        assert {t.y for t in fetcher.requested} == {44565}
        assert len(fetcher.requested) == 6  # 4 covered plus one dead end each side

    async def test_the_state_records_what_was_probed(self, tmp_path: Path) -> None:
        seed = Tile(APPLE_ZOOM, 67756, 44565)
        _, fetcher, state = await collect(road(seed, 4), writer_at(tmp_path), seed)
        assert state.hits == 4
        assert state.probed == len(fetcher.requested)

    async def test_a_limit_bounds_the_requests(self, tmp_path: Path) -> None:
        seed = Tile(APPLE_ZOOM, 67756, 44565)
        fetcher = StubFetcher(road(seed, 20))
        src = source()
        state = flood_state(src)
        writer = writer_at(tmp_path)
        async for _ in collect_coverage(
            fetcher, src, seeds=[seed], writer=writer, state=state, limit=5
        ):
            pass
        assert len(fetcher.requested) <= 5
        assert state.frontier  # the rest stays queued for the next call


class TestOrderingRule:
    async def test_nothing_snapshots_the_state_for_the_caller(self, tmp_path: Path) -> None:
        # Snapshotting before the rows are durable is what turns a crash into
        # silently missing coverage. Only the caller knows when they are.
        seed = Tile(APPLE_ZOOM, 67756, 44565)
        await collect(road(seed, 4), writer_at(tmp_path), seed)
        assert not list(tmp_path.glob("*.state"))
        assert not list(tmp_path.glob("*.tmp"))

    async def test_a_writer_of_the_wrong_precision_is_refused(self, tmp_path: Path) -> None:
        # Apple reports a full UTC timestamp; recording it as month precision
        # would make the front end under-claim on every Apple panorama.
        writer = PartWriter(tmp_path, provider="apple", run_id="test", capture_precision="month")
        seed = Tile(APPLE_ZOOM, 67756, 44565)
        with pytest.raises(ValueError, match="must be 'second'"):
            await collect({seed: body(seed, [(0.5, 0.5)])}, writer, seed)


class TestFloodState:
    def test_a_fresh_state_is_at_apple_s_zoom_and_empty(self) -> None:
        state = flood_state(source())
        assert state.zoom == APPLE_ZOOM
        assert state.probed == 0
        assert state.frontier == []

    def test_it_survives_a_round_trip(self, tmp_path: Path) -> None:
        state = flood_state(source())
        state.grid.add(Tile(APPLE_ZOOM, 67756, 44565))
        state.hits = 1
        path = tmp_path / "flood.state"
        state.save(path)
        restored = FloodState.load(path)
        assert restored.zoom == APPLE_ZOOM
        assert restored.probed == 1
        assert Tile(APPLE_ZOOM, 67756, 44565) in restored.grid

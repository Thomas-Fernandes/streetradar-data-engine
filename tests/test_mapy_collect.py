"""The Mapy collector: sweep the grid, walk out of every hit.

Driven against a stub that decodes the real FRPC call and encodes a real FRPC
response, so the codec is exercised in both directions and the synthetic road
graph is the only thing invented.

The properties worth pinning are the two that the previous engine got wrong: that
a panorama reached from two different tiles costs one request rather than two,
and that nothing snapshots the resume state on the collector's own initiative.
"""

from __future__ import annotations

import datetime
from collections.abc import Mapping
from pathlib import Path

import pyarrow.parquet as pq
import pyfrpc
import pytest

from streetradar.mapy import (
    CAPTURE_PRECISION,
    SWEEP_ZOOM,
    SeenIds,
    SweepState,
    collect_mapy,
    tile_of,
)
from streetradar.model import ObservationBatch
from streetradar.store import PartWriter

# A small box near Prague, inside what the previous engine actually collected.
# Wide enough to hold 81 tiles at zoom 17, so a resumed run has something
# left to sweep and the dedup across tiles is actually exercised.
BOUNDS = (15.010, 50.070, 15.030, 50.085)
WHEN = datetime.datetime(2022, 10, 20, 15, 38, 11, tzinfo=datetime.UTC)


def pan_info(pid: int, lon: float, lat: float) -> dict[str, object]:
    return {
        "pid": pid,
        "mark": {"lon": lon, "lat": lat, "alt": 245.7},
        "createdAt": WHEN,
        "provider": "cyclomedia",
        "timeline": [2014, 2016, 2018, 2022],
    }


class Road:
    """A chain of panoramas laid west to east, as a stub FRPC endpoint."""

    def __init__(self, count: int = 40, *, spacing: float = 0.00004) -> None:
        west, south, east, north = BOUNDS
        centre = (west + east) / 2
        self.panoramas = {
            1000 + i: (centre + i * spacing, (south + north) / 2) for i in range(count)
        }
        self.calls: list[str] = []

    def nearest(self, lon: float, lat: float) -> int | None:
        best, distance = None, 1e9
        for pid, (plon, plat) in self.panoramas.items():
            d = (plon - lon) ** 2 + (plat - lat) ** 2
            if d < distance:
                best, distance = pid, d
        # Roughly 150 m in degrees, which is the search radius the module uses.
        return best if distance < (0.0015) ** 2 else None

    @property
    def concurrency(self) -> int:
        return 8

    async def post(self, url: str, body: bytes, *, headers: Mapping[str, str]) -> bytes:
        call = pyfrpc.decode(body)
        self.calls.append(call.name)
        result: dict[str, object]
        if call.name == "getbest":
            lon, lat = call.args[0], call.args[1]
            pid = self.nearest(lon, lat)
            result = {"panInfo": pan_info(pid, *self.panoramas[pid])} if pid else {}
        else:
            pid = int(call.args[0])
            neighbours = []
            for step in (-1, 1):
                other = pid + step
                if other in self.panoramas:
                    neighbours.append(
                        {"angle": 0.0, "near": pan_info(other, *self.panoramas[other]), "far": {}}
                    )
            result = {"pid": pid, "count": len(neighbours), "neighbours": neighbours}
        return bytes(pyfrpc.encode(pyfrpc.FrpcResponse(data={"result": result}), version=0x0201))


def writer_at(directory: Path, *, rows_per_part: int = 20_000_000) -> PartWriter[ObservationBatch]:
    return PartWriter(
        directory,
        ObservationBatch(provider="mapy", run_id="test", capture_precision=CAPTURE_PRECISION),
        rows_per_part=rows_per_part,
    )


async def collect(
    road: Road, writer: PartWriter[ObservationBatch], state: SweepState, **kwargs: int
) -> list[Path]:
    return [part async for part in collect_mapy(road, BOUNDS, writer=writer, state=state, **kwargs)]


class TestSeenIds:
    def test_an_id_is_recorded_once(self) -> None:
        seen = SeenIds(1024)
        assert seen.add(500) and not seen.add(500)
        assert 500 in seen and len(seen) == 1

    def test_an_id_outside_the_range_counts_as_seen(self) -> None:
        # It cannot be recorded, so treating it as new would have every pass
        # walk it again for ever.
        seen = SeenIds(1024)
        assert not seen.add(99999)
        assert 99999 in seen

    def test_the_default_range_covers_mapy_s_ids_in_a_few_megabytes(self) -> None:
        # Measured on the previous export: 2,245,132 to 108,309,448. The engine
        # this replaces held the same information as a 162 MB JSON set.
        seen = SeenIds()
        assert seen.capacity > 108_309_448
        assert (seen.capacity + 7) // 8 < 64 * 1024 * 1024

    def test_a_zero_range_is_refused(self) -> None:
        with pytest.raises(ValueError, match="capacity must be positive"):
            SeenIds(0)


class TestSweepState:
    def test_a_fresh_state_is_empty_at_the_sweep_zoom(self) -> None:
        state = SweepState.for_bounds(BOUNDS)
        assert state.zoom == SWEEP_ZOOM
        assert state.swept == 0 and state.hits == 0 and state.panoramas == 0

    def test_it_survives_a_round_trip(self, tmp_path: Path) -> None:
        from streetradar.tiles import Tile

        state = SweepState.for_bounds(BOUNDS)
        tile = Tile(SWEEP_ZOOM, 71003, 44408)
        state.grid.add(tile)
        state.seen.add(92512299)
        state.hits, state.panoramas = 1, 7
        path = tmp_path / "mapy.state"
        state.save(path)

        back = SweepState.load(path)
        assert back.zoom == SWEEP_ZOOM
        assert back.swept == 1 and tile in back.grid
        assert len(back.seen) == 1 and 92512299 in back.seen
        assert (back.hits, back.panoramas) == (1, 7)

    def test_a_foreign_file_is_refused(self, tmp_path: Path) -> None:
        path = tmp_path / "not.state"
        path.write_bytes(b"nonsense")
        with pytest.raises(ValueError, match="not a Mapy sweep snapshot"):
            SweepState.load(path)

    def test_no_temporary_file_survives_a_save(self, tmp_path: Path) -> None:
        SweepState.for_bounds(BOUNDS).save(tmp_path / "mapy.state")
        assert not list(tmp_path.glob("*.tmp"))


class TestCollecting:
    async def test_the_whole_road_is_collected(self, tmp_path: Path) -> None:
        road, state = Road(40), SweepState.for_bounds(BOUNDS)
        writer = writer_at(tmp_path)
        parts = await collect(road, writer, state)
        assert writer.rows_written == 40
        assert len(parts) == 1
        assert pq.ParquetFile(parts[0]).metadata.num_rows == 40

    async def test_a_panorama_is_never_walked_twice(self, tmp_path: Path) -> None:
        # Every tile of the sweep sits on the same road, so without the id
        # bitmap each would re-walk all forty panoramas.
        road, state = Road(40), SweepState.for_bounds(BOUNDS)
        await collect(road, writer_at(tmp_path), state)
        assert road.calls.count("getneighbours") == 40
        assert state.panoramas == 40

    async def test_dates_and_provider_reach_the_part(self, tmp_path: Path) -> None:
        road, state = Road(5), SweepState.for_bounds(BOUNDS)
        writer = writer_at(tmp_path)
        parts = await collect(road, writer, state)
        table = pq.read_table(parts[0])
        assert all(v is not None for v in table.column("captured_at").to_pylist())
        assert set(table.column("coverage_type").to_pylist()) == {"cyclomedia"}

    async def test_rows_carry_the_tile_their_position_falls_in(self, tmp_path: Path) -> None:
        road, state = Road(40), SweepState.for_bounds(BOUNDS)
        writer = writer_at(tmp_path)
        parts = await collect(road, writer, state)
        table = pq.read_table(parts[0])
        pairs = set(
            zip(
                table.column("tile_x").to_pylist(),
                table.column("tile_y").to_pylist(),
                strict=True,
            )
        )
        assert pairs
        assert all(0 < x < (1 << SWEEP_ZOOM) for x, _ in pairs)

    async def test_every_tile_is_marked_swept(self, tmp_path: Path) -> None:
        road, state = Road(40), SweepState.for_bounds(BOUNDS)
        await collect(road, writer_at(tmp_path), state)
        assert state.swept > 0
        assert road.calls.count("getbest") == state.swept

    async def test_an_empty_region_yields_nothing(self, tmp_path: Path) -> None:
        road, state = Road(0), SweepState.for_bounds(BOUNDS)
        writer = writer_at(tmp_path)
        assert await collect(road, writer, state) == []
        assert writer.rows_written == 0
        assert state.hits == 0
        assert state.swept > 0  # the tiles were still asked

    async def test_a_limit_bounds_the_sweep(self, tmp_path: Path) -> None:
        road, state = Road(40), SweepState.for_bounds(BOUNDS)
        await collect(road, writer_at(tmp_path), state, limit=3)
        assert state.swept <= 3

    async def test_resuming_does_not_re_sweep(self, tmp_path: Path) -> None:
        road, state = Road(40), SweepState.for_bounds(BOUNDS)
        await collect(road, writer_at(tmp_path), state, limit=2)
        first = road.calls.count("getbest")
        await collect(road, writer_at(tmp_path / "second"), state)
        assert road.calls.count("getbest") == state.swept > first

    async def test_a_part_is_written_once_it_fills(self, tmp_path: Path) -> None:
        road, state = Road(40), SweepState.for_bounds(BOUNDS)
        writer = writer_at(tmp_path, rows_per_part=5)
        parts = await collect(road, writer, state)
        assert len(parts) > 1
        assert writer.pending == 0


class TestOrderingRule:
    async def test_nothing_snapshots_the_state_for_the_caller(self, tmp_path: Path) -> None:
        # Snapshotting before the rows are durable is what turns a crash into
        # tiles marked swept whose panoramas were never written.
        road, state = Road(10), SweepState.for_bounds(BOUNDS)
        await collect(road, writer_at(tmp_path), state)
        assert not list(tmp_path.glob("*.state"))
        assert not list(tmp_path.glob("*.tmp"))

    async def test_a_writer_of_the_wrong_precision_is_refused(self, tmp_path: Path) -> None:
        # Mapy reports a full timestamp; recording it as year precision would
        # make the front end under-claim on the best-dated provider we have.
        writer = PartWriter(
            tmp_path,
            ObservationBatch(provider="mapy", run_id="t", capture_precision="year"),
        )
        with pytest.raises(ValueError, match="must be 'second'"):
            await collect(Road(5), writer, SweepState.for_bounds(BOUNDS))


class TestTileOf:
    def test_a_position_lands_in_the_tile_that_contains_it(self) -> None:
        from streetradar.mapy import Panorama
        from streetradar.tiles import Tile

        pano = Panorama(1, 15.017908, 50.076914, None, None)
        x, y = tile_of(pano, SWEEP_ZOOM)
        west, south, east, north = Tile(SWEEP_ZOOM, x, y).bounds
        assert west <= pano.lon <= east
        assert south <= pano.lat <= north

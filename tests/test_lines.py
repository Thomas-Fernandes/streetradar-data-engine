"""Coverage that arrives as road lines rather than panorama positions.

The property everything rests on is the round trip: lines are stored in the
tile's own integer coordinates, so the WGS84 the tiling stage emits has to come
back out of them. Measured on the real Já fixture as well as synthetic tiles,
because a projection that is self-consistent but wrong would pass any test built
only from its own output.
"""

from __future__ import annotations

import json
import pathlib
from collections.abc import Mapping
from pathlib import Path

import pyarrow.parquet as pq
import pytest

from streetradar.decode import decode_tile
from streetradar.lines import LineBatch, collect_lines, line_schema
from streetradar.store import PartWriter
from streetradar.tiles import Tile, TileSource, offset_to_wgs84
from streetradar.tileset import line_features, read_line_parts

FIXTURE = pathlib.Path(__file__).parent / "fixtures" / "ja360-z14-7195-4355.pbf"
FIXTURE_TILE = Tile(14, 7195, 4355)
LAYER = "driveline"
EXTENT = 4096


def batch(zoom: int = 14, extent: int = EXTENT) -> LineBatch:
    return LineBatch(provider="ja", run_id="test", zoom=zoom, extent=extent)


def square() -> list[list[tuple[float, float]]]:
    return [[(100.0, 200.0), (300.0, 400.0), (500.0, 600.0)]]


class TestSchema:
    def test_coordinates_are_lists_of_small_integers(self) -> None:
        # int16 covers a 4096-unit extent with room for a clipped edge, and is
        # what makes a line 3.74 bytes per vertex instead of 7.40.
        schema = line_schema()
        assert str(schema.field("x").type) == "list<item: int16>"
        assert str(schema.field("y").type) == "list<item: int16>"

    def test_zoom_and_extent_travel_with_the_rows(self) -> None:
        # Without them the coordinates are integers with no position at all.
        names = set(line_schema().names)
        assert {"zoom", "extent", "tile_x", "tile_y"} <= names

    def test_there_is_no_panorama_identity(self) -> None:
        # A line is not a panorama; inventing an id would imply a re-drive could
        # be told from the original, and it cannot.
        assert "pano_id" not in line_schema().names


class TestLineBatch:
    def test_it_counts_lines_and_vertices_apart(self) -> None:
        b = batch()
        b.add(tile_x=1, tile_y=2, lines=square())
        assert len(b) == 1
        assert b.vertices == 3

    def test_coordinates_are_rounded_on_the_way_in(self) -> None:
        # Rounded here so that every writer quantises identically; two parts
        # disagreeing would put the same road in two places.
        b = batch()
        b.add(tile_x=1, tile_y=2, lines=[[(100.4, 200.6), (300.5, 400.5)]])
        table = b.to_arrow()
        assert table.column("x").to_pylist() == [[100, 300]]
        assert table.column("y").to_pylist() == [[201, 400]]

    def test_a_degenerate_line_is_dropped(self) -> None:
        # One vertex is not geometry; it would render as nothing and cost a row.
        b = batch()
        b.add(tile_x=1, tile_y=2, lines=[[(1.0, 1.0)], [(1.0, 1.0), (2.0, 2.0)]])
        assert len(b) == 1

    def test_these_providers_declare_their_dates_unknown(self) -> None:
        # Já and Naver publish road classification and nothing temporal, so the
        # column is null and the precision says so rather than implying a date.
        b = batch()
        b.add(tile_x=1, tile_y=2, lines=square())
        table = b.to_arrow()
        assert table.column("captured_at").to_pylist() == [None]
        assert table.column("capture_precision").to_pylist() == ["unknown"]

    def test_a_date_is_carried_when_there_is_one(self) -> None:
        b = LineBatch(provider="x", run_id="r", zoom=14, extent=EXTENT, capture_precision="month")
        b.add(tile_x=1, tile_y=2, lines=square(), captured_at=[1554163200000])
        assert b.to_arrow().column("captured_at").to_pylist() != [None]

    def test_a_date_column_of_the_wrong_length_is_refused(self) -> None:
        with pytest.raises(ValueError, match="captured_at has 2 values for 1 lines"):
            batch().add(tile_x=1, tile_y=2, lines=square(), captured_at=[1, 2])

    def test_rows_come_out_sorted_by_tile(self) -> None:
        b = batch()
        b.add(tile_x=9, tile_y=0, lines=square())
        b.add(tile_x=1, tile_y=0, lines=square())
        assert b.to_arrow().column("tile_x").to_pylist() == [1, 9]

    def test_an_empty_batch_still_matches_the_schema(self) -> None:
        assert batch().to_arrow().schema == line_schema()

    def test_clear_empties_it(self) -> None:
        b = batch()
        b.add(tile_x=1, tile_y=2, lines=square())
        b.clear()
        assert len(b) == 0 and b.vertices == 0

    @pytest.mark.parametrize(("zoom", "extent"), [(25, 4096), (14, 0)])
    def test_an_unusable_grid_is_refused(self, zoom: int, extent: int) -> None:
        with pytest.raises(ValueError):
            LineBatch(provider="x", run_id="r", zoom=zoom, extent=extent)

    def test_an_unknown_precision_is_refused(self) -> None:
        with pytest.raises(ValueError, match="unknown capture precision"):
            LineBatch(provider="x", run_id="r", zoom=14, extent=EXTENT, capture_precision="epoch")  # type: ignore[arg-type]


class TestRoundTrip:
    """Store the tile's own coordinates, get the same WGS84 back."""

    def test_the_real_fixture_survives_storage_and_reprojection(self, tmp_path: Path) -> None:
        body = FIXTURE.read_bytes()
        projected = decode_tile(body, FIXTURE_TILE, layer=LAYER)
        local = decode_tile(body, FIXTURE_TILE, layer=LAYER, project=False)
        assert projected.lines and len(projected.lines) == len(local.lines)

        b = LineBatch(provider="ja", run_id="t", zoom=FIXTURE_TILE.z, extent=local.extent)
        b.add(
            tile_x=FIXTURE_TILE.x,
            tile_y=FIXTURE_TILE.y,
            lines=[line.coords for line in local.lines],
        )
        PartWriter(tmp_path, b).flush()

        rebuilt = [
            json.loads(row)["geometry"]["coordinates"]
            for blob in line_features(read_line_parts(tmp_path))
            for row in blob.decode().splitlines()
        ]
        assert len(rebuilt) == len(projected.lines)

        # 11.1 cm was the worst deviation measured over 398,407 real vertices;
        # 1e-5 degrees is about a metre, so this fails long before the error
        # could matter and long after honest rounding.
        for original, back in zip(projected.lines, rebuilt, strict=True):
            for (lon, lat), (rlon, rlat) in zip(original.coords, back, strict=True):
                assert rlon == pytest.approx(lon, abs=1e-5)
                assert rlat == pytest.approx(lat, abs=1e-5)

    def test_the_projection_is_the_one_the_tile_grid_defines(self) -> None:
        # Not merely self-consistent: the corners must land on the tile's own
        # bounds, which come from an independent calculation.
        b = batch()
        b.add(tile_x=FIXTURE_TILE.x, tile_y=FIXTURE_TILE.y, lines=[[(0.0, 0.0), (4096.0, 4096.0)]])
        west, south, east, north = FIXTURE_TILE.bounds
        assert offset_to_wgs84(FIXTURE_TILE, 0.0, 0.0, EXTENT) == pytest.approx((west, south))
        assert offset_to_wgs84(FIXTURE_TILE, 4096.0, 4096.0, EXTENT) == pytest.approx((east, north))

    def test_undated_lines_carry_no_properties(self, tmp_path: Path) -> None:
        b = batch()
        b.add(tile_x=FIXTURE_TILE.x, tile_y=FIXTURE_TILE.y, lines=square())
        PartWriter(tmp_path, b).flush()
        row = next(iter(line_features(read_line_parts(tmp_path)))).decode().splitlines()[0]
        assert json.loads(row)["properties"] == {}

    def test_features_are_linestrings(self, tmp_path: Path) -> None:
        b = batch()
        b.add(tile_x=FIXTURE_TILE.x, tile_y=FIXTURE_TILE.y, lines=square())
        PartWriter(tmp_path, b).flush()
        row = next(iter(line_features(read_line_parts(tmp_path)))).decode().splitlines()[0]
        assert json.loads(row)["geometry"]["type"] == "LineString"


class StubFetcher:
    def __init__(self, covered: Mapping[Tile, bytes]) -> None:
        self._covered = covered
        self.requested: list[Tile] = []

    @property
    def concurrency(self) -> int:
        return 8

    async def fetch_tile(self, url: str, headers: Mapping[str, str]) -> bytes | None:
        z, x, y = (int(p) for p in url.rstrip("/").split("/")[-3:])
        self.requested.append(Tile(z, x, y))
        return self._covered.get(Tile(z, x, y))


def ja_source() -> TileSource:
    west, south, east, north = FIXTURE_TILE.bounds
    return TileSource(
        id="ja",
        url_template="https://example.invalid/{z}/{x}/{y}",
        min_zoom=11,
        max_zoom=14,
        bounds=(west + 1e-6, south + 1e-6, east - 1e-6, north - 1e-6),
    )


class TestCollectLines:
    async def collect(self, tmp_path: Path, covered: Mapping[Tile, bytes]) -> list[Path]:
        writer = PartWriter(tmp_path, LineBatch(provider="ja", run_id="t", zoom=14, extent=EXTENT))
        return [
            part
            async for part in collect_lines(
                StubFetcher(covered),
                ja_source(),
                layer=LAYER,
                target_zoom=14,
                writer=writer,
                start_zoom=11,
            )
        ]

    async def test_geometry_from_the_target_zoom_is_written(self, tmp_path: Path) -> None:
        body = FIXTURE.read_bytes()
        covered = {FIXTURE_TILE: body}
        # The descent needs its ancestors to return something to reach z14.
        tile = FIXTURE_TILE
        for _ in range(3):
            tile = tile.parent
            covered[tile] = body
        parts = await self.collect(tmp_path, covered)
        assert len(parts) == 1
        expected = len(decode_tile(body, FIXTURE_TILE, layer=LAYER).lines)
        assert pq.ParquetFile(parts[0]).metadata.num_rows == expected

    async def test_the_pruning_levels_are_not_stored(self, tmp_path: Path) -> None:
        # harvest_pyramid yields every level it visits. Storing the coarse ones
        # would double-count every road at the provider's own generalisation.
        parts = await self.collect(tmp_path, {})
        assert parts == []

    async def test_a_writer_at_the_wrong_zoom_is_refused(self, tmp_path: Path) -> None:
        writer = PartWriter(tmp_path, LineBatch(provider="ja", run_id="t", zoom=15, extent=EXTENT))
        with pytest.raises(ValueError, match="records zoom 15 but the descent targets 14"):
            async for _ in collect_lines(
                StubFetcher({}), ja_source(), layer=LAYER, target_zoom=14, writer=writer
            ):
                pass

    async def test_an_extent_that_changed_is_refused(self, tmp_path: Path) -> None:
        # Every vertex would be misplaced by a factor, silently and everywhere.
        body = FIXTURE.read_bytes()
        covered = {FIXTURE_TILE: body}
        tile = FIXTURE_TILE
        for _ in range(3):
            tile = tile.parent
            covered[tile] = body
        writer = PartWriter(tmp_path, LineBatch(provider="ja", run_id="t", zoom=14, extent=512))
        with pytest.raises(ValueError, match="extent 4096"):
            async for _ in collect_lines(
                StubFetcher(covered),
                ja_source(),
                layer=LAYER,
                target_zoom=14,
                writer=writer,
                start_zoom=11,
            ):
                pass

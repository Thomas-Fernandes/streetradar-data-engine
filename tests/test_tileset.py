"""Building the published tileset.

`tippecanoe` does the tiling, so what is worth pinning here is everything around
it: the feature stream it is fed, the flags it is invoked with, and the numbers
read back out of the archive — which is what the publish gate decides on.

The tests that actually run `tippecanoe` skip when it is absent. It is a system
binary rather than a Python dependency, so a developer machine without it should
still be able to run the suite.
"""

from __future__ import annotations

import datetime
import json
import shutil
import sqlite3
from collections.abc import Iterator
from pathlib import Path

import pyarrow as pa
import pytest

from streetradar.model import ObservationBatch
from streetradar.store import PartWriter
from streetradar.tileset import (
    TilesetSpec,
    ToolMissing,
    build_tileset,
    inspect_mbtiles,
    observation_features,
    read_parts,
)

needs_tippecanoe = pytest.mark.skipif(
    shutil.which("tippecanoe") is None, reason="tippecanoe is not installed"
)
needs_pmtiles = pytest.mark.skipif(
    shutil.which("pmtiles") is None, reason="the pmtiles CLI is not installed"
)


def ms(year: int, month: int) -> int:
    return int(datetime.datetime(year, month, 1, 12, tzinfo=datetime.UTC).timestamp() * 1000)


def observations(count: int = 50, *, dated: bool = True) -> pa.Table:
    """A small patch of coverage, spread widely enough to occupy several tiles."""
    batch = ObservationBatch(provider="apple", run_id="test", capture_precision="second")
    for i in range(count):
        batch.add(
            tile_x=i,
            tile_y=0,
            pano_id=[1000 + i],
            lon=[6.0 + i * 0.01],
            lat=[49.6 + i * 0.01],
            captured_at=[ms(2021, 1 + i % 12) if dated else None],
        )
    return batch.to_arrow()


def features(table: pa.Table) -> Iterator[bytes]:
    return observation_features(table.to_batches())


def lines(table: pa.Table) -> list[dict[str, object]]:
    body = b"".join(features(table)).decode()
    return [json.loads(line) for line in body.splitlines()]


class TestSpec:
    def test_a_tileset_needs_a_layer_name(self) -> None:
        with pytest.raises(ValueError, match="needs a layer name"):
            TilesetSpec(layer="")

    def test_an_inverted_zoom_range_is_refused(self) -> None:
        with pytest.raises(ValueError, match="not usable"):
            TilesetSpec(layer="coverage", min_zoom=14, max_zoom=6)

    def test_the_default_maximum_is_sixteen(self) -> None:
        # Measured: z16 retains every input point, so z17 would quadruple the
        # tile count to carry information the archive already has.
        assert TilesetSpec(layer="coverage").max_zoom == 16

    def test_dense_tiles_are_thinned_rather_than_truncated(self) -> None:
        assert "--drop-densest-as-needed" in TilesetSpec(layer="c").argv(Path("out.mbtiles"))

    def test_the_maximum_zoom_is_never_extended_silently(self) -> None:
        # --extend-zooms-if-still-dropping would push past max_zoom on a dense
        # country, so two countries would publish archives of different depth
        # without anyone having chosen that.
        argv = TilesetSpec(layer="c").argv(Path("out.mbtiles"))
        assert not any("extend-zooms" in flag for flag in argv)

    def test_the_command_is_a_list_never_a_shell_string(self) -> None:
        argv = TilesetSpec(layer="c").argv(Path("has space.mbtiles"))
        assert argv[0] == "tippecanoe"
        assert "--output=has space.mbtiles" in argv


class TestFeatureStream:
    def test_every_observation_becomes_one_feature(self) -> None:
        assert len(lines(observations(50))) == 50

    def test_a_feature_is_a_point_in_wgs84(self) -> None:
        first = lines(observations(1))[0]
        assert first["type"] == "Feature"
        geometry = first["geometry"]
        assert isinstance(geometry, dict)
        assert geometry["type"] == "Point"
        lon, lat = geometry["coordinates"]
        assert 5.9 < lon < 6.1 and 49.5 < lat < 49.7

    def test_the_capture_month_travels_as_ym(self) -> None:
        properties = lines(observations(1))[0]["properties"]
        assert isinstance(properties, dict)
        assert properties["ym"] == 202101

    def test_an_undated_observation_carries_no_month(self) -> None:
        # Absent rather than zero: a sentinel month would filter like real data.
        properties = lines(observations(1, dated=False))[0]["properties"]
        assert properties == {}

    def test_an_empty_batch_yields_nothing(self) -> None:
        empty = ObservationBatch(provider="a", run_id="r", capture_precision="second").to_arrow()
        assert b"".join(features(empty)) == b""

    def test_output_is_newline_delimited(self) -> None:
        body = b"".join(features(observations(5)))
        assert body.endswith(b"\n")
        assert body.count(b"\n") == 5


class TestReadParts:
    def test_reads_parts_in_order_and_only_what_tiling_needs(self, tmp_path: Path) -> None:
        writer = PartWriter(tmp_path, provider="apple", run_id="r", capture_precision="second")
        for i in range(3):
            writer.batch.add(
                tile_x=i, tile_y=0, pano_id=[i], lon=[6.0], lat=[49.6], captured_at=[ms(2021, 1)]
            )
            writer.flush()
        batches = list(read_parts(tmp_path))
        assert sum(b.num_rows for b in batches) == 3
        assert set(batches[0].schema.names) == {"lon", "lat", "captured_at"}

    def test_an_empty_directory_yields_nothing(self, tmp_path: Path) -> None:
        assert list(read_parts(tmp_path)) == []


class TestMissingTools:
    def test_a_missing_tippecanoe_is_named(self, tmp_path: Path, monkeypatch) -> None:  # type: ignore[no-untyped-def]
        # Rather than a bare FileNotFoundError surfacing from subprocess halfway
        # through a run that has already spent hours collecting.
        monkeypatch.setattr(shutil, "which", lambda _: None)
        with pytest.raises(ToolMissing, match="tippecanoe is not on PATH"):
            build_tileset(iter([]), tmp_path / "out.mbtiles", TilesetSpec(layer="c"))

    def test_a_missing_pmtiles_is_named_before_any_tiling(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        monkeypatch.setattr(shutil, "which", lambda tool: None if tool == "pmtiles" else "/x")
        with pytest.raises(ToolMissing, match="pmtiles is not on PATH"):
            build_tileset(iter([]), tmp_path / "out.pmtiles", TilesetSpec(layer="c"))
        assert list(tmp_path.iterdir()) == []


@needs_tippecanoe
class TestBuild:
    def test_builds_an_archive_and_reports_it(self, tmp_path: Path) -> None:
        destination = tmp_path / "coverage.mbtiles"
        stats = build_tileset(
            features(observations(200)), destination, TilesetSpec(layer="coverage", max_zoom=8)
        )
        assert destination.exists()
        assert stats.tiles > 0
        assert stats.bytes > 0
        assert stats.zooms == tuple(range(9))

    def test_no_temporary_file_survives_a_build(self, tmp_path: Path) -> None:
        build_tileset(
            features(observations(50)),
            tmp_path / "coverage.mbtiles",
            TilesetSpec(layer="coverage", max_zoom=6),
        )
        assert not list(tmp_path.glob("*.tmp"))

    def test_the_layer_is_named_as_asked(self, tmp_path: Path) -> None:
        destination = tmp_path / "coverage.mbtiles"
        build_tileset(
            features(observations(50)), destination, TilesetSpec(layer="driveline", max_zoom=6)
        )
        connection = sqlite3.connect(destination)
        metadata = dict(connection.execute("select name, value from metadata").fetchall())
        connection.close()
        assert "driveline" in metadata["json"]

    def test_a_failing_tippecanoe_leaves_nothing_behind(self, tmp_path: Path) -> None:
        # Malformed input: the archive must not exist at all rather than exist
        # empty, which the publish gate could mistake for a country with no
        # coverage.
        destination = tmp_path / "coverage.mbtiles"
        with pytest.raises(RuntimeError, match="exited with status"):
            build_tileset(iter([b"this is not geojson\n"]), destination, TilesetSpec(layer="c"))
        assert not destination.exists()
        assert not list(tmp_path.glob("*.tmp"))

    @needs_pmtiles
    def test_a_pmtiles_destination_is_converted(self, tmp_path: Path) -> None:
        # One object served from R2 by range request, against several hundred
        # thousand for a directory of tiles.
        destination = tmp_path / "coverage.pmtiles"
        stats = build_tileset(
            features(observations(200)), destination, TilesetSpec(layer="coverage", max_zoom=8)
        )
        assert destination.exists()
        assert destination.read_bytes()[:7] == b"PMTiles"
        assert not list(tmp_path.glob("*.tmp"))
        assert not list(tmp_path.glob("*.mbtiles"))
        assert stats.tiles > 0


@needs_tippecanoe
class TestStats:
    def test_totals_agree_with_the_archive(self, tmp_path: Path) -> None:
        destination = tmp_path / "coverage.mbtiles"
        stats = build_tileset(
            features(observations(200)), destination, TilesetSpec(layer="coverage", max_zoom=8)
        )
        connection = sqlite3.connect(destination)
        tiles, size = connection.execute(
            "select count(*), sum(length(tile_data)) from tiles"
        ).fetchone()
        connection.close()
        assert (stats.tiles, stats.bytes) == (tiles, size)

    def test_the_largest_tile_is_the_largest_of_any_zoom(self, tmp_path: Path) -> None:
        destination = tmp_path / "coverage.mbtiles"
        build_tileset(
            features(observations(200)), destination, TilesetSpec(layer="coverage", max_zoom=8)
        )
        stats = inspect_mbtiles(destination)
        assert stats.max_tile_bytes == max(z.max_tile_bytes for z in stats.by_zoom)
        assert stats.tiles == sum(z.tiles for z in stats.by_zoom)

    def test_zooms_are_reported_in_order(self, tmp_path: Path) -> None:
        destination = tmp_path / "coverage.mbtiles"
        build_tileset(
            features(observations(200)), destination, TilesetSpec(layer="coverage", max_zoom=8)
        )
        stats = inspect_mbtiles(destination)
        assert list(stats.zooms) == sorted(stats.zooms)

    def test_inspecting_does_not_write_to_the_archive(self, tmp_path: Path) -> None:
        # Read-only: the gate runs after the build, and must not be able to
        # change what it is judging.
        destination = tmp_path / "coverage.mbtiles"
        build_tileset(
            features(observations(50)), destination, TilesetSpec(layer="coverage", max_zoom=6)
        )
        before = destination.stat().st_mtime_ns
        inspect_mbtiles(destination)
        assert destination.stat().st_mtime_ns == before

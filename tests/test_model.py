"""The observation record and how it is written out.

What matters here is not that Arrow works, but that the schema keeps the
properties the storage decision rests on: coordinates as float32, dictionary
columns for the values that are constant within a part, and rows sorted by tile
so that delta encoding has something to work with.
"""

from __future__ import annotations

import datetime
from pathlib import Path

import pyarrow.parquet as pq
import pytest

from streetradar.model import CAPTURE_PRECISIONS, ObservationBatch, arrow_schema, capture_ym
from streetradar.store import PartWriter, write_part


def batch(provider: str = "apple", run_id: str = "run-1") -> ObservationBatch:
    return ObservationBatch(provider=provider, run_id=run_id, capture_precision="second")


def ms(year: int, month: int, day: int = 1, hour: int = 12) -> int:
    moment = datetime.datetime(year, month, day, hour, tzinfo=datetime.UTC)
    return int(moment.timestamp() * 1000)


class TestSchema:
    def test_coordinates_are_float32(self) -> None:
        # 3.3 bytes per row against 14.2 for a WKB point, and still sub-metre.
        schema = arrow_schema()
        assert schema.field("lon").type == "float"
        assert schema.field("lat").type == "float"

    def test_constant_columns_are_dictionary_encoded(self) -> None:
        # provider, precision and run_id do not vary within a part, so they must
        # cost essentially nothing while keeping the part self-describing.
        schema = arrow_schema()
        for name in ("provider", "capture_precision", "run_id"):
            assert str(schema.field(name).type).startswith("dictionary")

    def test_capture_time_is_nullable_but_precision_is_not(self) -> None:
        # A provider may not know when; it must always say how well it knows.
        schema = arrow_schema()
        assert schema.field("captured_at").nullable
        assert not schema.field("capture_precision").nullable

    def test_there_is_no_capture_ym_column(self) -> None:
        # Derived from captured_at. A stored copy is a copy that can disagree.
        assert "capture_ym" not in arrow_schema().names

    def test_render_only_fields_are_absent(self) -> None:
        # raw_orientation_json and raw_altitude cost 14.4 bytes per row in the
        # previous schema — 35% of the file — and no stage reads them.
        names = set(arrow_schema().names)
        assert not names & {"raw_orientation_json", "raw_altitude", "geometry", "has_blurs"}


class TestCaptureYm:
    @pytest.mark.parametrize(
        ("year", "month", "expected"), [(2019, 8, 201908), (2024, 12, 202412), (2021, 1, 202101)]
    )
    def test_packs_year_and_month(self, year: int, month: int, expected: int) -> None:
        assert capture_ym(ms(year, month)) == expected

    def test_an_unknown_time_has_no_key(self) -> None:
        assert capture_ym(None) is None

    def test_the_key_orders_like_time(self) -> None:
        assert capture_ym(ms(2019, 12)) < capture_ym(ms(2020, 1))  # type: ignore[operator]


class TestObservationBatch:
    def test_accumulates_and_reports_length(self) -> None:
        b = batch()
        b.add(
            tile_x=1,
            tile_y=2,
            pano_id=[10, 11],
            lon=[5.0, 5.1],
            lat=[49.0, 49.1],
            captured_at=[ms(2021, 6), ms(2021, 6)],
        )
        assert len(b) == 2

    def test_tile_coordinates_are_broadcast(self) -> None:
        b = batch()
        b.add(
            tile_x=7,
            tile_y=8,
            pano_id=[1, 2, 3],
            lon=[1.0, 2.0, 3.0],
            lat=[1.0, 2.0, 3.0],
            captured_at=[None, None, None],
        )
        table = b.to_arrow()
        assert table.column("tile_x").to_pylist() == [7, 7, 7]

    def test_a_column_of_the_wrong_length_is_refused(self) -> None:
        # Silently shifting one column against another is the kind of corruption
        # that stays invisible until someone maps the result.
        b = batch()
        with pytest.raises(ValueError, match="lon has 1 values for 2"):
            b.add(
                tile_x=0,
                tile_y=0,
                pano_id=[1, 2],
                lon=[1.0],
                lat=[1.0, 2.0],
                captured_at=[None, None],
            )

    def test_an_unknown_precision_is_refused(self) -> None:
        with pytest.raises(ValueError, match="unknown capture precision"):
            ObservationBatch(provider="x", run_id="r", capture_precision="epoch")  # type: ignore[arg-type]

    def test_rows_come_out_sorted_by_tile_then_time(self) -> None:
        # The single largest compression lever, and free.
        b = batch()
        b.add(tile_x=5, tile_y=1, pano_id=[3], lon=[1.0], lat=[1.0], captured_at=[ms(2020, 1)])
        b.add(tile_x=1, tile_y=1, pano_id=[2], lon=[1.0], lat=[1.0], captured_at=[ms(2022, 1)])
        b.add(tile_x=1, tile_y=1, pano_id=[1], lon=[1.0], lat=[1.0], captured_at=[ms(2019, 1)])
        table = b.to_arrow()
        assert table.column("tile_x").to_pylist() == [1, 1, 5]
        assert table.column("pano_id").to_pylist() == [1, 2, 3]

    def test_sorting_can_be_turned_off(self) -> None:
        b = batch()
        b.add(tile_x=5, tile_y=0, pano_id=[1], lon=[0.0], lat=[0.0], captured_at=[None])
        b.add(tile_x=1, tile_y=0, pano_id=[2], lon=[0.0], lat=[0.0], captured_at=[None])
        assert b.to_arrow(sort=False).column("tile_x").to_pylist() == [5, 1]

    def test_an_empty_batch_still_matches_the_schema(self) -> None:
        assert batch().to_arrow().schema == arrow_schema()

    def test_clear_empties_it(self) -> None:
        b = batch()
        b.add(tile_x=0, tile_y=0, pano_id=[1], lon=[0.0], lat=[0.0], captured_at=[None])
        b.clear()
        assert len(b) == 0

    def test_optional_columns_default_to_null(self) -> None:
        b = batch()
        b.add(tile_x=0, tile_y=0, pano_id=[1], lon=[0.0], lat=[0.0], captured_at=[None])
        table = b.to_arrow()
        assert table.column("build_id").to_pylist() == [None]
        assert table.column("coverage_type").to_pylist() == [None]


class TestPartWriter:
    def test_writes_a_part_and_reports_its_path(self, tmp_path: Path) -> None:
        writer = PartWriter(tmp_path, provider="apple", run_id="r1", capture_precision="second")
        writer.batch.add(
            tile_x=1,
            tile_y=1,
            pano_id=[1, 2],
            lon=[1.0, 1.0],
            lat=[1.0, 1.0],
            captured_at=[ms(2021, 5), ms(2021, 5)],
        )
        path = writer.flush()
        assert path is not None and path.exists()
        assert pq.ParquetFile(path).metadata.num_rows == 2

    def test_flushing_nothing_writes_nothing(self, tmp_path: Path) -> None:
        writer = PartWriter(tmp_path, provider="apple", run_id="r1", capture_precision="second")
        assert writer.flush() is None
        assert list(tmp_path.iterdir()) == []

    def test_parts_are_numbered_and_immutable(self, tmp_path: Path) -> None:
        writer = PartWriter(tmp_path, provider="apple", run_id="r1", capture_precision="second")
        paths = []
        for i in range(3):
            writer.batch.add(
                tile_x=i, tile_y=0, pano_id=[i], lon=[0.0], lat=[0.0], captured_at=[None]
            )
            paths.append(writer.flush())
        assert len(set(paths)) == 3
        assert writer.parts_written == 3
        assert writer.rows_written == 3

    def test_no_temporary_file_survives_a_write(self, tmp_path: Path) -> None:
        # A part is written to .tmp and renamed, so a crash mid-write leaves
        # something nothing references rather than a truncated part.
        writer = PartWriter(tmp_path, provider="apple", run_id="r1", capture_precision="second")
        writer.batch.add(tile_x=0, tile_y=0, pano_id=[1], lon=[0.0], lat=[0.0], captured_at=[None])
        writer.flush()
        assert not list(tmp_path.glob("*.tmp"))

    def test_pending_reports_what_a_crash_would_lose(self, tmp_path: Path) -> None:
        writer = PartWriter(tmp_path, provider="apple", run_id="r1", capture_precision="second")
        assert writer.pending == 0
        writer.batch.add(
            tile_x=0,
            tile_y=0,
            pano_id=[1, 2],
            lon=[0.0, 0.0],
            lat=[0.0, 0.0],
            captured_at=[None, None],
        )
        assert writer.pending == 2
        writer.flush()
        assert writer.pending == 0

    def test_full_reports_when_a_part_is_due(self, tmp_path: Path) -> None:
        writer = PartWriter(
            tmp_path, provider="apple", run_id="r1", capture_precision="second", rows_per_part=2
        )
        assert not writer.full()
        writer.batch.add(
            tile_x=0,
            tile_y=0,
            pano_id=[1, 2],
            lon=[0.0, 0.0],
            lat=[0.0, 0.0],
            captured_at=[None, None],
        )
        assert writer.full()

    def test_a_zero_size_part_is_refused(self, tmp_path: Path) -> None:
        with pytest.raises(ValueError, match="rows_per_part must be positive"):
            PartWriter(
                tmp_path, provider="a", run_id="r", capture_precision="second", rows_per_part=0
            )

    def test_parts_are_written_with_zstd(self, tmp_path: Path) -> None:
        # snappy is 2.1x larger for CPU savings that do not matter here.
        b = batch()
        b.add(
            tile_x=0,
            tile_y=0,
            pano_id=list(range(1000)),
            lon=[1.0] * 1000,
            lat=[1.0] * 1000,
            captured_at=[ms(2021, 1)] * 1000,
        )
        path = write_part(b.to_arrow(), tmp_path / "p.parquet")
        md = pq.ParquetFile(path).metadata
        assert md.row_group(0).column(0).compression == "ZSTD"


class TestPrecisions:
    def test_every_declared_precision_is_accepted(self) -> None:
        for precision in CAPTURE_PRECISIONS:
            ObservationBatch(provider="x", run_id="r", capture_precision=precision)

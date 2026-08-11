"""The observation record, and a column-oriented accumulator for it.

An observation is one panorama at one place at one time. It is the shape every
point-yielding provider reduces to — Apple, Já, Mapy, Mapillary, Panoramax —
and it is what carries the capture date through to the tiles.

## Why this is columnar rather than a list of objects

At 1,500 tiles a second and 291 panoramas per tile — both measured on Japan —
the writer sees roughly **436,000 rows a second**. Allocating a Python object per
row does not keep up, and neither does converting a list of dataclasses to Arrow
afterwards. Rows are appended straight into per-column lists, which Arrow then
takes as-is.

## Why the schema is this narrow

Measured, column by column, on a 9.6M-row Apple export: the previous engine's
schema cost **41.4 bytes per panorama**, of which `raw_orientation_json` was 12.7
and a WKB point was 14.2. Neither is read by any stage — coverage needs where and
when, not how to render a sphere. Dropping them, storing coordinates as two
`float32`, and sorting by tile before writing brings it to **8.0 bytes**.

The sort is the largest single lever and it is free: panoramas in one tile are
taken seconds apart and their ids run in sequence, so delta encoding does the
rest. `captured_at` falls 5.4 → 3.4 bytes and `pano_id` 7.4 → 2.4.
"""

from __future__ import annotations

from collections.abc import Sequence
from typing import TYPE_CHECKING, Final, Literal

if TYPE_CHECKING:
    import pyarrow as pa

__all__ = [
    "CAPTURE_PRECISIONS",
    "CapturePrecision",
    "ObservationBatch",
    "arrow_schema",
    "capture_ym",
]

#: How much of a capture timestamp the provider actually knows. Stored alongside
#: the value so the front end cannot over-claim: Apple reports a full UTC
#: timestamp, Já only a year and a month.
CapturePrecision = Literal["second", "day", "month", "year", "unknown"]

CAPTURE_PRECISIONS: Final[tuple[CapturePrecision, ...]] = (
    "second",
    "day",
    "month",
    "year",
    "unknown",
)


def arrow_schema() -> pa.Schema:
    """The observation schema.

    `provider`, `capture_precision` and `run_id` are dictionary-encoded: they are
    constant within a part, so they cost essentially nothing while keeping each
    part self-describing.

    Coordinates are `float32`. At these latitudes that is sub-metre, far below a
    capture vehicle's own GPS error, and it is 3.3 bytes per row against 14.2 for
    a WKB point.
    """
    import pyarrow as pa

    return pa.schema(
        [
            pa.field("provider", pa.dictionary(pa.int8(), pa.string()), nullable=False),
            pa.field("pano_id", pa.uint64(), nullable=False),
            # Provenance, and the sort key that makes everything else compress.
            pa.field("tile_x", pa.int32(), nullable=False),
            pa.field("tile_y", pa.int32(), nullable=False),
            pa.field("lon", pa.float32(), nullable=False),
            pa.field("lat", pa.float32(), nullable=False),
            pa.field("captured_at", pa.timestamp("ms", tz="UTC"), nullable=True),
            pa.field("capture_precision", pa.dictionary(pa.int8(), pa.string()), nullable=False),
            pa.field("coverage_type", pa.dictionary(pa.int8(), pa.string()), nullable=True),
            #: Apple's revision number. Null for providers without one.
            pa.field("build_id", pa.uint64(), nullable=True),
            pa.field("run_id", pa.dictionary(pa.int8(), pa.string()), nullable=False),
        ]
    )


def capture_ym(timestamp_ms: int | None) -> int | None:
    """`YYYYMM` from a millisecond UTC timestamp — the animation key.

    Derived rather than stored: it is a pure function of `captured_at`, and a
    column that repeats information is a column that can disagree with it.
    """
    if timestamp_ms is None:
        return None
    import datetime

    moment = datetime.datetime.fromtimestamp(timestamp_ms / 1000.0, datetime.UTC)
    return moment.year * 100 + moment.month


def _nullable_dictionary(values: Sequence[str | None]) -> pa.Array:
    """Dictionary-encode a column that may contain nulls.

    `pa.array(...).dictionary_encode()` puts the null *into the dictionary*, and
    parquet refuses to write that — `ArrowNotImplementedError: Writing
    DictionaryArray with null encoded in dictionary type`. Nulls belong in the
    indices instead, which is what this builds.

    It bites whenever a provider has no coverage type at all, so it is on the
    ordinary path rather than an edge case.
    """
    import pyarrow as pa

    seen: dict[str, int] = {}
    indices: list[int | None] = []
    for value in values:
        if value is None:
            indices.append(None)
            continue
        if value not in seen:
            seen[value] = len(seen)
        indices.append(seen[value])
    return pa.DictionaryArray.from_arrays(
        pa.array(indices, type=pa.int8()),
        pa.array(list(seen), type=pa.string()),
    )


class ObservationBatch:
    """Rows accumulated column by column, ready to hand to Arrow.

    Not a list of records: see the module docstring. `add` takes whole columns
    for one tile, which is how the decoders naturally produce them.
    """

    __slots__ = (
        "_build_id",
        "_captured_at",
        "_coverage_type",
        "_lat",
        "_lon",
        "_pano_id",
        "_tile_x",
        "_tile_y",
        "capture_precision",
        "provider",
        "run_id",
    )

    def __init__(
        self,
        provider: str,
        run_id: str,
        capture_precision: CapturePrecision,
    ) -> None:
        if capture_precision not in CAPTURE_PRECISIONS:
            raise ValueError(
                f"unknown capture precision {capture_precision!r}; "
                f"expected one of {CAPTURE_PRECISIONS}"
            )
        self.provider = provider
        self.run_id = run_id
        self.capture_precision: CapturePrecision = capture_precision
        self._pano_id: list[int] = []
        self._tile_x: list[int] = []
        self._tile_y: list[int] = []
        self._lon: list[float] = []
        self._lat: list[float] = []
        self._captured_at: list[int | None] = []
        self._coverage_type: list[str | None] = []
        self._build_id: list[int | None] = []

    def __len__(self) -> int:
        return len(self._pano_id)

    def add(
        self,
        *,
        tile_x: int,
        tile_y: int,
        pano_id: Sequence[int],
        lon: Sequence[float],
        lat: Sequence[float],
        captured_at: Sequence[int | None],
        coverage_type: Sequence[str | None] | None = None,
        build_id: Sequence[int | None] | None = None,
    ) -> None:
        """Append one tile's panoramas.

        Every sequence must be the same length; a mismatch here would silently
        shift one column against another, which is the kind of corruption that is
        invisible until someone maps it.
        """
        count = len(pano_id)
        for name, column in (("lon", lon), ("lat", lat), ("captured_at", captured_at)):
            if len(column) != count:
                raise ValueError(f"{name} has {len(column)} values for {count} panoramas")
        if coverage_type is not None and len(coverage_type) != count:
            raise ValueError(f"coverage_type has {len(coverage_type)} values for {count}")
        if build_id is not None and len(build_id) != count:
            raise ValueError(f"build_id has {len(build_id)} values for {count}")

        self._pano_id.extend(pano_id)
        self._tile_x.extend([tile_x] * count)
        self._tile_y.extend([tile_y] * count)
        self._lon.extend(lon)
        self._lat.extend(lat)
        self._captured_at.extend(captured_at)
        self._coverage_type.extend(coverage_type if coverage_type is not None else [None] * count)
        self._build_id.extend(build_id if build_id is not None else [None] * count)

    def clear(self) -> None:
        self._pano_id.clear()
        self._tile_x.clear()
        self._tile_y.clear()
        self._lon.clear()
        self._lat.clear()
        self._captured_at.clear()
        self._coverage_type.clear()
        self._build_id.clear()

    def to_arrow(self, *, sort: bool = True) -> pa.Table:
        """Build an Arrow table, sorted by tile then capture time by default.

        Sorting here rather than at write time because it is what makes the
        columns compress — and because the tiling stage wants tile-major order
        anyway, so it is not paid twice.
        """
        import pyarrow as pa

        count = len(self)
        table = pa.Table.from_arrays(
            [
                pa.DictionaryArray.from_arrays(
                    pa.array([0] * count, type=pa.int8()), pa.array([self.provider])
                ),
                pa.array(self._pano_id, type=pa.uint64()),
                pa.array(self._tile_x, type=pa.int32()),
                pa.array(self._tile_y, type=pa.int32()),
                pa.array(self._lon, type=pa.float32()),
                pa.array(self._lat, type=pa.float32()),
                pa.array(self._captured_at, type=pa.timestamp("ms", tz="UTC")),
                pa.DictionaryArray.from_arrays(
                    pa.array([0] * count, type=pa.int8()),
                    pa.array([self.capture_precision]),
                ),
                _nullable_dictionary(self._coverage_type),
                pa.array(self._build_id, type=pa.uint64()),
                pa.DictionaryArray.from_arrays(
                    pa.array([0] * count, type=pa.int8()), pa.array([self.run_id])
                ),
            ],
            schema=arrow_schema(),
        )
        if sort and count:
            table = table.sort_by(
                [("tile_x", "ascending"), ("tile_y", "ascending"), ("captured_at", "ascending")]
            )
        return table

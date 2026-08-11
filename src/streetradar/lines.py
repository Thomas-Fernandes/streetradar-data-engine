"""Coverage that arrives as road geometry rather than panorama positions.

Já and Naver publish a driveline layer: where the camera car went, as polylines.
Apple publishes where each panorama stands. Both answer "is there coverage here",
so both end up in tiles — but they do not compress the same way and they do not
carry the same information, so they do not share a schema.

## Lines stay lines

Reducing a polyline to its vertices would cost more, not less: at 4.3 vertices
per line it multiplies the row count while throwing away the connectivity that
makes a road render as a road. And a line has no panorama identity to carry —
there is no `pano_id` here, and no way to tell a re-drive from the original.

## Coordinates are the tile's own, not WGS84

Measured on 91,976 clipped lines from real Naver tiles:

    WKB float64   (previous engine)   40.85 bytes/line   9.43 per vertex
    WGS84 float32                     32.05              7.40
    tile-local int16                  16.20              3.74

**2.52x the previous engine's schema**, for exactly the reason Apple's offsets
are stored rather than its coordinates: a tile-local value is a small integer
whose neighbours are close by, and a longitude is a float whose leading digits
never change and whose low bits never repeat. `tiles.offset_to_wgs84` recovers
the WGS84 exactly, so nothing is given up.

Rounding to whole tile units is not free but it is small: reconstructing
398,407 real vertices through parquet and back to WGS84 deviates by **11.1 cm
at worst**, one tile unit being 6.1 cm at Korean latitudes. That is inside the
quantisation the provider already applied when it built the tile, and two
orders of magnitude inside a capture vehicle's own GPS error.

## These providers publish no capture date

`ResolvedSource.has_temporal_field` is false for both Já and Naver: the driveline
layers carry road classification (`road_k`, `dp_level`) and nothing temporal. So
`captured_at` is null here and `capture_precision` is `"unknown"`, and the time
cursor that works over Apple's coverage does not apply to theirs. The columns
exist anyway because a provider that starts publishing dates should not need a
schema change, and because a null that is *declared* unknown is honest where an
absent column would be silently assumed present later.
"""

from __future__ import annotations

from collections.abc import Sequence
from typing import TYPE_CHECKING

from .decode import decode_tile
from .model import CAPTURE_PRECISIONS, CapturePrecision
from .tiles import TileFetcher, TileSource, harvest_pyramid

if TYPE_CHECKING:
    from collections.abc import AsyncIterator
    from pathlib import Path

    import pyarrow as pa

    from .store import PartWriter

__all__ = ["LineBatch", "collect_lines", "line_schema"]


def line_schema() -> pa.Schema:
    """The coverage-line schema.

    `zoom` and `extent` are columns rather than metadata because they are needed
    to read a coordinate back: without them `x` and `y` are integers with no
    position. They are constant within a part and dictionary-encoded, so they
    cost nothing to carry.
    """
    import pyarrow as pa

    return pa.schema(
        [
            pa.field("provider", pa.dictionary(pa.int8(), pa.string()), nullable=False),
            pa.field("zoom", pa.dictionary(pa.int8(), pa.int8()), nullable=False),
            pa.field("tile_x", pa.int32(), nullable=False),
            pa.field("tile_y", pa.int32(), nullable=False),
            pa.field("extent", pa.dictionary(pa.int8(), pa.int32()), nullable=False),
            # Tile-local, clipped to the tile square. See the module docstring.
            pa.field("x", pa.list_(pa.int16()), nullable=False),
            pa.field("y", pa.list_(pa.int16()), nullable=False),
            pa.field("captured_at", pa.timestamp("ms", tz="UTC"), nullable=True),
            pa.field("capture_precision", pa.dictionary(pa.int8(), pa.string()), nullable=False),
            pa.field("run_id", pa.dictionary(pa.int8(), pa.string()), nullable=False),
        ]
    )


class LineBatch:
    """Lines accumulated column by column, ready to hand to Arrow.

    The same shape as `ObservationBatch` and for the same reason: `add` takes one
    tile's worth at a time, which is how the decoder produces them.
    """

    __slots__ = (
        "_captured_at",
        "_tile_x",
        "_tile_y",
        "_x",
        "_y",
        "capture_precision",
        "extent",
        "provider",
        "run_id",
        "zoom",
    )

    def __init__(
        self,
        provider: str,
        run_id: str,
        zoom: int,
        extent: int,
        capture_precision: CapturePrecision = "unknown",
    ) -> None:
        if capture_precision not in CAPTURE_PRECISIONS:
            raise ValueError(
                f"unknown capture precision {capture_precision!r}; "
                f"expected one of {CAPTURE_PRECISIONS}"
            )
        if not 0 <= zoom <= 24:
            raise ValueError(f"zoom {zoom} is not a usable tile zoom")
        if extent < 1:
            raise ValueError("extent must be positive")
        self.provider = provider
        self.run_id = run_id
        self.zoom = zoom
        self.extent = extent
        self.capture_precision: CapturePrecision = capture_precision
        self._tile_x: list[int] = []
        self._tile_y: list[int] = []
        self._x: list[list[int]] = []
        self._y: list[list[int]] = []
        self._captured_at: list[int | None] = []

    def __len__(self) -> int:
        return len(self._tile_x)

    @property
    def vertices(self) -> int:
        return sum(len(run) for run in self._x)

    def add(
        self,
        *,
        tile_x: int,
        tile_y: int,
        lines: Sequence[Sequence[tuple[float, float]]],
        captured_at: Sequence[int | None] | None = None,
    ) -> None:
        """Append one tile's polylines, in the tile's own coordinates.

        Coordinates are rounded here rather than by the caller, so that every
        writer quantises identically — two parts of the same run disagreeing
        about rounding would put the same road in two places.

        A line of fewer than two vertices is dropped: it is not geometry, and it
        would render as nothing while still costing a row.
        """
        if captured_at is not None and len(captured_at) != len(lines):
            raise ValueError(f"captured_at has {len(captured_at)} values for {len(lines)} lines")

        for index, line in enumerate(lines):
            if len(line) < 2:
                continue
            self._tile_x.append(tile_x)
            self._tile_y.append(tile_y)
            self._x.append([round(vertex[0]) for vertex in line])
            self._y.append([round(vertex[1]) for vertex in line])
            self._captured_at.append(None if captured_at is None else captured_at[index])

    def clear(self) -> None:
        self._tile_x.clear()
        self._tile_y.clear()
        self._x.clear()
        self._y.clear()
        self._captured_at.clear()

    def to_arrow(self, *, sort: bool = True) -> pa.Table:
        """Build an Arrow table, sorted by tile by default.

        Sorted for the same reason observations are: neighbouring tiles hold
        neighbouring coordinates, and that is what the integer columns compress
        against.
        """
        import pyarrow as pa

        count = len(self)

        def constant(value: object, value_type: pa.DataType) -> pa.Array:
            return pa.DictionaryArray.from_arrays(
                pa.array([0] * count, type=pa.int8()), pa.array([value], type=value_type)
            )

        table = pa.Table.from_arrays(
            [
                constant(self.provider, pa.string()),
                constant(self.zoom, pa.int8()),
                pa.array(self._tile_x, type=pa.int32()),
                pa.array(self._tile_y, type=pa.int32()),
                constant(self.extent, pa.int32()),
                pa.array(self._x, type=pa.list_(pa.int16())),
                pa.array(self._y, type=pa.list_(pa.int16())),
                pa.array(self._captured_at, type=pa.timestamp("ms", tz="UTC")),
                constant(self.capture_precision, pa.string()),
                constant(self.run_id, pa.string()),
            ],
            schema=line_schema(),
        )
        if sort and count:
            table = table.sort_by([("tile_x", "ascending"), ("tile_y", "ascending")])
        return table


async def collect_lines(
    fetcher: TileFetcher,
    source: TileSource,
    *,
    layer: str,
    target_zoom: int,
    writer: PartWriter[LineBatch],
    start_zoom: int | None = None,
    limit_per_level: int | None = None,
) -> AsyncIterator[Path]:
    """Descend the pyramid, writing the geometry found. Yields each finished part.

    The same generator shape as Apple's collector, so the ordering rule reads the
    same way — upload the part, then record that the work is done:

        async for part in collect_lines(..., writer=writer):
            upload(part)
            part.unlink()

    `harvest_pyramid` yields every level it visits, not only the deepest: the
    shallow ones are the mask that pruned the descent, and a caller may want to
    count what they cost. Only `target_zoom` carries geometry, so only it is
    written — storing the coarse levels would double-count every road at the
    provider's own generalisation.

    There is no resume state here. A descent is re-derivable from the source
    alone, and re-running one costs the pruned levels again rather than the whole
    country, which is why Já and Naver need nothing like `FloodState`.
    """
    if writer.batch.zoom != target_zoom:
        raise ValueError(
            f"writer records zoom {writer.batch.zoom} but the descent targets {target_zoom}"
        )

    async for result in harvest_pyramid(
        fetcher,
        source,
        target_zoom=target_zoom,
        start_zoom=start_zoom,
        limit_per_level=limit_per_level,
    ):
        if result.tile.z != target_zoom or not result.body:
            continue
        decoded = decode_tile(result.body, result.tile, layer=layer, project=False)
        if not decoded.lines:
            continue
        # An extent that changed under us would misplace every vertex by a
        # factor, silently and everywhere.
        if decoded.extent != writer.batch.extent:
            raise ValueError(
                f"tile {result.tile} has extent {decoded.extent}, "
                f"but the part records {writer.batch.extent}"
            )
        writer.batch.add(
            tile_x=result.tile.x,
            tile_y=result.tile.y,
            lines=[line.coords for line in decoded.lines],
        )
        if writer.full():
            part = writer.flush()
            if part is not None:
                yield part

    part = writer.flush()
    if part is not None:
        yield part

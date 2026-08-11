"""Turning observations into a vector tile archive.

This is the last stage before publication: parquet parts in, one tileset out.
The work itself is `tippecanoe`'s — what lives here is the feature stream that
feeds it, the invocation, and the statistics the publish gate reads back.

Distinct from `streetradar.tiles`, which is the tile *grid* and how coverage is
harvested from it. This module is about the archive that gets served.

## What the shape of the archive was chosen from

Measured on a real Apple export of Greece — 14,370,347 dated panoramas:

    total            91,809 tiles     115.5 MB      8.0 bytes/panorama
    median tile         0.5 KB
    p95 tile        under 5 KB at every zoom
    largest tile       88.5 KB

The largest tile is 88.5 KB although the run was made with
`--no-tile-size-limit` specifically to see how far it would climb. There was
nothing to contain, and three decisions follow from that.

**One archive, with the date as an attribute.** A tile carries a *median of one*
distinct capture month, and eleven at the very worst — 42 months span the Apple
data as a whole, but coverage is driven in campaigns, so a road is captured once
and occasionally twice. The time cursor is therefore a filter on `ym` over a
single tileset, not one tileset per month and not a packed year bitmask.

**Zoom 16 is the maximum.** Greece produced 15,566,656 features at z16 for
14,370,347 inputs: nothing was dropped, and the 8.3% surplus is the edge buffer
duplicating features across tile borders, which is what lets a renderer draw a
symbol that straddles two tiles. z17 would quadruple the tile count to carry
information already present.

**Density in does not decide size out.** The worst Greek z12 tile holds 775,810
panoramas and comes out at 83.5 KB, because `--drop-densest-as-needed` bounds the
tile by construction and spends the input on thinning instead. Japan is denser
overall, but its worst z16 tile holds 14,716 panoramas against Greece's 18,090 —
the extreme case is not the large country.

## `tippecanoe` is an external binary

It is not a Python dependency and cannot be one. `build_tileset` checks for it
and fails with a message naming what is missing, rather than letting `subprocess`
raise a bare `FileNotFoundError` from inside a long pipeline.
"""

from __future__ import annotations

import shutil
import sqlite3
import subprocess
from collections.abc import Iterable, Iterator
from dataclasses import dataclass
from pathlib import Path
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    import pyarrow as pa

__all__ = [
    "TilesetSpec",
    "TilesetStats",
    "ToolMissing",
    "ZoomStats",
    "build_tileset",
    "inspect_mbtiles",
    "observation_features",
    "read_parts",
]

_TIPPECANOE = "tippecanoe"
_PMTILES = "pmtiles"

#: Coordinates are written to six decimals — about 11 cm. Not false precision:
#: at zoom 16 a tile is 4096 units across, one unit being roughly 0.15 m, so
#: fewer decimals would quantise before tippecanoe does.
_COORD_FORMAT = '{{"type":"Feature","properties":{{{p}}},"geometry":{{"type":"Point","coordinates":[{x:.6f},{y:.6f}]}}}}\n'


class ToolMissing(RuntimeError):
    """An external binary the tiling stage needs is not on PATH."""


@dataclass(frozen=True)
class TilesetSpec:
    """How one tileset is cut.

    The defaults are the measured ones; see the module docstring. They are
    defaults rather than constants because Já and Naver yield road lines rather
    than panorama points, and lines survive low zooms differently.
    """

    layer: str
    min_zoom: int = 0
    max_zoom: int = 16

    def __post_init__(self) -> None:
        if not self.layer:
            raise ValueError("a tileset needs a layer name")
        if not 0 <= self.min_zoom <= self.max_zoom <= 24:
            raise ValueError(f"zoom range {self.min_zoom}..{self.max_zoom} is not usable")

    def argv(self, destination: Path) -> list[str]:
        """The tippecanoe command line, as a list — never a shell string."""
        return [
            _TIPPECANOE,
            f"--output={destination}",
            "--force",
            f"--layer={self.layer}",
            f"--minimum-zoom={self.min_zoom}",
            f"--maximum-zoom={self.max_zoom}",
            # Thin the dense tiles rather than truncate them. Chosen over
            # --drop-fraction-as-needed, which drops uniformly and so erases
            # sparse rural coverage at the same rate as redundant urban coverage.
            "--drop-densest-as-needed",
            "--read-parallel",
            "--quiet",
            # Deliberately *not* --extend-zooms-if-still-dropping: it silently
            # pushes past max_zoom on a dense country, so two countries would
            # publish archives of different depth without anyone choosing that.
        ]


@dataclass(frozen=True)
class ZoomStats:
    zoom: int
    tiles: int
    bytes: int
    max_tile_bytes: int


@dataclass(frozen=True)
class TilesetStats:
    """What the publish gate reads back before anything is uploaded."""

    tiles: int
    bytes: int
    by_zoom: tuple[ZoomStats, ...]

    @property
    def max_tile_bytes(self) -> int:
        return max((z.max_tile_bytes for z in self.by_zoom), default=0)

    @property
    def zooms(self) -> tuple[int, ...]:
        return tuple(z.zoom for z in self.by_zoom)


def read_parts(directory: Path) -> Iterator[pa.RecordBatch]:
    """Stream the parquet parts of a run, oldest first.

    Batched rather than concatenated: a country's parts do not fit in the VPS's
    6 GB, and nothing downstream needs them to.
    """
    import pyarrow.parquet as pq

    for path in sorted(directory.glob("*.parquet")):
        yield from pq.ParquetFile(path).iter_batches(
            batch_size=1_000_000, columns=["lon", "lat", "captured_at"]
        )


def observation_features(batches: Iterable[pa.RecordBatch]) -> Iterator[bytes]:
    """Observations as line-delimited GeoJSON, one blob per batch.

    `ym` is `YYYYMM`, and it is simply absent when the provider did not report a
    date — so a renderer filtering on it excludes undated coverage rather than
    lumping it into a sentinel month that would look like real data.
    """
    import pyarrow.compute as pc

    for batch in batches:
        if not batch.num_rows:
            continue
        timestamps = batch.column("captured_at")
        year_months = pc.add(
            pc.multiply(pc.year(timestamps).cast("int32"), 100),
            pc.month(timestamps).cast("int32"),
        ).to_pylist()
        lons = batch.column("lon").to_pylist()
        lats = batch.column("lat").to_pylist()

        yield "".join(
            _COORD_FORMAT.format(p="" if ym is None else f'"ym":{ym}', x=lon, y=lat)
            for lon, lat, ym in zip(lons, lats, year_months, strict=True)
        ).encode()


def build_tileset(
    features: Iterable[bytes],
    destination: Path,
    spec: TilesetSpec,
) -> TilesetStats:
    """Build one tileset from a feature stream, and report what came out.

    `destination` decides the format by suffix: `.pmtiles` is built as mbtiles
    and converted, which is the form that gets published — one object served
    from R2 by range request, against 397,596 objects for a directory of tiles.

    Written to a temporary name and renamed, like a parquet part: a tileset that
    was being built when the process died is something nothing references, not a
    truncated archive that the publish gate might accept.
    """
    to_pmtiles = destination.suffix == ".pmtiles"
    for tool in [_TIPPECANOE, *([_PMTILES] if to_pmtiles else [])]:
        if shutil.which(tool) is None:
            raise ToolMissing(f"{tool} is not on PATH; the tiling stage cannot run without it")

    destination.parent.mkdir(parents=True, exist_ok=True)
    mbtiles = destination.with_suffix(".mbtiles.tmp")

    process = subprocess.Popen(spec.argv(mbtiles), stdin=subprocess.PIPE)
    assert process.stdin is not None
    try:
        with process.stdin as sink:
            for blob in features:
                sink.write(blob)
    except BrokenPipeError:  # tippecanoe died; its own status is the real error
        pass
    if process.wait() != 0:
        mbtiles.unlink(missing_ok=True)
        raise RuntimeError(f"{_TIPPECANOE} exited with status {process.returncode}")

    stats = inspect_mbtiles(mbtiles)

    if to_pmtiles:
        converted = destination.with_suffix(".pmtiles.tmp")
        result = subprocess.run(
            [_PMTILES, "convert", str(mbtiles), str(converted)], capture_output=True
        )
        mbtiles.unlink(missing_ok=True)
        if result.returncode != 0:
            converted.unlink(missing_ok=True)
            raise RuntimeError(f"{_PMTILES} convert failed: {result.stderr.decode().strip()}")
        converted.replace(destination)
    else:
        mbtiles.replace(destination)

    return stats


def inspect_mbtiles(path: Path) -> TilesetStats:
    """Tile counts and sizes per zoom, read straight out of the archive.

    Taken from what tippecanoe actually wrote rather than from what it reported,
    because the gate's whole purpose is to distrust the step before it.
    """
    connection = sqlite3.connect(f"file:{path}?mode=ro", uri=True)
    try:
        rows = connection.execute(
            "select zoom_level, count(*), sum(length(tile_data)), max(length(tile_data)) "
            "from tiles group by zoom_level order by zoom_level"
        ).fetchall()
    finally:
        connection.close()

    by_zoom = tuple(
        ZoomStats(zoom=z, tiles=count, bytes=total or 0, max_tile_bytes=largest or 0)
        for z, count, total, largest in rows
    )
    return TilesetStats(
        tiles=sum(z.tiles for z in by_zoom),
        bytes=sum(z.bytes for z in by_zoom),
        by_zoom=by_zoom,
    )

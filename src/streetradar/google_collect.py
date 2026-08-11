"""Sweep one country's Google Street View geometry by flooding z17 tiles.

This is the geometry pass: it finds every current panorama and where it is, with
no dates — dates are a separate, more expensive pass (a tile carries none). It is
the Apple/Mapy pattern again: a resumable flood over a tile grid, clipped to the
country so it does not run off into neighbours.

## Why the flood is clipped

Coverage is contiguous along roads and roads cross borders, so an unclipped flood
from a seed in Luxembourg flows straight into Belgium, France and Germany — every
one covered — and never stops. Measured: it did exactly that. So the frontier is
bounded to the country's tile box; a polygon would be tighter, but the box plus a
`DISTINCT`/point-in-country filter at the storage stage is enough and far simpler.

## Seeding

The flood only finds what is connected to a seed, and a country's coverage is not
one component (towns, islands, isolated roads). So it is seeded from a coarse grid
across the box — one probe every `SEED_STEP` tiles — and every covered seed starts
a flood. A component smaller than the seed spacing between grid points can be
missed; the coverage raster (`google.has_coverage`) is the finer detector for a
world run, and is deliberately left to the orchestration layer rather than baked
in here, so a per-country run stays a plain flood.

## Points now, dates and lines later

Panoramas are written as observations with no date (`captured_at` null,
`unknown` precision), the same store-and-tile path as the other providers. The
dates pass fills them in; the road graph (`google.links_to_edges`) turns them into
lines at the tiling stage. Both are downstream of this.
"""

from __future__ import annotations

import math
from dataclasses import dataclass
from typing import TYPE_CHECKING

from .flood import ProbedGrid
from .google import TILE_ZOOM, getpanos
from .model import CapturePrecision
from .tiles import Tile, TileSource

if TYPE_CHECKING:
    from collections.abc import AsyncIterator
    from pathlib import Path

    from .google import Getter, Panorama
    from .model import ObservationBatch
    from .store import PartWriter
    from .tiles import Bounds

__all__ = ["GEOMETRY_PRECISION", "SEED_STEP", "GoogleState", "collect_geometry"]

#: Panoramas from a tile carry no date, so the geometry pass records "unknown"
#: precision. The dates pass supersedes this with month (or second) precision.
GEOMETRY_PRECISION: CapturePrecision = "unknown"

#: z17 tiles between seed-grid points. 16 is ~1.2 km at mid-latitudes: fine
#: enough to land a seed in any ordinary town, coarse enough that seeding a
#: country is a few hundred probes.
SEED_STEP = 16

_MAGIC = b"SRGL"
_VERSION = 1


def _tile_of(lon: float, lat: float, zoom: int) -> tuple[int, int]:
    span = 1 << zoom
    x = int((lon + 180.0) / 360.0 * span)
    y = int((1.0 - math.asinh(math.tan(math.radians(lat))) / math.pi) / 2.0 * span)
    return max(0, min(span - 1, x)), max(0, min(span - 1, y))


@dataclass
class GoogleState:
    """Which z17 tiles have been probed, plus the tally, for resuming a run."""

    grid: ProbedGrid
    zoom: int = TILE_ZOOM
    panoramas: int = 0
    covered: int = 0

    @classmethod
    def for_bounds(cls, bounds: Bounds, zoom: int = TILE_ZOOM) -> GoogleState:
        source = TileSource(
            id="google", url_template="", min_zoom=zoom, max_zoom=zoom, bounds=bounds
        )
        return cls(grid=ProbedGrid.for_bounds(bounds, source, zoom), zoom=zoom)

    @property
    def probed(self) -> int:
        return len(self.grid)

    def save(self, path: Path) -> None:
        import json
        import struct

        header = json.dumps(
            {
                "x0": self.grid.x0,
                "y0": self.grid.y0,
                "width": self.grid.width,
                "height": self.grid.height,
                "zoom": self.zoom,
                "panoramas": self.panoramas,
                "covered": self.covered,
                "probed": len(self.grid),
            }
        ).encode()
        tmp = path.with_suffix(path.suffix + ".tmp")
        with open(tmp, "wb") as handle:
            handle.write(_MAGIC)
            handle.write(struct.pack("<BI", _VERSION, len(header)))
            handle.write(header)
            handle.write(self.grid._bits)  # noqa: SLF001 — same package
        tmp.replace(path)

    @classmethod
    def load(cls, path: Path) -> GoogleState:
        import json
        import struct

        with open(path, "rb") as handle:
            if handle.read(4) != _MAGIC:
                raise ValueError(f"{path} is not a Google sweep snapshot")
            version, header_len = struct.unpack("<BI", handle.read(5))
            if version != _VERSION:
                raise ValueError(f"{path}: unsupported snapshot version {version}")
            header = json.loads(handle.read(header_len))
            grid = ProbedGrid(header["x0"], header["y0"], header["width"], header["height"])
            grid._bits[:] = handle.read(len(grid._bits))  # noqa: SLF001
            grid._count = header["probed"]  # noqa: SLF001
        return cls(
            grid=grid,
            zoom=header["zoom"],
            panoramas=header["panoramas"],
            covered=header["covered"],
        )


def _record(
    batch: ObservationBatch, panos: list[Panorama], probe: tuple[int, int], zoom: int
) -> None:
    """Add the panoramas that belong to the probed tile, undated.

    A tile's response includes panoramas whose position lies just over its edge —
    the same panorama therefore comes back from several adjacent tiles. Keeping
    only those whose position tile *is* the probed tile assigns each panorama to
    exactly one tile, so every panorama is written once, by its own tile, with no
    global de-duplication structure. A panorama's own tile is always probed (it is
    covered — it contains that panorama), so nothing is lost.
    """
    own = [p for p in panos if _tile_of(p.lon, p.lat, zoom) == probe]
    if not own:
        return
    batch.add(
        tile_x=probe[0],
        tile_y=probe[1],
        pano_id=[_pano_key(p.id) for p in own],
        lon=[p.lon for p in own],
        lat=[p.lat for p in own],
        captured_at=[None] * len(own),
    )


def _pano_key(pano_id: str) -> int:
    """A stable uint64 from Google's base64 pano id, for the numeric id column.

    The full string id is what the dates pass needs, and it is preserved
    elsewhere; here the observation store's `pano_id` is numeric, so a stable hash
    of the string fills it. Collisions across 2^64 are negligible at this scale.
    """
    return int.from_bytes(pano_id.encode()[:8].ljust(8, b"\0"), "big")


async def collect_geometry(
    fetcher: Getter,
    bounds: Bounds,
    *,
    writer: PartWriter[ObservationBatch],
    state: GoogleState,
    seed_step: int = SEED_STEP,
    limit: int | None = None,
) -> AsyncIterator[Path]:
    """Flood the country's z17 tiles, writing panoramas, yielding each part.

    The generator shape is the ordering rule the other collectors share — upload
    the part, then snapshot. Nothing here saves `state`. `limit` caps the number
    of tiles probed, which bounds a first run.
    """
    import asyncio
    from collections import deque

    if writer.batch.capture_precision != GEOMETRY_PRECISION:
        raise ValueError(
            f"writer records {writer.batch.capture_precision!r}; the geometry pass "
            f"has no dates, so it must be {GEOMETRY_PRECISION!r}"
        )

    source = TileSource(
        id="google", url_template="", min_zoom=state.zoom, max_zoom=state.zoom, bounds=bounds
    )
    x0, y0, x1, y1 = source.tile_range(state.zoom)

    def inside(x: int, y: int) -> bool:
        return x0 <= x < x1 and y0 <= y < y1

    # `queued` holds every tile put on the frontier this run, so a tile reached
    # from several covered neighbours is enqueued — and probed — exactly once.
    # Without it, duplicates in the frontier would each be probed and written.
    queued: set[tuple[int, int]] = set()
    frontier: deque[tuple[int, int]] = deque()

    def enqueue(x: int, y: int) -> None:
        if (x, y) not in queued and Tile(state.zoom, x, y) not in state.grid:
            queued.add((x, y))
            frontier.append((x, y))

    # Seed grid: one tile every seed_step, offset to the grid so a resumed run
    # samples the same points.
    for sx in range(x0 + (-x0) % seed_step, x1, seed_step):
        for sy in range(y0 + (-y0) % seed_step, y1, seed_step):
            enqueue(sx, sy)

    probed_this_run = 0
    while frontier:
        if limit is not None and probed_this_run >= limit:
            break
        batch: list[tuple[int, int]] = []
        while frontier and len(batch) < 64:
            batch.append(frontier.popleft())

        results = await asyncio.gather(*(getpanos(fetcher, x, y) for x, y in batch))
        probed_this_run += len(batch)
        for (x, y), panos in zip(batch, results, strict=True):
            state.grid.add(Tile(state.zoom, x, y))
            if panos:
                state.covered += 1
                _record(writer.batch, panos, (x, y), state.zoom)
                state.panoramas = writer.rows_written + writer.pending
                for dx, dy in ((-1, 0), (1, 0), (0, -1), (0, 1)):
                    nx, ny = x + dx, y + dy
                    if inside(nx, ny):
                        enqueue(nx, ny)

        if writer.full():
            part = writer.flush()
            if part is not None:
                yield part

    remainder = writer.flush()
    if remainder is not None:
        yield remainder

"""Sweep Streetside to completion, resumably, and write what it finds.

This drives `bing_sweep` over a country as a grid of root cells, each swept and
marked done, so a run that dies at hour six resumes at the root cell it was on
rather than from zero — the failure mode that stopped the previous ad-hoc
collectors.

## Root cells, and why this size

The world is swept in cells at `ROOT_ZOOM`. Small enough that one cell's bubbles
and its de-duplication set fit in memory; large enough that empty land — most of
the planet — is dismissed in one probe per cell rather than many. A cell is the
unit a `ProbedGrid` records, exactly as for Apple and Mapy.

## Points, not lines — for now

Bubbles are written as dated observations, the shape Apple and Mapy already use,
so the whole store-and-tile path downstream is unchanged. The drive geometry is
not lost: `bing.chains` reconstructs the polylines from the `prev`/`next` links,
and doing so is a tiling-stage transform rather than a collection concern. Storing
the lines directly — clipped to tiles, carrying each drive's date — is the natural
next step and needs the links preserved, which is a schema decision to make when
the line tiler is built.

## De-duplication

Within one root cell a bubble is returned by many overlapping probes, so the
sweep de-duplicates by id as it writes. Across cell seams a bubble near a boundary
appears in both cells; that residue is small — only bubbles within a certified
radius of a seam — and is removed by a `DISTINCT id` pass before tiling rather
than by holding a world-sized id set in memory.
"""

from __future__ import annotations

import math
from dataclasses import dataclass
from typing import TYPE_CHECKING

from .bing import CAPTURE_PRECISION, MAX_COUNT, getbubbles
from .bing_sweep import Cell, sweep
from .flood import ProbedGrid
from .tiles import Tile, TileSource

if TYPE_CHECKING:
    from collections.abc import AsyncIterator
    from pathlib import Path

    from .bing import Bubble, Getter
    from .model import ObservationBatch
    from .store import PartWriter
    from .tiles import Bounds

__all__ = ["BingState", "OBS_ZOOM", "ROOT_ZOOM", "collect_streetside"]

#: Root-cell zoom. z11 is ~20 km at mid-latitudes: a dense cell holds ~1M bubbles
#: — a de-dup set that fits in memory — while empty land is one probe per cell.
ROOT_ZOOM = 11

#: Zoom at which a bubble's tile coordinates are recorded, matching Apple and
#: Mapy so observations from every provider share one grid.
OBS_ZOOM = 17

_MAGIC = b"SRBG"
_VERSION = 1


def _tile_of(lon: float, lat: float, zoom: int) -> tuple[int, int]:
    span = 1 << zoom
    x = int((lon + 180.0) / 360.0 * span)
    y = int((1.0 - math.asinh(math.tan(math.radians(lat))) / math.pi) / 2.0 * span)
    return max(0, min(span - 1, x)), max(0, min(span - 1, y))


def _cell_of(tile: Tile) -> Cell:
    span = 1 << tile.z
    west = tile.x / span * 360.0 - 180.0
    east = (tile.x + 1) / span * 360.0 - 180.0
    north = math.degrees(math.atan(math.sinh(math.pi * (1.0 - 2.0 * tile.y / span))))
    south = math.degrees(math.atan(math.sinh(math.pi * (1.0 - 2.0 * (tile.y + 1) / span))))
    return Cell(north=north, south=south, east=east, west=west)


@dataclass
class BingState:
    """Which root cells are swept, plus the tally, for resuming a run."""

    grid: ProbedGrid
    zoom: int = ROOT_ZOOM
    bubbles: int = 0
    probes: int = 0

    @classmethod
    def for_bounds(cls, bounds: Bounds, zoom: int = ROOT_ZOOM) -> BingState:
        source = TileSource(id="bing", url_template="", min_zoom=zoom, max_zoom=zoom, bounds=bounds)
        return cls(grid=ProbedGrid.for_bounds(bounds, source, zoom), zoom=zoom)

    @property
    def cells_done(self) -> int:
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
                "bubbles": self.bubbles,
                "probes": self.probes,
                "cells_done": len(self.grid),
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
    def load(cls, path: Path) -> BingState:
        import json
        import struct

        with open(path, "rb") as handle:
            if handle.read(4) != _MAGIC:
                raise ValueError(f"{path} is not a Bing sweep snapshot")
            version, header_len = struct.unpack("<BI", handle.read(5))
            if version != _VERSION:
                raise ValueError(f"{path}: unsupported snapshot version {version}")
            header = json.loads(handle.read(header_len))
            grid = ProbedGrid(header["x0"], header["y0"], header["width"], header["height"])
            grid._bits[:] = handle.read(len(grid._bits))  # noqa: SLF001
            grid._count = header["cells_done"]  # noqa: SLF001
        return cls(
            grid=grid, zoom=header["zoom"], bubbles=header["bubbles"], probes=header["probes"]
        )


def _record(batch: ObservationBatch, bubbles: list[Bubble]) -> None:
    """Group a set of bubbles by observation tile and add them."""
    if not bubbles:
        return
    by_tile: dict[tuple[int, int], list[Bubble]] = {}
    for b in bubbles:
        by_tile.setdefault(_tile_of(b.lon, b.lat, OBS_ZOOM), []).append(b)
    for (x, y), group in by_tile.items():
        batch.add(
            tile_x=x,
            tile_y=y,
            pano_id=[b.id for b in group],
            lon=[b.lon for b in group],
            lat=[b.lat for b in group],
            captured_at=[b.captured_at for b in group],
        )


async def collect_streetside(
    fetcher: Getter,
    bounds: Bounds,
    *,
    writer: PartWriter[ObservationBatch],
    state: BingState,
    limit: int | None = None,
) -> AsyncIterator[Path]:
    """Sweep every root cell of `bounds`, writing bubbles, yielding each part.

    The generator shape is the ordering rule the other collectors share — upload
    the part, then snapshot:

        async for part in collect_streetside(fetcher, bounds, writer=writer, state=state):
            upload(part)
            part.unlink()
            state.save(snapshot)

    Nothing here saves `state`: only the caller knows when the rows became
    durable. `limit` caps the number of *root cells* swept, which bounds a first
    run; the sweep inside a cell is not capped, since stopping mid-cell would mark
    a cell done whose bubbles were only partly written.
    """
    if writer.batch.capture_precision != CAPTURE_PRECISION:
        raise ValueError(
            f"writer records {writer.batch.capture_precision!r}; Bing dates to the "
            f"second, so it must be {CAPTURE_PRECISION!r}"
        )

    async def probe(lat: float, lon: float) -> list[Bubble]:
        state.probes += 1
        return await getbubbles(fetcher, lon, lat)

    swept = 0
    source = TileSource(
        id="bing", url_template="", min_zoom=state.zoom, max_zoom=state.zoom, bounds=bounds
    )
    for tile in source.tiles_at(state.zoom):
        if tile in state.grid:
            continue
        if limit is not None and swept >= limit:
            break

        cell = _cell_of(tile)
        seen: set[int] = set()
        async for result in sweep(
            probe, (cell.west, cell.south, cell.east, cell.north), cap=MAX_COUNT
        ):
            fresh = [b for b in result.bubbles if b.id not in seen]
            seen.update(b.id for b in fresh)
            _record(writer.batch, fresh)

        state.bubbles += len(seen)
        state.grid.add(tile)
        swept += 1
        if writer.full():
            part = writer.flush()
            if part is not None:
                yield part

    remainder = writer.flush()
    if remainder is not None:
        yield remainder

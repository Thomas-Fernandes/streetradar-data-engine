"""Google Street View dates: the sampled second pass over a country.

The geometry pass found where the coverage is; this dates it. It floods the same
z17 tiles — which is where the string panorama ids live, since the observation
store keeps only a numeric hash — and for one panorama per sample cell it fetches
the metadata, writing that panorama's date and its whole historical stack as
dated observations.

## Why sampled, and by cell

Dating every panorama is one metadata request each — ~75 per tile, months of
requests worldwide (ADR 0017). But a drive shares a date and adjacent panoramas
share a history, so one call per `SAMPLE_ZOOM` cell (~150 m) captures the date and
the time-travel with negligible loss, at roughly one metadata request per covered
cell. That is the ×2-ish depth the owner chose over the ×75 of dating everything.

## What it writes

Dated observations at month precision — Google gives year and month, never a
first-party timestamp, so the front end must not render a false day. Each sampled
call yields the current panorama (dated) and every historical layer at that spot
(each dated), so the time slider has its full range from a sparse sample.

## Resuming

Two bitmaps: which z17 tiles the flood has probed, and which sample cells have
been dated. A run that dies resumes both. Nothing is written before it is durable,
and the caller snapshots after uploading — the ordering rule the other collectors
share.
"""

from __future__ import annotations

import math
from dataclasses import dataclass
from typing import TYPE_CHECKING

from .flood import ProbedGrid
from .google import TILE_ZOOM, getpanos
from .google_meta import METADATA_PRECISION, getmetadata
from .tiles import Tile, TileSource

if TYPE_CHECKING:
    from collections.abc import AsyncIterator
    from pathlib import Path

    from .google import Getter, Panorama
    from .google_meta import Metadata
    from .model import ObservationBatch
    from .store import PartWriter
    from .tiles import Bounds

__all__ = ["SAMPLE_ZOOM", "DatesState", "collect_dates"]

#: One metadata call per cell at this zoom — ~150 m at mid-latitudes, fine enough
#: that no drive's date is missed, coarse enough to keep the pass near ×2 of
#: geometry rather than ×75.
SAMPLE_ZOOM = 15

_MAGIC = b"SRGD"
_VERSION = 1


def _tile_of(lon: float, lat: float, zoom: int) -> tuple[int, int]:
    span = 1 << zoom
    x = int((lon + 180.0) / 360.0 * span)
    y = int((1.0 - math.asinh(math.tan(math.radians(lat))) / math.pi) / 2.0 * span)
    return max(0, min(span - 1, x)), max(0, min(span - 1, y))


def _pano_key(pano_id: str) -> int:
    return int.from_bytes(pano_id.encode()[:8].ljust(8, b"\0"), "big")


@dataclass
class DatesState:
    """Which tiles are flooded and which sample cells are dated, for resuming."""

    tiles: ProbedGrid
    cells: ProbedGrid
    tile_zoom: int = TILE_ZOOM
    sample_zoom: int = SAMPLE_ZOOM
    dated: int = 0
    metadata_calls: int = 0

    @classmethod
    def for_bounds(
        cls, bounds: Bounds, tile_zoom: int = TILE_ZOOM, sample_zoom: int = SAMPLE_ZOOM
    ) -> DatesState:
        def grid(zoom: int) -> ProbedGrid:
            src = TileSource(
                id="google", url_template="", min_zoom=zoom, max_zoom=zoom, bounds=bounds
            )
            return ProbedGrid.for_bounds(bounds, src, zoom)

        return cls(
            tiles=grid(tile_zoom),
            cells=grid(sample_zoom),
            tile_zoom=tile_zoom,
            sample_zoom=sample_zoom,
        )

    def save(self, path: Path) -> None:
        import json
        import struct

        def dims(g: ProbedGrid) -> dict[str, int]:
            return {"x0": g.x0, "y0": g.y0, "width": g.width, "height": g.height, "count": len(g)}

        header = json.dumps(
            {
                "tiles": dims(self.tiles),
                "cells": dims(self.cells),
                "tile_zoom": self.tile_zoom,
                "sample_zoom": self.sample_zoom,
                "dated": self.dated,
                "metadata_calls": self.metadata_calls,
            }
        ).encode()
        tmp = path.with_suffix(path.suffix + ".tmp")
        with open(tmp, "wb") as handle:
            handle.write(_MAGIC)
            handle.write(struct.pack("<BI", _VERSION, len(header)))
            handle.write(header)
            handle.write(self.tiles._bits)  # noqa: SLF001 — same package
            handle.write(self.cells._bits)  # noqa: SLF001
        tmp.replace(path)

    @classmethod
    def load(cls, path: Path) -> DatesState:
        import json
        import struct

        with open(path, "rb") as handle:
            if handle.read(4) != _MAGIC:
                raise ValueError(f"{path} is not a Google dates snapshot")
            version, header_len = struct.unpack("<BI", handle.read(5))
            if version != _VERSION:
                raise ValueError(f"{path}: unsupported snapshot version {version}")
            header = json.loads(handle.read(header_len))

            def load_grid(d: dict[str, int]) -> ProbedGrid:
                g = ProbedGrid(d["x0"], d["y0"], d["width"], d["height"])
                g._bits[:] = handle.read(len(g._bits))  # noqa: SLF001
                g._count = d["count"]  # noqa: SLF001
                return g

            tiles = load_grid(header["tiles"])
            cells = load_grid(header["cells"])
        return cls(
            tiles=tiles,
            cells=cells,
            tile_zoom=header["tile_zoom"],
            sample_zoom=header["sample_zoom"],
            dated=header["dated"],
            metadata_calls=header["metadata_calls"],
        )


def _write_metadata(batch: ObservationBatch, meta: Metadata) -> int:
    """Write the current panorama and its history as dated observations.

    Returns how many rows were written. An undated current panorama is skipped —
    the sample landed on coverage with no reported date, which the front end could
    not place on the slider — but its history, which is dated, is still written.
    """
    written = 0
    if meta.captured_at is not None:
        tx, ty = _tile_of(meta.lon, meta.lat, TILE_ZOOM)
        batch.add(
            tile_x=tx,
            tile_y=ty,
            pano_id=[_pano_key(meta.id)],
            lon=[meta.lon],
            lat=[meta.lat],
            captured_at=[meta.captured_at],
        )
        written += 1
    for h in meta.history:
        tx, ty = _tile_of(h.lon, h.lat, TILE_ZOOM)
        batch.add(
            tile_x=tx,
            tile_y=ty,
            pano_id=[_pano_key(h.id)],
            lon=[h.lon],
            lat=[h.lat],
            captured_at=[h.captured_at],
        )
        written += 1
    return written


async def collect_dates(
    fetcher: Getter,
    bounds: Bounds,
    *,
    writer: PartWriter[ObservationBatch],
    state: DatesState,
    seed_step: int = 16,
    limit: int | None = None,
) -> AsyncIterator[Path]:
    """Flood the country, date one panorama per sample cell, yield each part.

    The generator shape is the shared ordering rule — upload the part, then
    snapshot. Nothing here saves `state`. `limit` caps tiles probed.
    """
    import asyncio
    from collections import deque

    if writer.batch.capture_precision != METADATA_PRECISION:
        raise ValueError(
            f"writer records {writer.batch.capture_precision!r}; Google dates to the "
            f"month, so it must be {METADATA_PRECISION!r}"
        )

    source = TileSource(
        id="google",
        url_template="",
        min_zoom=state.tile_zoom,
        max_zoom=state.tile_zoom,
        bounds=bounds,
    )
    x0, y0, x1, y1 = source.tile_range(state.tile_zoom)

    def inside(x: int, y: int) -> bool:
        return x0 <= x < x1 and y0 <= y < y1

    queued: set[tuple[int, int]] = set()
    frontier: deque[tuple[int, int]] = deque()

    def enqueue(x: int, y: int) -> None:
        if (x, y) not in queued and Tile(state.tile_zoom, x, y) not in state.tiles:
            queued.add((x, y))
            frontier.append((x, y))

    for sx in range(x0 + (-x0) % seed_step, x1, seed_step):
        for sy in range(y0 + (-y0) % seed_step, y1, seed_step):
            enqueue(sx, sy)

    probed = 0
    while frontier:
        if limit is not None and probed >= limit:
            break
        batch: list[tuple[int, int]] = []
        while frontier and len(batch) < 32:
            batch.append(frontier.popleft())

        tile_panos = await asyncio.gather(*(getpanos(fetcher, x, y) for x, y in batch))
        probed += len(batch)

        # Pick one panorama per undated sample cell, from this round's coverage.
        to_date: dict[tuple[int, int], Panorama] = {}
        for (x, y), panos in zip(batch, tile_panos, strict=True):
            state.tiles.add(Tile(state.tile_zoom, x, y))
            if not panos:
                continue
            for pano in panos:
                for dx, dy in ((-1, 0), (1, 0), (0, -1), (0, 1)):
                    if inside(x + dx, y + dy):
                        enqueue(x + dx, y + dy)
                cell = _tile_of(pano.lon, pano.lat, state.sample_zoom)
                if cell not in to_date and Tile(state.sample_zoom, *cell) not in state.cells:
                    to_date[cell] = pano

        if to_date:
            metas = await asyncio.gather(*(getmetadata(fetcher, p.id) for p in to_date.values()))
            state.metadata_calls += len(metas)
            for cell, meta in zip(to_date.keys(), metas, strict=True):
                state.cells.add(Tile(state.sample_zoom, *cell))
                if meta is not None:
                    state.dated += _write_metadata(writer.batch, meta)

        if writer.full():
            part = writer.flush()
            if part is not None:
                yield part

    remainder = writer.flush()
    if remainder is not None:
        yield remainder

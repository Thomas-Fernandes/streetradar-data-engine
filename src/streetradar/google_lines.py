"""Google Street View coverage as lines: chain the road graph, tile by tile.

The geometry pass stores panoramas as points. This stores the same flood as
*lines* — the coverage the front end draws — by chaining each tile's neighbour
graph into polylines. It is the line counterpart of `collect_geometry`: pick this
one when the output is the coverage-line layer rather than dated points.

## How lines are made

A tile's response carries the panoramas and the links between them.
`google.chain_graph` cuts that graph into maximal runs of degree-two nodes,
breaking at junctions — so a straight road is one line and a crossroads splits
them, the shape a road network has. Each polyline is stored in the tile's own
integer coordinates (`LineBatch`, 3.7 bytes/vertex) and rendered back to WGS84 at
the tiling stage, exactly as Já and Naver lines are.

## Undated, on purpose

These lines are the *current* coverage geometry and carry no date — a tile has
none. The time slider is fed separately by the dates pass (`google_dates`), which
samples the per-panorama metadata. So the line layer answers "where is there
coverage" and the dated points answer "when"; keeping them apart is what lets the
cheap line pass and the expensive dates pass run independently.
"""

from __future__ import annotations

import math
from typing import TYPE_CHECKING

from .google import chain_graph, getpanos
from .lines import LineBatch
from .tiles import Tile, TileSource

if TYPE_CHECKING:
    from collections.abc import AsyncIterator
    from pathlib import Path

    from .google import Getter
    from .google_collect import GoogleState
    from .store import PartWriter
    from .tiles import Bounds

__all__ = ["LINE_EXTENT", "LINE_PRECISION", "collect_lines"]

#: The tile-local coordinate grid the lines are stored on. 4096 matches the MVT
#: extent Já and Naver use, so the line tiler treats every provider the same.
LINE_EXTENT = 4096

#: The line layer is the current coverage; it carries no date (a tile has none).
LINE_PRECISION = "unknown"


def _to_local(lon: float, lat: float, tx: int, ty: int, zoom: int) -> tuple[float, float]:
    """A WGS84 point to the tile's own coordinates — the inverse of the tiler's.

    Matches `tiles.offset_to_wgs84`, including its north-up y flip, so a point
    stored here comes back to the same lon/lat at the tiling stage.
    """
    span = 1 << zoom
    x = ((lon + 180.0) / 360.0 * span - tx) * LINE_EXTENT
    world_y = (1.0 - math.asinh(math.tan(math.radians(lat))) / math.pi) / 2.0
    y = (ty + 1 - world_y * span) * LINE_EXTENT
    return x, y


async def collect_lines(
    fetcher: Getter,
    bounds: Bounds,
    *,
    writer: PartWriter[LineBatch],
    state: GoogleState,
    seed_step: int = 16,
    limit: int | None = None,
) -> AsyncIterator[Path]:
    """Flood the country's z17 tiles, writing chained coverage lines, yielding parts.

    The same flood as `collect_geometry` — clipped to the country, resumable via
    the shared `GoogleState` — but each covered tile's graph is chained and stored
    as lines instead of its panoramas as points. The generator shape is the shared
    ordering rule; nothing here saves `state`.
    """
    import asyncio
    from collections import deque

    if writer.batch.zoom != state.zoom:
        raise ValueError(f"writer stores zoom {writer.batch.zoom}, not the tile zoom {state.zoom}")
    if writer.batch.extent != LINE_EXTENT:
        raise ValueError(
            f"writer extent {writer.batch.extent} is not the line extent {LINE_EXTENT}"
        )

    source = TileSource(
        id="google", url_template="", min_zoom=state.zoom, max_zoom=state.zoom, bounds=bounds
    )
    x0, y0, x1, y1 = source.tile_range(state.zoom)

    def inside(x: int, y: int) -> bool:
        return x0 <= x < x1 and y0 <= y < y1

    queued: set[tuple[int, int]] = set()
    frontier: deque[tuple[int, int]] = deque()

    def enqueue(x: int, y: int) -> None:
        if (x, y) not in queued and Tile(state.zoom, x, y) not in state.grid:
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
        while frontier and len(batch) < 64:
            batch.append(frontier.popleft())

        results = await asyncio.gather(*(getpanos(fetcher, x, y) for x, y in batch))
        probed += len(batch)
        for (x, y), panos in zip(batch, results, strict=True):
            state.grid.add(Tile(state.zoom, x, y))
            if not panos:
                continue
            state.covered += 1
            polylines = chain_graph(panos)
            if polylines:
                local = [
                    [_to_local(lon, lat, x, y, state.zoom) for lon, lat in line]
                    for line in polylines
                ]
                writer.batch.add(tile_x=x, tile_y=y, lines=local)
            for dx, dy in ((-1, 0), (1, 0), (0, -1), (0, 1)):
                if inside(x + dx, y + dy):
                    enqueue(x + dx, y + dy)

        if writer.full():
            part = writer.flush()
            if part is not None:
                yield part

    remainder = writer.flush()
    if remainder is not None:
        yield remainder

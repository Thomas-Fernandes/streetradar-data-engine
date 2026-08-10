"""Flood fill over a tile grid, for providers that publish no pyramid.

`harvest_pyramid` works by descending: fetch a coarse tile, and only descend
where it had content. That needs the provider to serve more than one zoom. Apple
does not — every zoom other than 17 returns an empty body — so there is no coarse
level to prune against, and a flat sweep of a country-sized bounding box is
hundreds of millions of requests.

What Apple does give is the position of every panorama inside the tile. Coverage
follows roads, so a tile crossed by a straight road has two exits rather than
four: if no panorama comes near the western edge, the road does not leave that
way and the western neighbour is not worth a request. Expanding only along the
exits is what makes this affordable.

Measured on a real Apple export of Luxembourg — 34,965 covered tiles:

    flat sweep of the bounding box     111,197 requests   3.17 per useful tile
    flood, all four neighbours          63,709 requests   1.82 per useful tile
    flood, exits only                   36,610 requests   1.06 per useful tile

The same ratio held on Greece and Japan (1.06 and 1.03 requests per useful tile),
where a flat sweep would have cost 9.8M and 20.1M requests respectively.

## Seeding

A flood only finds what is connected to a seed, and coverage is not one
component: Luxembourg has 22, Greece 83, Japan 59. A single seed recovers 99.6%
of Luxembourg but only 76% of Greece and 62% of Japan. Seeds from the previous
run recover 100% everywhere, which is why the maintenance regime seeds from the
tiles it found last time.

## Resuming

The state that matters is which tiles have already been probed, held as a bitmap
over the region — 125 MB for Australia, where a set of tuples would be 4 GB. The
pending frontier is small and is stored alongside it.

The ordering rule is the one the previous engine got backwards: **snapshot after
the data is durable, never before.** A crash then re-probes everything since the
last snapshot, which is wasteful but correct, rather than marking tiles done
whose rows were never written.
"""

from __future__ import annotations

import json
import struct
from collections.abc import AsyncIterator, Callable, Collection, Iterable, Sequence
from dataclasses import dataclass, field
from pathlib import Path
from typing import Final

from .tiles import Bounds, Tile, TileFetcher, TileResult, TileSource

__all__ = [
    "DIRECTIONS",
    "Direction",
    "FloodState",
    "ProbedGrid",
    "exits_from_points",
    "harvest_flood",
]

Direction = str

#: Offsets for the four edges a road can leave a tile through. Diagonals are
#: deliberately absent: measured, they cost 33–52% more requests and their recall
#: advantage disappears as soon as seeding is adequate.
DIRECTIONS: Final[dict[Direction, tuple[int, int]]] = {
    "W": (-1, 0),
    "E": (1, 0),
    "N": (0, -1),
    "S": (0, 1),
}

_MAGIC: Final = b"SRFF"
_VERSION: Final = 1


class ProbedGrid:
    """Which tiles have been probed, as a bitmap over a bounded region.

    A plain `bytearray` rather than numpy: the operations are one bit test and
    one bit set per tile, so at a few thousand tiles a second the cost is
    irrelevant, and it keeps numpy out of the core dependencies.
    """

    __slots__ = ("_bits", "_count", "height", "width", "x0", "y0")

    def __init__(self, x0: int, y0: int, width: int, height: int) -> None:
        if width <= 0 or height <= 0:
            raise ValueError("grid must have positive extent")
        self.x0 = x0
        self.y0 = y0
        self.width = width
        self.height = height
        self._bits = bytearray((width * height + 7) // 8)
        self._count = 0

    @classmethod
    def for_bounds(cls, bounds: Bounds, source: TileSource, zoom: int) -> ProbedGrid:
        """Cover everything `source` could enumerate inside `bounds`, plus a margin.

        The margin matters: a flood legitimately probes one tile beyond the last
        covered one, and that probe must be recordable or it repeats forever.
        """
        west, south, east, north = bounds
        corner = TileSource(
            id=source.id,
            url_template=source.url_template,
            min_zoom=source.min_zoom,
            max_zoom=source.max_zoom,
            bounds=(west, south, east, north),
        )
        tiles = list(corner.tiles_at(zoom))
        if not tiles:
            raise ValueError(f"{source.id}: bounds enclose no tiles at zoom {zoom}")
        xs = [t.x for t in tiles]
        ys = [t.y for t in tiles]
        margin = 2
        return cls(
            x0=min(xs) - margin,
            y0=min(ys) - margin,
            width=max(xs) - min(xs) + 1 + 2 * margin,
            height=max(ys) - min(ys) + 1 + 2 * margin,
        )

    def __len__(self) -> int:
        return self._count

    def __contains__(self, tile: Tile) -> bool:
        index = self._index(tile)
        if index is None:
            # Outside the region counts as probed: it stops the flood at the
            # edge rather than letting it wander off the grid.
            return True
        return bool(self._bits[index >> 3] & (1 << (index & 7)))

    def add(self, tile: Tile) -> bool:
        """Record a probe. Returns False when it was already recorded."""
        index = self._index(tile)
        if index is None:
            return False
        byte, bit = index >> 3, 1 << (index & 7)
        if self._bits[byte] & bit:
            return False
        self._bits[byte] |= bit
        self._count += 1
        return True

    def _index(self, tile: Tile) -> int | None:
        dx = tile.x - self.x0
        dy = tile.y - self.y0
        if not (0 <= dx < self.width and 0 <= dy < self.height):
            return None
        return dy * self.width + dx


@dataclass
class FloodState:
    """Everything needed to resume a flood exactly where it stopped."""

    grid: ProbedGrid
    frontier: list[Tile] = field(default_factory=list)
    zoom: int = 0
    hits: int = 0

    @property
    def probed(self) -> int:
        return len(self.grid)

    def save(self, path: Path) -> None:
        """Write a snapshot, atomically.

        Call this **after** the data for everything probed so far is durable.
        Snapshotting first is what turns a crash into silently missing rows.
        """
        header = json.dumps(
            {
                "x0": self.grid.x0,
                "y0": self.grid.y0,
                "width": self.grid.width,
                "height": self.grid.height,
                "zoom": self.zoom,
                "hits": self.hits,
                "probed": len(self.grid),
                "frontier": len(self.frontier),
            }
        ).encode()

        tmp = path.with_suffix(path.suffix + ".tmp")
        with open(tmp, "wb") as handle:
            handle.write(_MAGIC)
            handle.write(struct.pack("<BI", _VERSION, len(header)))
            handle.write(header)
            handle.write(self.grid._bits)  # noqa: SLF001 — same module
            for tile in self.frontier:
                handle.write(struct.pack("<ii", tile.x, tile.y))
        tmp.replace(path)

    @classmethod
    def load(cls, path: Path) -> FloodState:
        with open(path, "rb") as handle:
            if handle.read(4) != _MAGIC:
                raise ValueError(f"{path} is not a flood snapshot")
            version, header_len = struct.unpack("<BI", handle.read(5))
            if version != _VERSION:
                raise ValueError(f"{path}: unsupported snapshot version {version}")
            header = json.loads(handle.read(header_len))
            grid = ProbedGrid(header["x0"], header["y0"], header["width"], header["height"])
            bits = handle.read(len(grid._bits))  # noqa: SLF001
            grid._bits[:] = bits  # noqa: SLF001
            grid._count = header["probed"]  # noqa: SLF001
            frontier = [
                Tile(header["zoom"], *struct.unpack("<ii", handle.read(8)))
                for _ in range(header["frontier"])
            ]
        return cls(grid=grid, frontier=frontier, zoom=header["zoom"], hits=header["hits"])


def exits_from_points(
    points: Iterable[tuple[float, float]],
    tile: Tile,
    *,
    margin: float = 0.05,
) -> set[Direction]:
    """Which edges a tile's content approaches, from WGS84 points inside it.

    `margin` is a fraction of the tile. 0.05 was the best of the values measured
    on Luxembourg — 36,610 requests against 38,417 at 0.10 and 43,419 at 0.25,
    for the same recall. Widening it only buys probes into tiles the road never
    reaches.
    """
    import math

    span = float(1 << tile.z)
    found: set[Direction] = set()
    for lon, lat in points:
        world_x = (lon + 180.0) / 360.0 * span
        sin_lat = math.sin(math.radians(lat))
        world_y = (0.5 - math.log((1 + sin_lat) / (1 - sin_lat)) / (4 * math.pi)) * span
        fx = world_x - tile.x
        fy = world_y - tile.y
        if fx < margin:
            found.add("W")
        if fx > 1.0 - margin:
            found.add("E")
        if fy < margin:
            found.add("N")
        if fy > 1.0 - margin:
            found.add("S")
        if len(found) == 4:
            break
    return found


async def harvest_flood(
    fetcher: TileFetcher,
    source: TileSource,
    zoom: int,
    *,
    seeds: Iterable[Tile],
    exits: Callable[[bytes, Tile], Collection[Direction]],
    state: FloodState | None = None,
    limit: int | None = None,
) -> AsyncIterator[TileResult]:
    """Expand outwards from `seeds`, following the exits each tile declares.

    `exits` receives a non-empty body and returns the edges its content
    approaches; `exits_from_points` covers the usual case. It is only called for
    tiles that returned something, so it never has to handle absence.

    Pass `state` to resume: seeds already probed are skipped and the stored
    frontier is picked up. Snapshot it with `FloodState.save` **after** flushing
    whatever the results were written into — see the module docstring.

    `limit` caps the number of requests, which is what makes a bounded probe
    bounded. The state is left consistent, so the next call continues.
    """
    if state is None:
        state = FloodState(grid=ProbedGrid.for_bounds(source.bounds, source, zoom), zoom=zoom)
    if state.zoom and state.zoom != zoom:
        raise ValueError(f"state was built for zoom {state.zoom}, not {zoom}")
    state.zoom = zoom

    frontier: list[Tile] = list(state.frontier)
    frontier.extend(t for t in seeds if t not in state.grid)
    state.frontier = []

    spent = 0
    while frontier:
        if limit is not None and spent >= limit:
            break

        batch: list[Tile] = []
        for tile in frontier:
            if limit is not None and spent + len(batch) >= limit:
                break
            if tile in state.grid:
                continue
            batch.append(tile)
        if not batch:
            break

        # Everything not taken this round stays queued, so a limit or a crash
        # loses nothing.
        taken = set(batch)
        remaining = [t for t in frontier if t not in taken and t not in state.grid]

        results = await _probe(fetcher, source, batch)
        spent += len(batch)

        next_frontier: list[Tile] = []
        for result in results:
            state.grid.add(result.tile)
            if result.body:
                state.hits += 1
                for direction in exits(result.body, result.tile):
                    offset = DIRECTIONS.get(direction)
                    if offset is None:
                        raise ValueError(f"unknown exit direction {direction!r}")
                    neighbour = Tile(zoom, result.tile.x + offset[0], result.tile.y + offset[1])
                    if neighbour not in state.grid:
                        next_frontier.append(neighbour)
            yield result

        frontier = remaining + next_frontier

    state.frontier = [t for t in frontier if t not in state.grid]


async def _probe(
    fetcher: TileFetcher, source: TileSource, tiles: Sequence[Tile]
) -> list[TileResult]:
    """Fetch one round of the frontier concurrently."""
    import asyncio

    bodies = await asyncio.gather(
        *(fetcher.fetch_tile(source.url(t), source.headers(t)) for t in tiles)
    )
    return [TileResult(tile, body) for tile, body in zip(tiles, bodies, strict=True)]

"""Google Street View coverage, over the endpoints its own viewer uses.

The original brief excluded Google; the owner reversed that. Google is the
largest and most temporally rich provider — coverage back to 2007 with many
revisits — and its endpoints are the internal ones the maps.google.com viewer
calls, taken via `streetlevel`'s reverse engineering. All are permitted by
`robots.txt` on the hosts we use. ADR 0017.

## Three endpoints, three jobs

    photometa/ac/v1  (tactile, z17)   the coverage tile: id, position, elevation,
                                      orientation, and the neighbour graph. NO date.
    SingleImageSearch  (per pano)     the date and the full historical stack.
    mts2 .../mapslt  (svv raster)     a coverage detector at any zoom.

## Why a tile is geometry-only, and what that costs

A z17 tile returns ~66–170 panoramas — every current panorama in that tile, with
the links between them — in one request. It carries no capture date: the tile is
a pure geometry index. The date, and the time-travel stack, live only in the
per-panorama metadata endpoint, one request per panorama. So geometry is cheap
(one request per tile) and dates are the expensive second pass — which is also
the only possible order, since dating a panorama needs its id, which the tile
supplies. This module does the geometry; `google_meta` does the dates.

## The raster layer is a free coverage index

`mts2.google.com/.../mapslt?lyrs=svv` serves the blue-line coverage layer as a
PNG at any zoom. Measured: an empty tile is a fixed 68-byte transparent PNG at
every zoom; any coverage makes it larger. So the response *size* is a coverage
detector — no need to decode the image. At z8 the whole world is 65,536 tiles, so
detecting where coverage exists, before flooding, is minutes of work and prunes
oceans and deserts. This is what avoids probing the empty 99% of the planet.
"""

from __future__ import annotations

import json
from typing import TYPE_CHECKING, Final, NamedTuple, Protocol

if TYPE_CHECKING:
    from collections.abc import Sequence

__all__ = [
    "EMPTY_RASTER_BYTES",
    "Getter",
    "Panorama",
    "TILE_ZOOM",
    "coverage_raster_url",
    "getpanos",
    "chain_graph",
    "links_to_edges",
    "has_coverage",
    "parse_tile",
    "tile_url",
]

#: Coverage tiles are served at zoom 17 and no other; every other zoom errors.
TILE_ZOOM: Final = 17

_TILE_URL: Final = (
    "https://www.google.com/maps/photometa/ac/v1?pb=!1m1!1smaps_sv.tactile!6m3!1i{x}!2i{y}!3i17!8b1"
)

#: The blue-line coverage raster, used only as a size-based detector.
_RASTER_URL: Final = (
    "https://mts2.google.com/mapslt?lyrs=svv&x={x}&y={y}&z={z}&w=256&h=256&hl=en&style=40,18"
)

#: An empty coverage-raster tile is this fixed transparent PNG at every zoom.
#: Anything larger has coverage. Measured across many tiles and zooms.
EMPTY_RASTER_BYTES: Final = 68

#: Google's response is JSON behind an anti-hijacking prefix.
_XSSI_PREFIX: Final = ")]}'"


class Panorama(NamedTuple):
    """One current panorama from a coverage tile.

    `links` are indices into the same tile's panorama list — the local road
    graph, which is what turns the points into lines. No date: a tile carries
    none, so `captured_at` is filled only by the metadata pass, never here.
    """

    id: str
    lon: float
    lat: float
    elevation: float | None
    links: tuple[int, ...]


class Getter(Protocol):
    """The single method these calls need from a fetcher — a plain GET."""

    async def get(self, url: str, *, missing_ok: bool = ...) -> bytes | None: ...


def tile_url(x: int, y: int) -> str:
    """The coverage-tile URL for slippy tile (x, y) at zoom 17."""
    return _TILE_URL.format(x=x, y=y)


def coverage_raster_url(x: int, y: int, z: int) -> str:
    """The coverage-raster URL for detecting whether tile (x, y, z) has coverage."""
    return _RASTER_URL.format(x=x, y=y, z=z)


def has_coverage(body: bytes | None) -> bool:
    """Whether a coverage-raster response indicates any coverage.

    Pure size test: the empty tile is a fixed 68-byte PNG, so a larger body means
    the region has coverage. Decoding the PNG would be exact but this is what makes
    world-scale detection cheap.
    """
    return body is not None and len(body) > EMPTY_RASTER_BYTES


def parse_tile(body: bytes | None) -> list[Panorama]:
    """Decode a coverage-tile response into current panoramas. Pure — no I/O.

    An absent or empty body, and a tile with no coverage (`data[1]` is null),
    both yield no panoramas rather than raising: over a sweep most tiles are empty.

    A malformed entry is skipped rather than aborting the tile — one bad record
    must not lose the other hundred-odd panoramas in a dense tile.
    """
    if not body:
        return []
    text = body.decode("utf-8", "replace") if isinstance(body, bytes) else body
    if text.startswith(_XSSI_PREFIX):
        newline = text.find("\n")
        text = text[newline + 1 :] if newline != -1 else text[len(_XSSI_PREFIX) :]
    try:
        data = json.loads(text)
    except (json.JSONDecodeError, ValueError):
        return []

    if not isinstance(data, list) or len(data) < 2 or not data[1]:
        return []
    entries = data[1][1] if len(data[1]) > 1 and data[1][1] else []

    panos: list[Panorama] = []
    for entry in entries:
        pano = _parse_entry(entry)
        if pano is not None:
            panos.append(pano)
    return panos


def _parse_entry(entry: object) -> Panorama | None:
    try:
        head = entry[0]  # type: ignore[index]
        identifier = head[0][1]
        position = head[2]
        lat = float(position[0][2])
        lon = float(position[0][3])
        elevation = float(position[1][0]) if position[1] and position[1][0] is not None else None
        links = tuple(int(i) for i in (entry[1] or ()))  # type: ignore[index]
    except (IndexError, TypeError, ValueError, KeyError):
        return None
    if not isinstance(identifier, str):
        return None
    return Panorama(id=identifier, lon=lon, lat=lat, elevation=elevation, links=links)


async def getpanos(fetcher: Getter, x: int, y: int) -> list[Panorama]:
    """Fetch and decode the coverage tile (x, y) at zoom 17, through the fetch path."""
    body = await fetcher.get(tile_url(x, y), missing_ok=True)
    return parse_tile(body)


def links_to_edges(panos: Sequence[Panorama]) -> set[tuple[str, str]]:
    """The undirected road-graph edges within a tile, as id pairs.

    Each link is an index into `panos`; an index past the end is dropped rather
    than raising, which happens for links that point at a panorama in a
    neighbouring tile not present here. Ordered so (a, b) and (b, a) coincide,
    for deduplication when tiles are stitched.
    """
    edges: set[tuple[str, str]] = set()
    for pano in panos:
        for index in pano.links:
            if 0 <= index < len(panos):
                other = panos[index]
                edges.add((pano.id, other.id) if pano.id < other.id else (other.id, pano.id))
    return edges


def chain_graph(panos: Sequence[Panorama]) -> list[list[tuple[float, float]]]:
    """Chain a tile's road graph into polylines — the coverage lines.

    A drive is a run of panoramas each linked to the next; a junction is where
    three or more roads meet. So the graph is cut into maximal paths whose
    interior nodes have exactly two neighbours, breaking at endpoints and
    junctions — the same shape a road network has. Returns each polyline as
    (lon, lat) points.

    Cross-tile links (indices past the tile) are absent, so a road leaving the
    tile simply ends at the edge; the neighbouring tile carries its continuation,
    and the two meet at the shared edge panorama, so the rendered lines join even
    though each tile is chained on its own. Pure closed loops with no junction are
    walked once from their lowest index so the output is deterministic.
    """
    n = len(panos)
    adjacency: list[set[int]] = [set() for _ in range(n)]
    for i, pano in enumerate(panos):
        for j in pano.links:
            if 0 <= j < n and j != i:
                adjacency[i].add(j)
                adjacency[j].add(i)

    used: set[frozenset[int]] = set()

    def walk(start: int, second: int) -> list[int]:
        path = [start, second]
        used.add(frozenset((start, second)))
        prev, current = start, second
        while len(adjacency[current]) == 2:
            nxt = next((k for k in adjacency[current] if k != prev), None)
            if nxt is None or frozenset((current, nxt)) in used:
                break
            used.add(frozenset((current, nxt)))
            path.append(nxt)
            prev, current = current, nxt
        return path

    paths: list[list[int]] = []
    # Start every path at an endpoint or junction, so interiors stay degree-2.
    for i in range(n):
        if len(adjacency[i]) != 2:
            for j in sorted(adjacency[i]):
                if frozenset((i, j)) not in used:
                    paths.append(walk(i, j))
    # Whatever edges remain lie on pure cycles with no junction.
    for i in range(n):
        for j in sorted(adjacency[i]):
            if frozenset((i, j)) not in used:
                paths.append(walk(i, j))

    return [[(panos[k].lon, panos[k].lat) for k in path] for path in paths]

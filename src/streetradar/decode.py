"""Turn tile bodies into usable WGS84 geometry.

Three things here are easy to get wrong and all three are handled explicitly.

**The y axis.** `mapbox_vector_tile.decode` defaults to `y_coord_down=False` and
applies `y = extent - y`, so decoded y points north while the MVT wire format has
it pointing south. Getting this backwards mirrors every road about the tile's
horizontal centre line — which looks plausible at a glance and is completely
wrong.

**Latitude is not linear within a tile.** A tile spans an equal amount of *Web
Mercator*, not an equal number of degrees of latitude. Interpolating latitude
linearly between the tile's north and south edges introduces an error that grows
with latitude — at Iceland's 64°N it is tens of metres, enough to put a road on
the wrong side of a building. The inverse Mercator is used instead.

**Tiles overlap.** Tippecanoe emits a small buffer beyond each tile's edge, so
decoded coordinates can be slightly negative or past `extent`, and the same
sliver of road appears in two neighbouring tiles. Left alone, summing lengths
across a country double-counts every tile boundary. Geometry is therefore clipped
to the tile's own square, which partitions the linework exactly once.
"""

from __future__ import annotations

import math
from collections.abc import Iterator, Sequence
from typing import Any, NamedTuple

from .tiles import Tile

__all__ = [
    "DecodedTile",
    "Line",
    "decode_tile",
    "geodesic_length_m",
]

_EARTH_RADIUS_M = 6_371_008.8

#: A polyline in WGS84, as (lon, lat) pairs.
Coords = tuple[tuple[float, float], ...]


class Line(NamedTuple):
    """One polyline and the attributes its feature carried."""

    coords: Coords
    properties: dict[str, Any]

    @property
    def length_m(self) -> float:
        return geodesic_length_m(self.coords)


class DecodedTile(NamedTuple):
    """Everything one tile body yielded."""

    tile: Tile
    layer: str
    extent: int
    lines: tuple[Line, ...]

    @property
    def length_m(self) -> float:
        return sum(line.length_m for line in self.lines)

    @property
    def vertices(self) -> int:
        return sum(len(line.coords) for line in self.lines)


def decode_tile(
    body: bytes | None,
    tile: Tile,
    *,
    layer: str,
    clip: bool = True,
) -> DecodedTile:
    """Decode one MVT body into WGS84 polylines.

    An absent (None) or empty body decodes to zero lines rather than raising:
    for a tile sweep "nothing here" is the common answer. Já signals it with
    HTTP 204 and Naver with a zero-length 200, and both arrive here as falsy.

    `clip` restricts geometry to the tile's own square, discarding tippecanoe's
    edge buffer. Leave it on for anything that aggregates across tiles; turn it
    off only when rendering a single tile in isolation.
    """
    if not body:
        return DecodedTile(tile=tile, layer=layer, extent=0, lines=())

    # Imported lazily so that `streetradar.tiles` stays importable — and the
    # harvest logic stays testable — without the decoding stack present.
    import mapbox_vector_tile

    decoded = mapbox_vector_tile.decode(body)
    if layer not in decoded:
        return DecodedTile(tile=tile, layer=layer, extent=0, lines=())

    layer_data = decoded[layer]
    extent = int(layer_data.get("extent", 4096))
    lines: list[Line] = []

    for feature in layer_data.get("features", ()):
        geometry = feature.get("geometry") or {}
        properties = dict(feature.get("properties") or {})
        for ring in _rings(geometry):
            for piece in _clip_to_tile(ring, extent) if clip else (ring,):
                if len(piece) < 2:
                    continue
                coords = tuple(_to_wgs84(x, y, tile, extent) for x, y in piece)
                lines.append(Line(coords=coords, properties=properties))

    return DecodedTile(tile=tile, layer=layer, extent=extent, lines=tuple(lines))


def geodesic_length_m(coords: Sequence[tuple[float, float]]) -> float:
    """Haversine length of a (lon, lat) polyline, in metres.

    Haversine rather than a projected length: the numbers this produces are
    compared against a published figure for the whole of Iceland, and a planar
    approximation at 64°N would be off by enough to make that comparison
    meaningless.
    """
    total = 0.0
    for (lon1, lat1), (lon2, lat2) in zip(coords, coords[1:], strict=False):
        phi1, phi2 = math.radians(lat1), math.radians(lat2)
        dphi = phi2 - phi1
        dlambda = math.radians(lon2 - lon1)
        h = math.sin(dphi / 2) ** 2 + math.cos(phi1) * math.cos(phi2) * math.sin(dlambda / 2) ** 2
        total += 2 * _EARTH_RADIUS_M * math.asin(math.sqrt(min(1.0, h)))
    return total


def _rings(geometry: dict[str, Any]) -> Iterator[list[tuple[float, float]]]:
    """Yield each polyline of a decoded geometry, whatever its type."""
    kind = geometry.get("type")
    coordinates = geometry.get("coordinates") or []
    if kind == "LineString":
        yield [(float(x), float(y)) for x, y in coordinates]
    elif kind == "MultiLineString":
        for ring in coordinates:
            yield [(float(x), float(y)) for x, y in ring]


def _to_wgs84(x: float, y: float, tile: Tile, extent: int) -> tuple[float, float]:
    """Tile-local (x, y) to (lon, lat).

    Decoded y points north, the Mercator formula wants it pointing south, hence
    the `extent - y`.
    """
    span = float(1 << tile.z) * extent
    world_x = (tile.x * extent + x) / span
    world_y = (tile.y * extent + (extent - y)) / span
    lon = world_x * 360.0 - 180.0
    lat = math.degrees(math.atan(math.sinh(math.pi * (1.0 - 2.0 * world_y))))
    return (lon, lat)


def _clip_to_tile(
    ring: Sequence[tuple[float, float]], extent: int
) -> list[list[tuple[float, float]]]:
    """Clip a polyline to [0, extent]², returning the surviving runs.

    Liang–Barsky per segment. A polyline that leaves and re-enters the square
    comes back as several runs, which is why this returns a list rather than one
    polyline.
    """
    lo, hi = 0.0, float(extent)
    runs: list[list[tuple[float, float]]] = []
    current: list[tuple[float, float]] = []

    for start, end in zip(ring, ring[1:], strict=False):
        clipped = _clip_segment(start, end, lo, hi)
        if clipped is None:
            if len(current) >= 2:
                runs.append(current)
            current = []
            continue
        a, b = clipped
        if not current:
            current = [a, b]
        elif current[-1] == a:
            current.append(b)
        else:
            if len(current) >= 2:
                runs.append(current)
            current = [a, b]

    if len(current) >= 2:
        runs.append(current)
    return runs


def _clip_segment(
    start: tuple[float, float],
    end: tuple[float, float],
    lo: float,
    hi: float,
) -> tuple[tuple[float, float], tuple[float, float]] | None:
    """Liang–Barsky clip of one segment against the square [lo, hi]²."""
    x0, y0 = start
    x1, y1 = end
    dx, dy = x1 - x0, y1 - y0
    t0, t1 = 0.0, 1.0

    for numerator, denominator in (
        (lo - x0, dx),
        (x0 - hi, -dx),
        (lo - y0, dy),
        (y0 - hi, -dy),
    ):
        if denominator == 0.0:
            if numerator > 0.0:
                return None
            continue
        t = numerator / denominator
        if denominator > 0.0:
            if t > t1:
                return None
            t0 = max(t0, t)
        else:
            if t < t0:
                return None
            t1 = min(t1, t)

    if t0 > t1:
        return None
    return (
        (x0 + t0 * dx, y0 + t0 * dy),
        (x0 + t1 * dx, y0 + t1 * dy),
    )

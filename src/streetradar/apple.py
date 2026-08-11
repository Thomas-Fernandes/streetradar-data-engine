"""Apple Look Around coverage tiles.

The one provider with a module of its own, because it is the one whose wire
format is not MVT. Everything else about it — the tile grid, the outbound path,
the flood fill that enumerates it — is shared.

## What a coverage tile contains

A protobuf `GroundMetadataTile`: a list of panorama positions, a build table
they index into, and the tile's own coordinates. Positions are **sixty-fourths
of a pixel over a 255-unit tile**, which resolves to the same inverse Mercator
that MVT needs, so `tiles.offset_to_wgs84` does the work for both.

The message also carries camera intrinsics, rig occlusion scores, altitude and
orientation. Coverage reads none of it. Letting protobuf skip those fields as
unknown was measured at **1.3%**, so the schema is kept whole rather than
trimmed — a schema narrowed to what we read is a schema that mis-parses silently
when a field number is wrong.

## Why the schema is vendored rather than imported

`streetlevel` owns the reverse engineering and is MIT, but importing it brings
pyproj, scipy and pycryptodome — 251 MB — for camera geometry, altitude
conversion and imagery signing, none of which coverage touches. The engine
resolved Já and Naver through its own MVT path, so the library would be carried
for a single 25-line function.

Measured on 200 synthetic tiles of 291 panoramas each:

    streetlevel parse_coverage_tile        509 tiles/s
    this module                          2,544 tiles/s

The gap is not protobuf — both use the same C backend. It is that upstream
builds a `LookaroundPanorama` dataclass with camera metadata per panorama, and
this yields columns straight into `ObservationBatch`. ADR 0011.

`streetlevel` stays the oracle this is tested against, the way `archive/` is the
oracle for `osm_match`, without ever being a dependency.
"""

from __future__ import annotations

import os
from typing import TYPE_CHECKING, Final, NamedTuple

from .tiles import Bounds, Tile, TileSource, offset_to_wgs84

if TYPE_CHECKING:
    from .model import CapturePrecision

__all__ = [
    "APPLE_ZOOM",
    "COVERAGE_TILE_URL",
    "COVERAGE_TYPES",
    "CoverageTile",
    "TokenMissing",
    "apple_source",
    "decode_coverage_tile",
]

#: Apple serves coverage at one zoom and rejects every other. This is why the
#: pyramid descent that works for Já and Naver cannot work here, and why
#: `streetradar.flood` exists.
APPLE_ZOOM: Final = 17

COVERAGE_TILE_URL: Final = "https://gspe76-ssl.ls.apple.com/api/tile?"

#: Tile-local positions are sixty-fourths of a pixel across a 255-unit tile.
_EXTENT: Final = 255.0
_SUBPIXEL: Final = 64.0

#: From the schema's own enum. `default` is declared by Apple and has never been
#: observed — 4,347,804 car and 108,857 backpack across the Luxembourg and Greece
#: exports — but it is handled rather than assumed away, which is where
#: streetlevel raises `ValueError: 0 is not a valid CoverageType`.
COVERAGE_TYPES: Final[dict[int, str]] = {0: "default", 2: "car", 3: "backpack"}

#: Apple reports a full UTC millisecond timestamp, so nothing is rounded away.
CAPTURE_PRECISION: Final[CapturePrecision] = "second"

_TOKEN_VARIABLE: Final = "ENGINE_APPLE_TOKEN"


class TokenMissing(RuntimeError):
    """`ENGINE_APPLE_TOKEN` is not set."""


class CoverageTile(NamedTuple):
    """One decoded tile, as the columns `ObservationBatch.add` takes.

    Columns rather than a list of records: at 1,500 tiles a second the writer
    sees ~436,000 panoramas a second, and a Python object per panorama does not
    keep up. See `streetradar.model`.
    """

    tile: Tile
    pano_id: tuple[int, ...]
    lon: tuple[float, ...]
    lat: tuple[float, ...]
    captured_at: tuple[int | None, ...]
    coverage_type: tuple[str | None, ...]
    build_id: tuple[int | None, ...]

    def __len__(self) -> int:
        return len(self.pano_id)


def decode_coverage_tile(body: bytes | None, tile: Tile) -> CoverageTile:
    """Decode one coverage tile body. Pure — no I/O, no clock, no network.

    An absent or empty body decodes to zero panoramas rather than raising: over a
    country sweep "nothing here" is the common answer, and Apple returns an empty
    message for an uncovered tile.

    The panorama positions are relative to the tile coordinate *inside the
    message*, not to the one requested. Those normally agree; when they do not,
    trusting the request would place every panorama in this tile somewhere else
    entirely, so the mismatch raises instead.
    """
    from .proto import apple_coverage_pb2

    if not body:
        return CoverageTile(tile, (), (), (), (), (), ())

    message = apple_coverage_pb2.GroundMetadataTile()
    message.ParseFromString(body)
    if not message.pano:
        return CoverageTile(tile, (), (), (), (), (), ())

    origin = message.tile_coordinate
    if (origin.x, origin.y) != (tile.x, tile.y):
        raise ValueError(
            f"tile {tile.z}/{tile.x}/{tile.y} answered with coordinates "
            f"{origin.z}/{origin.x}/{origin.y}"
        )

    # Resolved once per tile rather than per panorama: a tile holds a few builds
    # and hundreds of panoramas pointing at them.
    builds = [
        (build.build_id or None, COVERAGE_TYPES.get(build.coverage_type))
        for build in message.build_table
    ]

    pano_id: list[int] = []
    lon: list[float] = []
    lat: list[float] = []
    captured_at: list[int | None] = []
    coverage_type: list[str | None] = []
    build_id: list[int | None] = []

    for pano in message.pano:
        position = pano.tile_position
        x, y = offset_to_wgs84(tile, position.x / _SUBPIXEL, position.y / _SUBPIXEL, _EXTENT)
        pano_id.append(pano.panoid)
        lon.append(x)
        lat.append(y)
        captured_at.append(pano.timestamp or None)
        # An index outside the build table means the tile disagrees with itself;
        # the panorama's position is still good, so it is kept unattributed
        # rather than dropped.
        known = builds[pano.build_table_idx] if pano.build_table_idx < len(builds) else (None, None)
        build_id.append(known[0])
        coverage_type.append(known[1])

    return CoverageTile(
        tile=tile,
        pano_id=tuple(pano_id),
        lon=tuple(lon),
        lat=tuple(lat),
        captured_at=tuple(captured_at),
        coverage_type=tuple(coverage_type),
        build_id=tuple(build_id),
    )


def apple_source(bounds: Bounds, *, token: str | None = None) -> TileSource:
    """The coverage endpoint as an ordinary tile source.

    Apple needs no special fetching: the URL is constant and the coordinates
    travel in `maps-tile-x/y/z`, which is what `TileSource.header_template`
    formats. Everything therefore goes through `streetradar.fetch` like every
    other request.

    The token is read from `ENGINE_APPLE_TOKEN` rather than committed. It is
    public — it ships in an MIT library — but it is still Apple's access control,
    the only one guarding this endpoint, and it is static with no published way
    to refresh it. Keeping it in the environment means a rotation is a config
    change on the VPS rather than a rebuild; keeping it out of the repository
    means this public showcase does not republish someone's credential.
    """
    resolved = token or os.environ.get(_TOKEN_VARIABLE)
    if not resolved:
        raise TokenMissing(
            f"{_TOKEN_VARIABLE} is not set; Apple's coverage endpoint returns 403 "
            f"on every route without it"
        )
    return TileSource(
        id="apple",
        url_template=COVERAGE_TILE_URL,
        min_zoom=APPLE_ZOOM,
        max_zoom=APPLE_ZOOM,
        bounds=bounds,
        header_template={
            "maps-tile-style": "style=57&size=2&scale=0&v=0&preflight=2",
            "maps-tile-x": "{x}",
            "maps-tile-y": "{y}",
            "maps-tile-z": "{z}",
            "maps-auth-token": resolved,
        },
    )

"""Mapy.cz coverage, over the one endpoint that is still open.

Seznam runs three doors onto the same panorama data, and only one of them is
ours to use:

    tilecache.mapy.com/panorama_ln_hybrid-m/{z}-{x}-{y}   403  CloudFront signed
                                                               cookies
    api.mapy.cz/v1/panorama/detail|getneighbours          401  API key
    pro.mapy.cz/panorpc  (FRPC)                           200  open

The first two answer only to a browser session or a registered key. Getting past
either would mean replaying someone else's credential, which is a different thing
from ignoring a `robots.txt` convention and is not something this engine does.
The third is the endpoint the previous engine used, it is unauthenticated, and it
is what this module speaks. ADR 0015.

## Why there is no BFS here

The previous run walked the neighbour graph from a single seed. It did not crash:
it terminated with an empty queue after 31.7 hours and 16,227,263 panoramas,
having covered longitudes 12.38 to 15.07 — a quarter of the country. Everything
not connected to its seed was unreachable no matter how long it ran, which is the
same failure Apple's coverage has at 1,843 components.

So discovery is a **flat sweep of the zoom-17 tile grid**, one `getbest` at each
tile centre. Every tile is asked, so connectivity stops mattering: 3,494,322
calls, 39 minutes at the measured rate. Geometry then comes from walking
neighbours out of each hit, bounded to the tile that found it, so a walk cannot
wander the way a global BFS does.

## What the endpoint sustains

Measured, on a ladder of concurrency levels against `pro.mapy.cz/panorpc`:

    concurrency    8     140 req/s     0 errors
                  32     566           0
                  64   1,204           0
                 128   1,864           0      <- plateau
                 256   1,793           0

then held at a paced 1,500 req/s for two minutes: 180,000 requests, **zero
errors**, p50 latency flat at 40 ms from the first ten-second window to the last,
p95 falling slightly from 140 ms to 133 ms. Nothing throttles in this range.

Note what that says about the old run's 142 req/s: it is what concurrency 8
produces here. The previous collector held a `Semaphore(1000)` and still achieved
only that, because it rewrote a 162 MB `visited_ids.json` every 2,000 panoramas.
The ceiling was ours, not Seznam's.

1,500 is used rather than the 1,864 plateau: the margin costs a few hours on a
twelve-hour run and leaves room for retries.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Any, Final, NamedTuple, Protocol

if TYPE_CHECKING:
    from collections.abc import Iterator, Mapping

    from .tiles import Bounds, Tile

__all__ = [
    "CAPTURE_PRECISION",
    "PANORPC_URL",
    "SWEEP_ZOOM",
    "Panorama",
    "Poster",
    "encode_call",
    "getbest",
    "getneighbours",
    "sweep_points",
]


class Poster(Protocol):
    """The whole surface these calls need from a fetcher.

    A protocol rather than `Fetcher` itself, for the reason the harvesters take
    `TileFetcher`: it keeps the calls testable against recorded bytes without a
    session, and it states plainly that FRPC needs exactly one thing the tile
    path does not — a POST with a body.
    """

    async def post(self, url: str, body: bytes, *, headers: Mapping[str, str]) -> bytes: ...


PANORPC_URL: Final = "https://pro.mapy.cz/panorpc"

#: FRPC protocol version, as pyfrpc's own client defaults to.
_FRPC_VERSION: Final = 0x0201
_FRPC_TYPE: Final = "application/x-frpc"

#: Cyclomedia panoramas — everything from 2020 on — are withheld unless the
#: request carries this. Not authentication: the same request without it returns
#: an older panorama rather than an error.
_REFERER: Final = "https://en.mapy.cz/"

#: One `getbest` per zoom-17 tile centre. A tile is 197 m across at Czech
#: latitudes, which is the granularity of the discovery sweep — not of the
#: geometry, which comes from the neighbour walk.
SWEEP_ZOOM: Final = 17

#: Mapy reports `createdAt` as a full timestamp, so nothing is rounded away.
CAPTURE_PRECISION: Final = "second"

_SEARCH_RADIUS_M: Final = 150.0


class Panorama(NamedTuple):
    """One panorama, reduced to what coverage needs."""

    pano_id: int
    lon: float
    lat: float
    captured_at: int | None
    provider: str | None
    #: Years in which this spot was re-driven, as the endpoint reports them. It
    #: arrives with every panorama at no extra request, which is why "all the
    #: dates" costs one call per place rather than one per pass.
    timeline: tuple[int, ...] = ()

    @property
    def has_position(self) -> bool:
        return not (self.lon == 0.0 and self.lat == 0.0)


def headers() -> dict[str, str]:
    return {
        "Content-Type": _FRPC_TYPE,
        "Accept": _FRPC_TYPE,
        "Referer": _REFERER,
    }


def encode_call(method: str, args: tuple[Any, ...]) -> bytes:
    """Encode one FRPC call to the bytes that go on the wire.

    `pyfrpc` is used for the codec and *not* for its client: its `FrpcClient`
    carries its own `requests` session, which would put this one provider outside
    the budget, retries and metrics that every other request passes through.
    Encoding is upstream's problem; the request is ours. Invariant 1.
    """
    import pyfrpc

    return bytes(pyfrpc.encode(pyfrpc.FrpcCall(name=method, args=args), version=_FRPC_VERSION))


def _decode(body: bytes | None) -> Any:
    import pyfrpc

    if not body:
        return None
    answer = pyfrpc.decode(body)
    data = getattr(answer, "data", None)
    if not isinstance(data, dict):
        return None
    return data.get("result")


def _panorama(result: Mapping[str, Any] | None) -> Panorama | None:
    """One `panInfo`-shaped mapping to a `Panorama`, or None when there is none."""
    if not result:
        return None
    info = result.get("panInfo") if "panInfo" in result else result
    if not isinstance(info, dict):
        return None
    mark = info.get("mark") or {}
    pano_id = info.get("pid")
    if pano_id is None:
        return None
    moment = info.get("createdAt")
    return Panorama(
        pano_id=int(pano_id),
        lon=float(mark.get("lon", 0.0)),
        lat=float(mark.get("lat", 0.0)),
        # `createdAt` arrives as an aware datetime from the FRPC codec, so the
        # conversion is exact rather than a parse of a formatted string.
        captured_at=int(moment.timestamp() * 1000) if moment is not None else None,
        provider=info.get("provider"),
        timeline=tuple(int(y) for y in info.get("timeline") or ()),
    )


async def getbest(
    fetcher: Poster, lon: float, lat: float, *, radius: float = _SEARCH_RADIUS_M
) -> Panorama | None:
    """The nearest panorama to a point, or None if there is none within `radius`.

    This is the primitive the whole strategy turns on, because it takes a
    *position* rather than a panorama id. `getneighbours` can only reach what is
    already connected to something known; this can land anywhere on the map, so a
    sweep of it cannot miss a disconnected component.
    """
    body = await fetcher.post(
        PANORPC_URL, encode_call("getbest", (lon, lat, radius, {})), headers=headers()
    )
    return _panorama(_decode(body))


async def getneighbours(fetcher: Poster, pano_id: int) -> list[Panorama]:
    """The panoramas adjacent to one, along the road it sits on.

    Both sides of each entry are taken. Keeping only `near` would halve the
    walk's reach per request and double the number of requests a road costs.
    """
    body = await fetcher.post(
        PANORPC_URL, encode_call("getneighbours", (pano_id, {})), headers=headers()
    )
    result = _decode(body)
    if not isinstance(result, dict):
        return []
    found = []
    for entry in result.get("neighbours") or ():
        if not isinstance(entry, dict):
            continue
        # An entry is {angle, near, far}, not a panorama: `near` is the adjacent
        # capture and `far` the next one along the same bearing, either of which
        # may be an empty dict where the road ends.
        for side in ("near", "far"):
            pano = _panorama(entry.get(side))
            if pano is not None and pano.has_position:
                found.append(pano)
    return found


def sweep_points(bounds: Bounds, zoom: int = SWEEP_ZOOM) -> Iterator[tuple[Tile, float, float]]:
    """Every tile of `bounds` and the WGS84 centre to ask `getbest` about.

    The tile comes back with the point because it is the unit of work: it is what
    a `ProbedGrid` records, which is what makes the sweep resumable after a crash
    without the 162 MB rewrite that throttled the previous engine.
    """
    import math

    from .tiles import Tile

    span = 1 << zoom
    west, south, east, north = bounds

    def to_tile(lon: float, lat: float) -> tuple[int, int]:
        x = int((lon + 180.0) / 360.0 * span)
        y = int((1.0 - math.asinh(math.tan(math.radians(lat))) / math.pi) / 2.0 * span)
        return x, y

    x0, y0 = to_tile(west, north)
    x1, y1 = to_tile(east, south)
    for x in range(max(0, x0), min(span - 1, x1) + 1):
        for y in range(max(0, y0), min(span - 1, y1) + 1):
            # Tile centre rather than corner: a corner sits on the boundary
            # between four tiles and biases every probe the same way.
            lon = (x + 0.5) / span * 360.0 - 180.0
            lat = math.degrees(math.atan(math.sinh(math.pi * (1.0 - 2.0 * (y + 0.5) / span))))
            yield Tile(zoom, x, y), lon, lat

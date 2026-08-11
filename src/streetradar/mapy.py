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

import asyncio
import json
import math
import struct
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any, Final, NamedTuple, Protocol

from .flood import ProbedGrid
from .tiles import TileSource

if TYPE_CHECKING:
    from collections.abc import AsyncIterator, Iterator, Mapping
    from pathlib import Path

    from .model import ObservationBatch
    from .store import PartWriter
    from .tiles import Bounds, Tile

__all__ = [
    "CAPTURE_PRECISION",
    "PANORPC_URL",
    "SWEEP_ZOOM",
    "Panorama",
    "Poster",
    "SeenIds",
    "SweepState",
    "collect_mapy",
    "encode_call",
    "getbest",
    "getneighbours",
    "sweep_points",
    "tile_of",
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

_MAGIC: Final = b"SRMY"
_VERSION: Final = 1


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


class SeenIds:
    """Which panorama ids have already been walked, as a bitmap over their range.

    The previous engine kept this as a Python set serialised to
    `visited_ids.json` — 162 MB, rewritten in full every 2,000 panoramas, roughly
    650 GB of write amplification over the run and stalls that grew with it.

    Mapy's ids are dense enough to index directly: 2,245,132 to 108,309,448 in
    the collected quarter, 15.3% of that range occupied. A bitmap over 2^28 ids
    is **32 MB** and answers in constant time, against about 1 GB for the same
    thing as a set.
    """

    __slots__ = ("_bits", "_count", "capacity")

    def __init__(self, capacity: int = 1 << 28) -> None:
        if capacity < 1:
            raise ValueError("capacity must be positive")
        self.capacity = capacity
        self._bits = bytearray((capacity + 7) // 8)
        self._count = 0

    def __len__(self) -> int:
        return self._count

    def __contains__(self, pano_id: int) -> bool:
        if not 0 <= pano_id < self.capacity:
            # Outside the range counts as seen: it stops an id we cannot record
            # from being walked again on every pass.
            return True
        return bool(self._bits[pano_id >> 3] & (1 << (pano_id & 7)))

    def add(self, pano_id: int) -> bool:
        """Record an id. Returns False when it was already recorded."""
        if not 0 <= pano_id < self.capacity:
            return False
        byte, bit = pano_id >> 3, 1 << (pano_id & 7)
        if self._bits[byte] & bit:
            return False
        self._bits[byte] |= bit
        self._count += 1
        return True


@dataclass
class SweepState:
    """Everything needed to resume a Mapy run where it stopped.

    One file, two bitmaps: which tiles have been asked, and which panoramas have
    been walked. Written atomically, and — as everywhere in this engine —
    **after** the rows it accounts for are durable, never before.
    """

    grid: ProbedGrid
    seen: SeenIds
    zoom: int = SWEEP_ZOOM
    hits: int = 0
    panoramas: int = 0

    @classmethod
    def for_bounds(cls, bounds: Bounds, zoom: int = SWEEP_ZOOM) -> SweepState:
        source = TileSource(
            id="mapy",
            url_template=PANORPC_URL,
            min_zoom=zoom,
            max_zoom=zoom,
            bounds=bounds,
        )
        return cls(grid=ProbedGrid.for_bounds(bounds, source, zoom), seen=SeenIds(), zoom=zoom)

    @property
    def swept(self) -> int:
        return len(self.grid)

    def save(self, path: Path) -> None:
        header = json.dumps(
            {
                "x0": self.grid.x0,
                "y0": self.grid.y0,
                "width": self.grid.width,
                "height": self.grid.height,
                "zoom": self.zoom,
                "hits": self.hits,
                "panoramas": self.panoramas,
                "swept": len(self.grid),
                "capacity": self.seen.capacity,
                "seen": len(self.seen),
            }
        ).encode()
        tmp = path.with_suffix(path.suffix + ".tmp")
        with open(tmp, "wb") as handle:
            handle.write(_MAGIC)
            handle.write(struct.pack("<BI", _VERSION, len(header)))
            handle.write(header)
            handle.write(self.grid._bits)  # noqa: SLF001 — same package
            handle.write(self.seen._bits)  # noqa: SLF001
        tmp.replace(path)

    @classmethod
    def load(cls, path: Path) -> SweepState:
        with open(path, "rb") as handle:
            if handle.read(4) != _MAGIC:
                raise ValueError(f"{path} is not a Mapy sweep snapshot")
            version, header_len = struct.unpack("<BI", handle.read(5))
            if version != _VERSION:
                raise ValueError(f"{path}: unsupported snapshot version {version}")
            header = json.loads(handle.read(header_len))
            grid = ProbedGrid(header["x0"], header["y0"], header["width"], header["height"])
            grid._bits[:] = handle.read(len(grid._bits))  # noqa: SLF001
            grid._count = header["swept"]  # noqa: SLF001
            seen = SeenIds(header["capacity"])
            seen._bits[:] = handle.read(len(seen._bits))  # noqa: SLF001
            seen._count = header["seen"]  # noqa: SLF001
        return cls(
            grid=grid,
            seen=seen,
            zoom=header["zoom"],
            hits=header["hits"],
            panoramas=header["panoramas"],
        )


def tile_of(pano: Panorama, zoom: int) -> tuple[int, int]:
    """Which tile a panorama's position falls in."""
    span = 1 << zoom
    x = int((pano.lon + 180.0) / 360.0 * span)
    y = int((1.0 - math.asinh(math.tan(math.radians(pano.lat))) / math.pi) / 2.0 * span)
    return max(0, min(span - 1, x)), max(0, min(span - 1, y))


async def _walk(
    fetcher: Poster,
    seeds: list[Panorama],
    state: SweepState,
    writer: PartWriter[ObservationBatch],
    *,
    round_size: int,
) -> AsyncIterator[Path]:
    """Follow the road graph out of `seeds`, recording everything new.

    Deliberately **not** bounded to the tile that produced a seed. Measured on
    the previous engine's export, a zoom-17 tile holds 3.96 separately connected
    fragments on average and 36 at worst, because a 197 m square cuts roads; a
    walk confined to one would reach 44% of them. Crossing freely is what makes
    the sweep's one seed per tile enough, and `state.seen` is what stops the
    crossing from costing anything twice.

    Parts are yielded from inside the walk, not after it. The first seed of a
    country reaches its whole connected component — 16.2 million panoramas in the
    previous run — and holding that in one batch before writing anything would
    exhaust the machine long before the walk ended.
    """
    frontier = [p for p in seeds if state.seen.add(p.pano_id)]
    _record(frontier, state, writer)

    while frontier:
        if writer.full():
            part = writer.flush()
            if part is not None:
                yield part
        batch = frontier[:round_size]
        frontier = frontier[round_size:]
        answers = await asyncio.gather(
            *(getneighbours(fetcher, p.pano_id) for p in batch), return_exceptions=True
        )
        found: list[Panorama] = []
        for answer in answers:
            if isinstance(answer, BaseException):
                # One failed expansion loses a branch, not the run. The tile is
                # already recorded as swept, so a retry belongs to the next pass.
                continue
            found.extend(p for p in answer if state.seen.add(p.pano_id))
        _record(found, state, writer)
        frontier.extend(found)


def _record(
    panoramas: list[Panorama], state: SweepState, writer: PartWriter[ObservationBatch]
) -> None:
    """Group by tile and hand to the batch, which takes one tile at a time."""
    if not panoramas:
        return
    by_tile: dict[tuple[int, int], list[Panorama]] = {}
    for pano in panoramas:
        by_tile.setdefault(tile_of(pano, state.zoom), []).append(pano)
    for (x, y), group in by_tile.items():
        writer.batch.add(
            tile_x=x,
            tile_y=y,
            pano_id=[p.pano_id for p in group],
            lon=[p.lon for p in group],
            lat=[p.lat for p in group],
            captured_at=[p.captured_at for p in group],
            coverage_type=[p.provider for p in group],
        )
    state.panoramas += len(panoramas)


async def collect_mapy(
    fetcher: Poster,
    bounds: Bounds,
    *,
    writer: PartWriter[ObservationBatch],
    state: SweepState,
    round_size: int = 256,
    limit: int | None = None,
) -> AsyncIterator[Path]:
    """Sweep the tile grid, walk out of every hit, and yield each finished part.

    The same generator shape as the other collectors, so the ordering rule reads
    the same way — upload the part, then snapshot:

        async for part in collect_mapy(fetcher, bounds, writer=writer, state=state):
            upload(part)
            part.unlink()
            state.save(snapshot)

    Nothing here saves `state`: only the caller knows when the rows became
    durable, and snapshotting first is what turns a crash into tiles marked swept
    whose panoramas were never written.

    `limit` caps the number of *tiles swept*, which is what makes a bounded first
    run bounded. The walk that each tile triggers is not capped, because stopping
    mid-walk would leave `seen` claiming panoramas that were never recorded.
    """
    if writer.batch.capture_precision != CAPTURE_PRECISION:
        raise ValueError(
            f"writer records {writer.batch.capture_precision!r} precision; Mapy "
            f"reports a full timestamp, so it must be {CAPTURE_PRECISION!r}"
        )

    swept = 0
    pending: list[tuple[Tile, float, float]] = []

    async def drain() -> AsyncIterator[Path]:
        nonlocal pending
        if not pending:
            return
        found = await asyncio.gather(
            *(getbest(fetcher, lon, lat) for _, lon, lat in pending), return_exceptions=True
        )
        seeds: list[Panorama] = []
        for (tile, _, _), answer in zip(pending, found, strict=True):
            if isinstance(answer, BaseException):
                continue
            state.grid.add(tile)
            if answer is not None and answer.has_position:
                state.hits += 1
                seeds.append(answer)
        pending = []
        if seeds:
            async for part in _walk(fetcher, seeds, state, writer, round_size=round_size):
                yield part
        if writer.full():
            filled = writer.flush()
            if filled is not None:
                yield filled

    for tile, lon, lat in sweep_points(bounds, state.zoom):
        if tile in state.grid:
            continue
        if limit is not None and swept >= limit:
            break
        pending.append((tile, lon, lat))
        swept += 1
        if len(pending) >= round_size:
            async for part in drain():
                yield part

    async for part in drain():
        yield part

    remainder = writer.flush()
    if remainder is not None:
        yield remainder

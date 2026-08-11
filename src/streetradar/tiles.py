"""Tile pyramids: addressing, enumeration, and harvesting.

The shape this generalises is "a provider publishes coverage as a z/x/y pyramid",
which is how Já, Naver and Apple all work. Only three things differ between them,
and all three are data rather than code:

* the URL template — Já and Naver put z/x/y in the path;
* the headers — Apple puts the tile coordinates in `maps-tile-x/y/z` instead,
  which `header_template` covers;
* the zoom range and the area of interest.

Two harvest strategies are provided. `harvest_flat` sweeps one zoom over a
bounding box, which is what the previous engine did for Naver: simple, and it
spends a request on every tile in the box whether or not it holds anything —
measured at 66% empty for Korea. `harvest_pyramid` descends instead, spending a
request at zoom z+1 only where zoom z returned content. On a published pyramid
that already exists, the descent is free to use and strictly cheaper.
"""

from __future__ import annotations

import asyncio
import math
from collections.abc import AsyncIterator, Callable, Iterator, Mapping, Sequence
from dataclasses import dataclass, field
from typing import NamedTuple, Protocol

__all__ = [
    "Bounds",
    "Tile",
    "TileFetcher",
    "TileResult",
    "TileSource",
    "harvest_flat",
    "harvest_pyramid",
]


class TileFetcher(Protocol):
    """The whole surface the harvesters need from a fetcher.

    Deliberately two members wide: `streetradar.fetch.Fetcher` satisfies it, and
    so does a dozen-line stub, which is what lets the harvest logic be tested
    without a network.
    """

    @property
    def concurrency(self) -> int: ...

    async def fetch_tile(self, url: str, headers: Mapping[str, str]) -> bytes | None: ...


#: (west, south, east, north) in WGS84 degrees.
Bounds = tuple[float, float, float, float]


class Tile(NamedTuple):
    """A slippy-map tile address."""

    z: int
    x: int
    y: int

    def __str__(self) -> str:
        return f"{self.z}/{self.x}/{self.y}"

    @property
    def children(self) -> tuple[Tile, Tile, Tile, Tile]:
        z, x, y = self.z + 1, self.x * 2, self.y * 2
        return (
            Tile(z, x, y),
            Tile(z, x + 1, y),
            Tile(z, x, y + 1),
            Tile(z, x + 1, y + 1),
        )

    @property
    def parent(self) -> Tile:
        return Tile(self.z - 1, self.x // 2, self.y // 2)

    @property
    def bounds(self) -> Bounds:
        """WGS84 bounds of this tile, as (west, south, east, north)."""
        north, west = _num2deg(self.x, self.y, self.z)
        south, east = _num2deg(self.x + 1, self.y + 1, self.z)
        return (west, south, east, north)

    def ground_size_m(self) -> float:
        """Approximate side length in metres at this tile's latitude.

        Useful for sanity-checking a target zoom against the feature size you
        care about: at z14 in Iceland a tile is about 1.05 km across.
        """
        _, south, _, north = self.bounds
        lat = math.radians((south + north) / 2.0)
        # 2.0** rather than 2**: the latter widens to Any under mypy strict.
        return 40_075_016.686 / (2.0**self.z) * math.cos(lat)


class TileResult(NamedTuple):
    """One harvested tile and its body. `body` is None when the tile is absent."""

    tile: Tile
    body: bytes | None

    @property
    def has_content(self) -> bool:
        return bool(self.body)


@dataclass(frozen=True)
class TileSource:
    """Where a provider's tile pyramid lives and how to address it."""

    id: str
    url_template: str
    min_zoom: int
    max_zoom: int
    bounds: Bounds
    #: Header values are formatted with z/x/y too, which is how Apple's
    #: coverage endpoint takes its coordinates.
    header_template: Mapping[str, str] = field(default_factory=dict)

    def __post_init__(self) -> None:
        if not self.min_zoom <= self.max_zoom:
            raise ValueError(f"{self.id}: min_zoom must not exceed max_zoom")
        if self.min_zoom < 0:
            raise ValueError(f"{self.id}: min_zoom must not be negative")
        west, south, east, north = self.bounds
        if not (west < east and south < north):
            raise ValueError(f"{self.id}: bounds must be (west, south, east, north)")

    def url(self, tile: Tile) -> str:
        return self.url_template.format(z=tile.z, x=tile.x, y=tile.y)

    def headers(self, tile: Tile) -> dict[str, str]:
        return {k: v.format(z=tile.z, x=tile.x, y=tile.y) for k, v in self.header_template.items()}

    def tile_range(self, zoom: int) -> tuple[int, int, int, int]:
        """The half-open tile box `bounds` covers at `zoom`, as (x0, y0, x1, y1).

        Clamped to the world. Public because a caller that wants a *subset* of
        the box — a sampled lattice, a shard — must not have to enumerate it
        first: Australia at zoom 17 is on the order of 10^8 tiles.
        """
        self._check_zoom(zoom)
        west, south, east, north = self.bounds
        x0, y0 = _deg2num(north, west, zoom)
        x1, y1 = _deg2num(south, east, zoom)
        # `1 << zoom` rather than `2**zoom`: int ** int widens to Any under
        # mypy strict, and the tile span is an integer count.
        span = 1 << zoom
        return max(0, x0), max(0, y0), min(span - 1, x1) + 1, min(span - 1, y1) + 1

    def tiles_at(self, zoom: int) -> Iterator[Tile]:
        """Every tile of `bounds` at `zoom`, row-major.

        Yields lazily: a country at z14 is ~10^5 tiles and there is no reason to
        hold them all in memory.
        """
        x0, y0, x1, y1 = self.tile_range(zoom)
        for x in range(x0, x1):
            for y in range(y0, y1):
                yield Tile(zoom, x, y)

    def tile_count_at(self, zoom: int) -> int:
        """How many tiles a flat sweep at `zoom` would request."""
        x0, y0, x1, y1 = self.tile_range(zoom)
        return max(0, x1 - x0) * max(0, y1 - y0)

    def _check_zoom(self, zoom: int) -> None:
        if not self.min_zoom <= zoom <= self.max_zoom:
            raise ValueError(
                f"{self.id}: zoom {zoom} outside published range {self.min_zoom}-{self.max_zoom}"
            )


async def harvest_flat(
    fetcher: TileFetcher,
    source: TileSource,
    zoom: int,
    *,
    limit: int | None = None,
) -> AsyncIterator[TileResult]:
    """Sweep one zoom over the source's bounds, yielding results as they land.

    `limit` caps the number of tiles requested, which is what makes a bounded
    probe bounded.
    """
    tiles = source.tiles_at(zoom)
    async for result in _fetch_many(fetcher, source, tiles, limit=limit):
        yield result


async def harvest_pyramid(
    fetcher: TileFetcher,
    source: TileSource,
    *,
    target_zoom: int,
    start_zoom: int | None = None,
    has_content: Callable[[bytes | None], bool] | None = None,
    limit_per_level: int | None = None,
) -> AsyncIterator[TileResult]:
    """Descend the pyramid, spending requests only where content exists.

    Each level is a barrier: the candidates at z+1 are the children of the tiles
    that returned content at z. That is inherent to the strategy, not an
    implementation shortcut.

    Only results at `target_zoom` carry the geometry you want; the levels above
    are the mask that got you there, and they are yielded too so a caller can
    count them and report what the descent cost.
    """
    predicate = has_content or (lambda body: bool(body))
    z = start_zoom if start_zoom is not None else source.min_zoom
    source._check_zoom(z)  # noqa: SLF001 — same module, deliberate
    source._check_zoom(target_zoom)  # noqa: SLF001
    if z > target_zoom:
        raise ValueError(f"start_zoom {z} is deeper than target_zoom {target_zoom}")

    candidates: Sequence[Tile] = list(source.tiles_at(z))

    while True:
        survivors: list[Tile] = []
        async for result in _fetch_many(fetcher, source, iter(candidates), limit=limit_per_level):
            yield result
            if predicate(result.body):
                survivors.append(result.tile)

        if z >= target_zoom or not survivors:
            return

        z += 1
        # Children must be filtered back to the area of interest. Tile edges do
        # not align with a bounding box, so the descendants of the tiles that
        # merely *overlap* the box cover a strictly larger area — measured on the
        # capital region, an unfiltered descent collected 18% more road length
        # than a flat sweep of the same box, all of it outside the box.
        candidates = [
            child
            for tile in survivors
            for child in tile.children
            if _intersects(child, source.bounds)
        ]


async def _fetch_many(
    fetcher: TileFetcher,
    source: TileSource,
    tiles: Iterator[Tile],
    *,
    limit: int | None,
) -> AsyncIterator[TileResult]:
    """Fetch tiles with a fixed worker pool, yielding results as they complete.

    A worker pool rather than `gather` over the whole iterator: memory stays flat
    whether there are 40 tiles or 160,000. Concurrency itself is bounded by the
    Fetcher's semaphore, so the pool size here only needs to be large enough to
    keep it saturated.
    """
    workers = fetcher.concurrency
    queue: asyncio.Queue[TileResult] = asyncio.Queue(maxsize=workers * 4)
    sentinel = object()
    remaining = limit
    lock = asyncio.Lock()

    async def next_tile() -> Tile | None:
        nonlocal remaining
        async with lock:
            if remaining is not None and remaining <= 0:
                return None
            tile = next(tiles, None)
            if tile is None:
                return None
            if remaining is not None:
                remaining -= 1
            return tile

    async def worker() -> None:
        while True:
            tile = await next_tile()
            if tile is None:
                return
            body = await fetcher.fetch_tile(source.url(tile), source.headers(tile))
            await queue.put(TileResult(tile, body))

    async def supervise() -> None:
        try:
            await asyncio.gather(*(worker() for _ in range(workers)))
        finally:
            await queue.put(sentinel)  # type: ignore[arg-type]

    task = asyncio.create_task(supervise())
    try:
        while True:
            item = await queue.get()
            if item is sentinel:
                break
            yield item
        await task
    finally:
        if not task.done():
            task.cancel()


def _intersects(tile: Tile, bounds: Bounds) -> bool:
    """Whether a tile overlaps a bounding box, touching edges excluded."""
    t_west, t_south, t_east, t_north = tile.bounds
    west, south, east, north = bounds
    return not (t_east <= west or t_west >= east or t_north <= south or t_south >= north)


def offset_to_wgs84(tile: Tile, x: float, y: float, extent: float) -> tuple[float, float]:
    """A position inside a tile, as (lon, lat).

    `extent` is the tile's own coordinate grid and `y` points north, so the
    Mercator formula — which wants it pointing south — gets `extent - y`.

    Shared because two providers need it from different wire formats: MVT gives
    tile-local integers over an extent of 4096, and Apple gives sixty-fourths of
    a pixel over an extent of 255. Only the numbers differ; both are slippy-map
    tiles, and getting the latitude wrong is the same mistake in both.
    """
    span = float(1 << tile.z) * extent
    world_x = (tile.x * extent + x) / span
    world_y = (tile.y * extent + (extent - y)) / span
    lon = world_x * 360.0 - 180.0
    lat = math.degrees(math.atan(math.sinh(math.pi * (1.0 - 2.0 * world_y))))
    return (lon, lat)


def _deg2num(lat: float, lon: float, zoom: int) -> tuple[int, int]:
    n = 2.0**zoom
    x = int((lon + 180.0) / 360.0 * n)
    y = int((1.0 - math.asinh(math.tan(math.radians(lat))) / math.pi) / 2.0 * n)
    return x, y


def _num2deg(x: int, y: int, zoom: int) -> tuple[float, float]:
    """North-west corner of a tile, as (lat, lon)."""
    n = 2.0**zoom
    lon = x / n * 360.0 - 180.0
    lat = math.degrees(math.atan(math.sinh(math.pi * (1.0 - 2.0 * y / n))))
    return lat, lon

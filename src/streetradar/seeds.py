"""Where a flood starts.

A pyramid descent needs no seeds — it starts at the top and prunes. A flood only
finds what is connected to something it already knows, so for Apple the seed set
*is* half the enumeration strategy, and the recall it produces is a measured
quantity rather than a property of the algorithm.

## Coverage is not one component

Measured at zoom 17 on the previous engine's exports:

    Luxembourg        22 components   largest 99.57%
    Greece            83              largest 77.16%
    Japan             59              largest 62.42%
    Australia      1,843              largest 62.90%

Australia is the case that settles it: 1,843 components, and even the fifty
largest reach only 84.98%. No hand-picked seed list covers a country.

## Seeding from the previous run, measured honestly

The natural seed set is every tile the last run found. To measure what that
actually recovers, the exports are cut by capture date — coverage that existed
before the cut plays the part of the previous run — and the flood is asked to
find what appeared after it.

                          flood alone   with the sampling net
    Japan, 1-year gap          90.79%                  99.92%
    Australia, 1-year gap      99.79%                  99.91%
    Japan, 4-year gap          68.17%                  99.84%
    Australia, 5-year gap      86.44%                  90.91%

Two things follow, and neither is what ADR 0012 assumed.

**The flood alone is not complete, and the shortfall is large.** Japan loses
9.2% of a year's new coverage — one new tile in eleven — because new coverage
appears in places touching nothing already known. ADR 0012's "100% recall"
measured recovery of the *old* coverage from itself, which a seed set drawn from
that same coverage returns trivially.

**The sampling net is what makes it work, and it has a shelf life.** Over one
year both countries hold above 99.9%. Over five, Australia falls to 90.91%: the
misses fragment into 499 components with a median of 3 tiles, and a one-in-1024
sample steps straight over them. The baseline cadence is therefore annual — an
operational parameter ADR 0012 left open, and it is not free to choose.
"""

from __future__ import annotations

from typing import TYPE_CHECKING

from .tiles import Tile, TileSource

if TYPE_CHECKING:
    from collections.abc import Iterator
    from pathlib import Path

__all__ = ["SAMPLING_STEP", "sampling_seeds", "seeds_from_parts"]

#: One tile in 32 on each axis — one in 1,024. Measured above: this is what
#: rescues a year's disconnected coverage. Larger steps step over the small
#: components that are most of the misses; smaller ones cost more than the flood.
SAMPLING_STEP = 32


def seeds_from_parts(directory: Path, zoom: int) -> Iterator[Tile]:
    """Every tile a previous run wrote, deduplicated, in a stable order.

    This is the maintenance regime's seed set. Pair it with `sampling_seeds`:
    on its own it misses whatever appeared with no connection to what was already
    known, which is 9.2% of a year's new coverage in Japan.

    Held in memory as packed integers — 1.3M tiles for Australia, the largest
    country measured. A world-sized set would want the flood's own bitmap
    instead, but nothing collects at that scale in one run.
    """
    import pyarrow.parquet as pq

    span = 1 << zoom
    seen: set[int] = set()
    for path in sorted(directory.glob("*.parquet")):
        for batch in pq.ParquetFile(path).iter_batches(
            batch_size=1_000_000, columns=["tile_x", "tile_y"]
        ):
            for x, y in zip(
                batch.column("tile_x").to_pylist(),
                batch.column("tile_y").to_pylist(),
                strict=True,
            ):
                key = x * span + y
                if key not in seen:
                    seen.add(key)
                    yield Tile(zoom, x, y)


def sampling_seeds(source: TileSource, zoom: int, *, step: int = SAMPLING_STEP) -> Iterator[Tile]:
    """A regular sample of the source's box, as the net under the flood.

    Anchored to the tile grid rather than to the box, so two runs of the same
    country sample the same tiles and a resumed flood does not re-probe a
    shifted lattice.

    Finds any new region large enough to contain a sampled tile. What it misses
    is small and scattered: over a five-year gap on Australia the misses break
    into 499 components with a median of 3 tiles, and only 32.92% are recovered.
    Over one year, 99.91%.
    """
    if step < 1:
        raise ValueError("step must be positive")
    x0, y0, x1, y1 = source.tile_range(zoom)
    # The lattice is computed, not filtered out of the full box: Australia at
    # zoom 17 is ~10^8 tiles and only ~10^5 of them are sampled.
    for x in range(x0 + (-x0) % step, x1, step):
        for y in range(y0 + (-y0) % step, y1, step):
            yield Tile(zoom, x, y)

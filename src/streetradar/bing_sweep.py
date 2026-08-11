"""The certified-disc sweep over Streetside's lossy endpoint.

`streetradar.bing` established that a call returns roughly the nearest bubbles to
a point but drops some near the rim of what it returns — reliably complete only
within `CERTIFY_FRACTION` of the farthest-returned distance. This turns that fact
into a complete enumeration.

## The algorithm

A quadtree over the target box. For each cell:

- probe its centre; let `r` be the farthest-returned distance;
- **certify** the cell done when the inner disc — radius `CERTIFY_FRACTION * r` —
  covers it, i.e. that radius reaches the cell's corners;
- **prune** the cell when the probe is empty (nothing near this centre);
- otherwise **subdivide** into four and recurse.

Because every certified disc is (measured) omission-free and the certified discs
tile the box, their union is complete — not by statistical overlap but because
each piece is individually complete. Saturated dense cells have a small `r`, so
their certified disc is small, so they subdivide until the pieces are small
enough to be clean; sparse cells have a large `r` and certify in one probe.

## The audit rides along for free

Every probe is a completeness experiment: a known bubble inside a call's
certified disc that the call did not return is a downward-closure violation, with
a witness. `audit` runs that check across the whole sweep. Zero violations over
`V` pairs bounds the omission rate at ~`3/V`. This is what makes the completeness
*measured* rather than assumed — the endpoint is lossy, so there is no proof,
only a bound the sweep tightens as it runs. ADR 0016.

## What this does not decide

Resumability, deduplication and writing belong to the collector that drives this
(`streetradar.bing_collect`). A `sweep` over one box is bounded and in-memory; a
country is a grid of boxes, each swept and marked done.
"""

from __future__ import annotations

import math
from collections.abc import AsyncIterator, Awaitable, Callable, Iterable, Sequence
from dataclasses import dataclass
from typing import TYPE_CHECKING

from .bing import CERTIFY_FRACTION

if TYPE_CHECKING:
    from .bing import Bubble
    from .tiles import Bounds

__all__ = [
    "Cell",
    "Probe",
    "Result",
    "audit",
    "certified_radius",
    "sweep",
]

#: A probe answers "the bubbles near this point", which is `getbubbles` bound to
#: a fetcher. Taking the function rather than a fetcher keeps the sweep testable
#: against a synthetic point field with controllable rim loss.
Probe = Callable[[float, float], Awaitable[Sequence["Bubble"]]]

#: A cell never subdivides below this side, in metres. A cell still uncertified
#: here holds more than the cap can separate at one point — an irreducible stack,
#: counted and disclosed rather than silently dropped.
DEFAULT_FLOOR_M = 40.0


@dataclass(frozen=True)
class Cell:
    """A box in the quadtree, as (north, south, east, west) degrees."""

    north: float
    south: float
    east: float
    west: float

    @property
    def centre(self) -> tuple[float, float]:
        return (self.north + self.south) / 2, (self.east + self.west) / 2

    @property
    def half_diagonal_m(self) -> float:
        lat, lon = self.centre
        return _haversine(lat, lon, self.north, self.east)

    @property
    def side_m(self) -> float:
        return _haversine(self.north, self.west, self.north, self.east)

    def quarters(self) -> tuple[Cell, Cell, Cell, Cell]:
        mlat, mlon = self.centre
        return (
            Cell(self.north, mlat, mlon, self.west),
            Cell(self.north, mlat, self.east, mlon),
            Cell(mlat, self.south, mlon, self.west),
            Cell(mlat, self.south, self.east, mlon),
        )


@dataclass
class Result:
    """One probe: where it was, how far it reached, and what it returned.

    Kept for the audit, which needs the centre, the radius and the ids. The
    bubbles themselves flow to the writer.
    """

    lat: float
    lon: float
    r: float
    bubbles: Sequence[Bubble]
    saturated: bool

    @property
    def ids(self) -> frozenset[int]:
        return frozenset(b.id for b in self.bubbles)


def _haversine(lat1: float, lon1: float, lat2: float, lon2: float) -> float:
    r = 6_371_000.0
    p1, p2 = math.radians(lat1), math.radians(lat2)
    dp = math.radians(lat2 - lat1)
    dl = math.radians(lon2 - lon1)
    a = math.sin(dp / 2) ** 2 + math.cos(p1) * math.cos(p2) * math.sin(dl / 2) ** 2
    return 2 * r * math.asin(math.sqrt(min(1.0, a)))


def certified_radius(r: float) -> float:
    """The radius a probe of reach `r` is trusted to cover without omission."""
    return CERTIFY_FRACTION * r


async def sweep(
    probe: Probe,
    bounds: Bounds,
    *,
    cap: int,
    floor_m: float = DEFAULT_FLOOR_M,
    concurrency: int = 16,
) -> AsyncIterator[Result]:
    """Enumerate one box, yielding each probe's result.

    Yields every probe — including empty and floored ones — so the caller can
    both collect bubbles and audit. A cell is certified (not subdivided) when its
    inner disc covers it; empty when the probe returns nothing; floored when it is
    already at `floor_m` and still not certified, which is logged via a saturated
    result whose disc did not reach the corners.

    `cap` is the endpoint's return ceiling (`bing.MAX_COUNT`); a response at the
    cap is flagged saturated, which is how a caller spots an irreducible stack at
    the floor.
    """
    west, south, east, north = bounds
    frontier: list[Cell] = [Cell(north, south, east, west)]

    while frontier:
        batch, frontier = frontier[:concurrency], frontier[concurrency:]
        probed = await _probe_batch(probe, batch)
        for cell, bubbles in zip(batch, probed, strict=True):
            clat, clon = cell.centre
            r = max((_haversine(clat, clon, b.lat, b.lon) for b in bubbles), default=0.0)
            result = Result(clat, clon, r, bubbles, saturated=len(bubbles) >= cap)
            yield result

            if not bubbles:
                continue  # empty: nothing near this centre
            if certified_radius(r) >= cell.half_diagonal_m:
                continue  # certified: the inner disc covers the cell
            if cell.side_m <= floor_m:
                continue  # floored: an irreducible stack, flagged by result.saturated
            frontier.extend(cell.quarters())


async def _probe_batch(probe: Probe, cells: Sequence[Cell]) -> list[Sequence[Bubble]]:
    import asyncio

    return list(await asyncio.gather(*(probe(*cell.centre) for cell in cells)))


def audit(results: Iterable[Result], bubbles: Iterable[Bubble]) -> tuple[int, int]:
    """Count downward-closure violations against the certified radius.

    A violation is a bubble inside a result's *certified* disc — the radius we
    trust — that the result did not return. Returns (violations, pairs_checked).
    Zero violations bounds the omission rate at about 3/pairs; a non-zero count
    means `CERTIFY_FRACTION` is too generous for this data and must shrink.

    O(results x bubbles): fine for one box's worth, which is all it is ever handed
    — a country audits box by box.
    """
    points = [(b.id, b.lat, b.lon) for b in bubbles]
    violations = pairs = 0
    for res in results:
        rc = certified_radius(res.r)
        if rc <= 0:
            continue
        for bid, la, lo in points:
            if _haversine(res.lat, res.lon, la, lo) < rc:
                pairs += 1
                if bid not in res.ids:
                    violations += 1
    return violations, pairs

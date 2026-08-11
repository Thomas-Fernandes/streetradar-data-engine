"""Bing Streetside coverage, over the bubble-metadata endpoint.

Microsoft is winding Bing Maps for Enterprise down, so the aim here is to take
all of Streetside while it is still served. The endpoint is the one the public
Streetside viewer calls:

    https://t.ssl.ak.tiles.virtualearth.net/tiles/cmd/StreetSideBubbleMetaData
      ?count=2048&north=&south=&east=&west=

It answers with `text/plain` JSON: element [0] is a header (`{"elapsed": ...}`),
the rest are bubbles. Each bubble carries its position, its capture date to the
second, and — the reason coverage lines fall out for free — a link to its
predecessor and successor along the drive it belongs to.

## It is not a bounding box, and it is not exact

Two measured facts shape everything downstream, and both were surprises:

**The window only supplies a centre.** Growing the requested box from 4 km to
668 km around a fixed centre returned the same set clustered near it. So a call
is really "the ~2048 bubbles nearest this point", and `count` caps at 2048.

**The response is lossy toward its own rim.** Measured against an oversampled
ground truth in three areas, the bubbles a call omits are concentrated near the
edge of what it returns: zero omissions in the inner core, rising to 15–80% at
the outer edge. So the farthest-returned distance `r` overstates the radius the
call actually covers. The reliable radius is `f · r`, with `f ≈ 0.53` in dense
city, `0.44` suburban, `0.30` rural — worst where responses are *unsaturated*,
which rules out the 2048 cap as the sole cause and points at a cell-based
backend that drops bubbles at the edge of what it gathers.

This module therefore certifies only the inner `CERTIFY_FRACTION · r` disc of a
call (see `streetradar.bing_sweep`), and the completeness it can offer is
**measured, not proven**: no call returns a clean k-nearest, so exact 100% is
not obtainable from this endpoint. What is obtainable — a measured omission bound
driven arbitrarily low by overlap, audited on every run — is documented in
ADR 0016.
"""

from __future__ import annotations

import datetime
import json
from typing import TYPE_CHECKING, Final, NamedTuple, Protocol

if TYPE_CHECKING:
    from collections.abc import Iterable, Iterator

__all__ = [
    "BUBBLE_ENDPOINT",
    "CAPTURE_PRECISION",
    "CERTIFY_FRACTION",
    "MAX_COUNT",
    "Bubble",
    "Getter",
    "bubble_url",
    "chains",
    "getbubbles",
    "parse_bubbles",
]

BUBBLE_ENDPOINT: Final = (
    "https://t.ssl.ak.tiles.virtualearth.net/tiles/cmd/StreetSideBubbleMetaData"
)

#: The endpoint truncates any request above this, so it is the working ceiling.
MAX_COUNT: Final = 2048

#: Fraction of the farthest-returned distance that a single call actually covers
#: without omission. Measured: 0.30 rural, 0.44 suburban, 0.53 dense. The global
#: value is the worst of the three, because a sweep must be safe everywhere and
#: the cost of being conservative is only more calls — which is the trade the
#: owner chose. It is a measured floor from a finite sample, not a proof; the
#: run-time audit is what confirms it holds. ADR 0016.
CERTIFY_FRACTION: Final = 0.30

#: Streetside reports a full timestamp, so nothing is rounded away.
CAPTURE_PRECISION: Final = "second"

#: The date field is like "4/24/2021 3:03:57 PM". Month-first, 12-hour clock.
_DATE_FORMAT: Final = "%m/%d/%Y %I:%M:%S %p"


class Bubble(NamedTuple):
    """One Streetside bubble, reduced to what coverage needs.

    `prev`/`next` are the ids of the neighbouring bubbles along the same drive.
    They are what turn a cloud of points into lines without a second request:
    89% of the time they are simply `id ∓ 1`, but they are followed explicitly
    rather than inferred, because the other 11% are where a drive actually turns
    or restarts.
    """

    id: int
    lon: float
    lat: float
    captured_at: int | None
    prev: int | None
    next: int | None


class Getter(Protocol):
    """The single method these calls need from a fetcher.

    A protocol, like the other providers': it keeps parsing testable against
    recorded bytes, and states that Streetside needs only a plain GET — the
    endpoint is effectively a static file server keyed by a query string.
    """

    async def get(self, url: str, *, missing_ok: bool = ...) -> bytes | None: ...


def bubble_url(lon: float, lat: float, *, half: float = 0.05, count: int = MAX_COUNT) -> str:
    """The endpoint URL for a call centred on `(lon, lat)`.

    `half` is the requested half-window in degrees. It only supplies a centre,
    but it must be *larger* than the call's natural reach or the response is
    truncated to the window — so the default is generous. The centre is
    `(west+east)/2, (north+south)/2`, which is why the window is symmetric.

    Coordinates are formatted to six decimals — about 11 cm, finer than any
    bubble spacing — so the URL is stable and free of float noise, which also
    lets a cache key on it.
    """
    return (
        f"{BUBBLE_ENDPOINT}?count={count}"
        f"&north={lat + half:.6f}&south={lat - half:.6f}"
        f"&east={lon + half:.6f}&west={lon - half:.6f}"
    )


def _parse_date(raw: str) -> int | None:
    """`"4/24/2021 3:03:57 PM"` to a millisecond UTC timestamp.

    Treated as UTC: Microsoft does not say, and coverage only needs month
    precision on the map, so a few hours either way never crosses the boundary
    the front end draws. Recorded here so the assumption is visible.
    """
    if not raw:
        return None
    try:
        moment = datetime.datetime.strptime(raw, _DATE_FORMAT).replace(tzinfo=datetime.UTC)
    except ValueError:
        return None
    return int(moment.timestamp() * 1000)


def parse_bubbles(body: bytes | None) -> list[Bubble]:
    """Decode a response body into bubbles. Pure — no I/O, no clock.

    An absent or empty body is zero bubbles, not an error: over a sweep most
    calls in empty regions return nothing, and the endpoint answers a
    no-coverage point with an empty list.

    Element [0] is the `{"elapsed": ...}` header and is dropped. A record missing
    a position is skipped rather than placed at (0, 0) in the Gulf of Guinea.
    """
    if not body:
        return []
    try:
        raw = json.loads(body)
    except (json.JSONDecodeError, ValueError):
        return []
    if not isinstance(raw, list) or len(raw) < 2:
        return []

    bubbles: list[Bubble] = []
    for record in raw[1:]:
        if not isinstance(record, dict) or "la" not in record or "lo" not in record:
            continue
        identifier = record.get("id")
        if identifier is None:
            continue
        bubbles.append(
            Bubble(
                id=int(identifier),
                lon=float(record["lo"]),
                lat=float(record["la"]),
                captured_at=_parse_date(record.get("cd", "")),
                prev=record.get("pr") or None,
                next=record.get("ne") or None,
            )
        )
    return bubbles


async def getbubbles(
    fetcher: Getter, lon: float, lat: float, *, half: float = 0.05
) -> list[Bubble]:
    """Fetch the bubbles near a point, through the outbound path.

    Returns them in the order the endpoint gave, which is roughly nearest-first;
    the caller computes the certified radius from it. `missing_ok` is set so a
    404 is an empty answer rather than a raise, matching the sweep's treatment of
    an uncovered point.
    """
    body = await fetcher.get(bubble_url(lon, lat, half=half), missing_ok=True)
    return parse_bubbles(body)


def chains(bubbles: Iterable[Bubble]) -> Iterator[list[Bubble]]:
    """Group bubbles into drive sequences by following `prev`/`next`.

    Each yielded list is one maximal chain in order — a polyline of coverage.
    Only links whose target is present are followed, so a chain that leaves the
    collected set simply ends; the missing neighbour, if real, is found by
    another call and stitched on a later pass. Bubbles with no present neighbour
    come back as singletons, which the line writer drops as non-geometry.

    Deterministic regardless of input order: chains start from the bubble in each
    connected run with no present predecessor (or the lowest id in a cycle), so
    the same set always yields the same lines.
    """
    by_id = {b.id: b for b in bubbles}
    seen: set[int] = set()

    def heads() -> Iterator[Bubble]:
        # A head is a bubble whose predecessor is not in the set. Ties and cycles
        # fall back to lowest id so the walk is order-independent.
        for bubble in sorted(by_id.values(), key=lambda b: b.id):
            if bubble.prev is None or bubble.prev not in by_id:
                yield bubble

    def walk(start: Bubble) -> list[Bubble]:
        run = []
        current: Bubble | None = start
        while current is not None and current.id not in seen:
            seen.add(current.id)
            run.append(current)
            nxt = current.next
            current = by_id.get(nxt) if nxt is not None else None
        return run

    for head in heads():
        if head.id not in seen:
            yield walk(head)
    # Anything left is inside a cycle with no head; start from its lowest id.
    for bubble in sorted(by_id.values(), key=lambda b: b.id):
        if bubble.id not in seen:
            yield walk(bubble)

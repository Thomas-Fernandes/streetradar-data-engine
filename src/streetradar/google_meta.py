"""Google Street View per-panorama metadata: the capture date and the history.

A coverage tile carries no date, so this is the second pass. Given a panorama id
from the geometry pass, the metadata endpoint returns that panorama's capture
date and its full historical stack — every past capture at the same spot, back to
2007, each with its own date and position. This is Google's unique richness: no
other provider exposes the whole time-travel.

## Why by id rather than by location

The geometry pass produces ids, and querying by id returns exactly that
panorama's date and history, where querying by location returns the *nearest*
panorama — which may be a different one. So the dates pass keys on the ids it
already holds.

## The wire format

`photometa/v1` answers XSSI-guarded JSON. The panorama message is `data[1][0]`,
not `data[1]` (the location-search endpoint wraps it differently). Within it:

    msg[1][1]                the id
    msg[5][0][1][0][2..3]    position
    msg[6][7]                the capture date, as [year, month(, day)]
    msg[5][0][8]             historical entries, as [[index, [year, month]], ...]
    msg[5][0][3][0]          the connected panoramas the indices point into

So one call yields the location's whole dated history — the indices in
`msg[5][0][8]` select, from the connected list, the past captures and give each
its date. Everything is read defensively: a field Google drops must leave the
panorama undated, never raise, since the pass runs over millions of ids.
"""

from __future__ import annotations

import datetime
import json
from collections.abc import Callable
from typing import TYPE_CHECKING, Final, NamedTuple, Protocol

if TYPE_CHECKING:
    from .model import CapturePrecision

__all__ = [
    "METADATA_PRECISION",
    "Getter",
    "HistoricalPano",
    "Metadata",
    "getmetadata",
    "metadata_url",
    "parse_metadata",
]

#: Google reports month precision (year + month), occasionally a day. Never a
#: full timestamp for first-party coverage, so the front end must not over-claim.
METADATA_PRECISION: Final[CapturePrecision] = "month"

_XSSI_PREFIX: Final = ")]}'"

#: The by-id metadata URL. The id is verbatim in the pb protobuf at `!2s{id}!`;
#: the rest is the fixed request the viewer sends (all toggles on). If Google
#: changes the shape, re-capture this from a fresh request.
_METADATA_URL: Final = (
    "https://www.google.com/maps/photometa/v1?authuser=0&hl=en&gl=en&pb="
    "!1m4!1smaps_sv.tactile!11m2!2m1!1b1!2m2!1sen!2sen!3m3!1m2!1e2!2s{id}"
    "!4m54!1e1!1e2!1e3!1e4!1e5!1e6!1e8!1e12!2m1!1e1!4m1!1i48!5m0!6m0!9m36"
    "!1m3!1e2!2b1!3e2!1m3!1e2!2b0!3e3!1m3!1e3!2b1!3e2!1m3!1e3!2b0!3e3!1m3!1e8!2b0!3e3"
    "!1m3!1e1!2b0!3e3!1m3!1e4!2b0!3e3!1m3!1e10!2b1!3e2!1m3!1e10!2b0!3e3!11m2!3m1!4b1"
)


class HistoricalPano(NamedTuple):
    """One past capture at a location, from the time-travel stack."""

    id: str
    lon: float
    lat: float
    captured_at: int


class Metadata(NamedTuple):
    """A panorama's own date and its historical stack."""

    id: str
    lon: float
    lat: float
    captured_at: int | None
    history: tuple[HistoricalPano, ...]


class Getter(Protocol):
    async def get(self, url: str, *, missing_ok: bool = ...) -> bytes | None: ...


def metadata_url(pano_id: str) -> str:
    """The by-id metadata URL for a panorama."""
    return _METADATA_URL.format(id=pano_id)


def _to_ms(date: object) -> int | None:
    """A `[year, month(, day)]` list to a millisecond UTC timestamp.

    Month precision, so day defaults to the first — the front end is told the
    precision separately (`METADATA_PRECISION`) and must not render a false day.
    """
    if not isinstance(date, list) or len(date) < 2:
        return None
    try:
        year, month = int(date[0]), int(date[1])
        day = int(date[2]) if len(date) > 2 and date[2] else 1
        moment = datetime.datetime(year, month, day, tzinfo=datetime.UTC)
    except (ValueError, TypeError):
        return None
    return int(moment.timestamp() * 1000)


def parse_metadata(body: bytes | None) -> Metadata | None:
    """Decode a by-id metadata response. Pure — no I/O.

    Returns None when the response holds no panorama (an unknown id, or an empty
    body); a partly-malformed message yields a panorama with whatever it could
    read — an undated panorama rather than none — because over millions of ids a
    single odd record must not abort the pass.
    """
    if not body:
        return None
    text = body.decode("utf-8", "replace") if isinstance(body, bytes) else body
    if text.startswith(_XSSI_PREFIX):
        newline = text.find("\n")
        text = text[newline + 1 :] if newline != -1 else text[len(_XSSI_PREFIX) :]
    try:
        data = json.loads(text)
    except (json.JSONDecodeError, ValueError):
        return None

    try:
        msg = data[1][0]
        pano_id = msg[1][1]
        position = msg[5][0][1][0]
        lat = float(position[2])
        lon = float(position[3])
    except (IndexError, TypeError, KeyError, ValueError):
        return None
    if not isinstance(pano_id, str):
        return None

    captured_at = _to_ms(_try(lambda: msg[6][7]))
    dated = _try(lambda: msg[5][0][8])
    connected = _try(lambda: msg[5][0][3][0])
    history = _parse_history(dated, connected, pano_id)
    return Metadata(id=pano_id, lon=lon, lat=lat, captured_at=captured_at, history=history)


def _parse_history(
    dated_indices: object, connected: object, current_id: str
) -> tuple[HistoricalPano, ...]:
    """The dated past captures, from the index→date map and the connected list."""
    if not isinstance(dated_indices, list) or not isinstance(connected, list):
        return ()

    out: list[HistoricalPano] = []
    for entry in dated_indices:
        try:
            index = int(entry[0])
            captured_at = _to_ms(entry[1])
            other = connected[index]
            other_id = other[0][1]
            lat = float(other[2][0][2])
            lon = float(other[2][0][3])
        except (IndexError, TypeError, KeyError, ValueError):
            continue
        if captured_at is None or not isinstance(other_id, str) or other_id == current_id:
            continue
        out.append(HistoricalPano(id=other_id, lon=lon, lat=lat, captured_at=captured_at))
    return tuple(out)


def _try[T](getter: Callable[[], T]) -> T | None:
    try:
        return getter()
    except (IndexError, TypeError, KeyError):
        return None


async def getmetadata(fetcher: Getter, pano_id: str) -> Metadata | None:
    """Fetch and decode one panorama's metadata, through the fetch path."""
    body = await fetcher.get(metadata_url(pano_id), missing_ok=True)
    return parse_metadata(body)

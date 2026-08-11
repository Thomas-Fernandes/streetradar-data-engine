"""Mapy over FRPC.

The fixtures here are **recorded**, not synthetic: real bytes off
`pro.mapy.cz/panorpc`, so they pin the wire format as it actually is rather than
as a reimplementation of it believes it to be. That is what invariant 4 asks for,
and it is what the Apple fixtures could not have.

Three responses are recorded — a `getbest` that found something, one that found
nothing, and a `getneighbours` — because the shapes differ and the empty case is
the common one over a country-wide sweep.
"""

from __future__ import annotations

import pathlib
from collections.abc import Coroutine, Mapping

import pytest

from streetradar.mapy import (
    CAPTURE_PRECISION,
    PANORPC_URL,
    SWEEP_ZOOM,
    Panorama,
    encode_call,
    getbest,
    getneighbours,
    headers,
    sweep_points,
)

FIXTURES = pathlib.Path(__file__).parent / "fixtures"
HIT = (FIXTURES / "mapy-getbest-hit.frpc").read_bytes()
MISS = (FIXTURES / "mapy-getbest-miss.frpc").read_bytes()
NEIGHBOURS = (FIXTURES / "mapy-getneighbours.frpc").read_bytes()

# Prague-ish, inside the bounds the previous engine actually collected.
BOUNDS = (15.0160, 50.0750, 15.0180, 50.0770)


class StubFetcher:
    """Replays one recorded body, and records what was posted to it."""

    def __init__(self, body: bytes) -> None:
        self.body = body
        self.posted: list[tuple[str, bytes, Mapping[str, str]]] = []

    @property
    def concurrency(self) -> int:
        return 8

    async def post(self, url: str, body: bytes, *, headers: Mapping[str, str]) -> bytes:
        self.posted.append((url, body, headers))
        return self.body


class TestEncoding:
    def test_it_produces_frpc_on_the_wire(self) -> None:
        # 0xca11 is FRPC's magic and 0x0201 the protocol version pyfrpc's own
        # client defaults to; a mismatch here is answered with a fault, not an
        # error, so it would look like missing coverage.
        raw = encode_call("getbest", (15.0, 50.0, 150.0, {}))
        assert raw[:2] == b"\xca\x11"
        assert raw[2:4] == b"\x02\x01"

    def test_the_method_name_travels_in_the_body(self) -> None:
        assert b"getbest" in encode_call("getbest", (15.0, 50.0, 150.0, {}))
        assert b"getneighbours" in encode_call("getneighbours", (1, {}))

    def test_the_content_type_is_frpc_both_ways(self) -> None:
        sent = headers()
        assert sent["Content-Type"] == sent["Accept"] == "application/x-frpc"

    def test_the_referer_that_unlocks_recent_coverage_is_sent(self) -> None:
        # Cyclomedia panoramas — everything from 2020 on — are withheld without
        # it, and withheld silently: an older panorama comes back instead.
        assert "mapy.cz" in headers()["Referer"]


class TestParsingRecordedResponses:
    def test_a_hit_yields_a_positioned_dated_panorama(self) -> None:
        stub = StubFetcher(HIT)
        pano = _run(getbest(stub, 15.017908, 50.076914))
        assert isinstance(pano, Panorama)
        assert pano.pano_id > 0
        assert 15.0 < pano.lon < 15.1
        assert 50.0 < pano.lat < 50.1
        assert pano.captured_at is not None

    def test_the_capture_time_is_a_millisecond_timestamp(self) -> None:
        pano = _run(getbest(StubFetcher(HIT), 15.017908, 50.076914))
        assert pano is not None and pano.captured_at is not None
        # Between 2013, when Mapy's oldest coverage was driven, and now.
        assert 1_356_998_400_000 < pano.captured_at < 2_000_000_000_000

    def test_the_recapture_years_arrive_with_the_panorama(self) -> None:
        # This is why "every date" costs one call per place rather than one per
        # pass: the timeline is free with any panorama.
        pano = _run(getbest(StubFetcher(HIT), 15.017908, 50.076914))
        assert pano is not None
        assert len(pano.timeline) > 1
        assert all(2000 < year < 2100 for year in pano.timeline)

    def test_a_miss_is_none_rather_than_an_error(self) -> None:
        # Over a sweep of 3.5 million tiles most answers are misses, so this is
        # the ordinary path, not an edge case.
        assert _run(getbest(StubFetcher(MISS), 14.0, 49.0)) is None

    def test_neighbours_come_back_positioned(self) -> None:
        found = _run(getneighbours(StubFetcher(NEIGHBOURS), 92512299))
        assert found
        assert all(p.has_position for p in found)
        assert all(p.pano_id > 0 for p in found)

    def test_an_empty_body_yields_nothing(self) -> None:
        assert _run(getbest(StubFetcher(b""), 15.0, 50.0)) is None
        assert _run(getneighbours(StubFetcher(b""), 1)) == []


class TestRequests:
    def test_every_call_goes_to_the_open_endpoint(self) -> None:
        # Not tilecache.mapy.com (403, CloudFront signed cookies) and not
        # api.mapy.cz/v1 (401, API key). ADR 0015.
        stub = StubFetcher(HIT)
        _run(getbest(stub, 15.0, 50.0))
        assert stub.posted[0][0] == PANORPC_URL
        assert "tilecache" not in PANORPC_URL and "/v1/" not in PANORPC_URL

    def test_the_search_radius_is_carried(self) -> None:
        stub = StubFetcher(HIT)
        _run(getbest(stub, 15.0, 50.0, radius=42.0))
        assert stub.posted[0][1] != encode_call("getbest", (15.0, 50.0, 150.0, {}))


class TestSweepPoints:
    def test_it_asks_about_tile_centres(self) -> None:
        # A corner sits on the boundary of four tiles and would bias every probe
        # the same way.
        for tile, lon, lat in sweep_points(BOUNDS):
            west, south, east, north = tile.bounds
            assert west < lon < east
            assert south < lat < north

    def test_every_point_carries_the_tile_it_stands_for(self) -> None:
        # The tile is the unit of work a ProbedGrid records, which is what makes
        # the sweep resumable without rewriting a 162 MB visited set.
        points = list(sweep_points(BOUNDS))
        assert points
        assert all(tile.z == SWEEP_ZOOM for tile, _, _ in points)
        assert len({tile for tile, _, _ in points}) == len(points)

    def test_the_grid_covers_the_bounds(self) -> None:
        # The *tiles* must cover the box; their centres cannot reach its edges,
        # which is a property of centres rather than a gap in the sweep.
        west, south, east, north = BOUNDS
        tiles = [tile for tile, _, _ in sweep_points(BOUNDS)]
        assert min(t.bounds[0] for t in tiles) <= west
        assert max(t.bounds[2] for t in tiles) >= east
        assert min(t.bounds[1] for t in tiles) <= south
        assert max(t.bounds[3] for t in tiles) >= north

    def test_a_coarser_zoom_asks_fewer_questions(self) -> None:
        assert len(list(sweep_points(BOUNDS, zoom=16))) < len(list(sweep_points(BOUNDS)))


class TestDeclarations:
    def test_the_sweep_zoom_is_seventeen(self) -> None:
        # 197 m per tile at Czech latitudes: fine enough that no component hides
        # between probes, coarse enough that the sweep is 39 minutes.
        assert SWEEP_ZOOM == 17

    def test_mapy_dates_to_the_second(self) -> None:
        # Unlike Já and Naver, whose driveline layers carry no date at all.
        assert CAPTURE_PRECISION == "second"


def _run[T](coro: Coroutine[None, None, T]) -> T:
    import asyncio

    return asyncio.run(coro)


@pytest.mark.parametrize("fixture", [HIT, MISS, NEIGHBOURS])
def test_the_fixtures_are_real_frpc(fixture: bytes) -> None:
    assert fixture[:2] == b"\xca\x11"

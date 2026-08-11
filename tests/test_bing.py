"""Decoding Bing Streetside bubbles, and chaining them into drives.

The fixtures are recorded, not synthetic: real bytes off the endpoint, so they
pin the wire format as it is. `bing-rural.json` is a real response trimmed to its
first 120 bubbles — small enough to commit, and its trimmed edge exercises the
case that matters most for chaining, a link whose target is not in the set.
"""

from __future__ import annotations

import datetime
import json
import pathlib

import pytest

from streetradar.bing import (
    BUBBLE_ENDPOINT,
    CAPTURE_PRECISION,
    CERTIFY_FRACTION,
    Bubble,
    bubble_url,
    chains,
    getbubbles,
    parse_bubbles,
)

FIXTURES = pathlib.Path(__file__).parent / "fixtures"
RURAL = (FIXTURES / "bing-rural.json").read_bytes()
EMPTY = (FIXTURES / "bing-empty-ocean.json").read_bytes()


class TestParse:
    def test_the_real_response_decodes_to_positioned_bubbles(self) -> None:
        bubbles = parse_bubbles(RURAL)
        assert len(bubbles) == 120
        assert all(-2.0 < b.lon < 0.0 and 51.0 < b.lat < 52.0 for b in bubbles)

    def test_the_header_element_is_dropped(self) -> None:
        # Element [0] is {"elapsed": ...}; it has no position and must not become
        # a bubble at (0, 0).
        assert all(b.id != 0 for b in parse_bubbles(RURAL))

    def test_dates_decode_to_millisecond_timestamps(self) -> None:
        dated = [b for b in parse_bubbles(RURAL) if b.captured_at is not None]
        assert dated
        for b in dated:
            # Between Streetside's oldest coverage (2011) and now.
            assert 1_293_840_000_000 < b.captured_at < 2_000_000_000_000  # type: ignore[operator]

    def test_the_capture_month_is_recoverable(self) -> None:
        b = next(b for b in parse_bubbles(RURAL) if b.captured_at is not None)
        moment = datetime.datetime.fromtimestamp(b.captured_at / 1000, datetime.UTC)  # type: ignore[operator]
        assert 2010 < moment.year < 2030

    def test_the_drive_links_are_carried(self) -> None:
        # The whole reason lines come for free: bubbles point at their neighbours.
        linked = [b for b in parse_bubbles(RURAL) if b.prev is not None or b.next is not None]
        assert len(linked) > 100

    @pytest.mark.parametrize("body", [None, b"", EMPTY])
    def test_no_coverage_is_no_bubbles_rather_than_an_error(self, body: bytes | None) -> None:
        # The empty-ocean fixture is the header alone. Over a world sweep this is
        # the overwhelmingly common answer.
        assert parse_bubbles(body) == []

    def test_malformed_json_is_empty_not_a_raise(self) -> None:
        assert parse_bubbles(b"<html>error</html>") == []

    def test_a_record_without_a_position_is_skipped(self) -> None:
        body = json.dumps([{"elapsed": 0.1}, {"id": 5, "cd": ""}]).encode()
        assert parse_bubbles(body) == []

    def test_an_unparseable_date_leaves_the_bubble_undated(self) -> None:
        body = json.dumps(
            [{"elapsed": 0.1}, {"id": 5, "la": 51.5, "lo": -0.1, "cd": "not a date"}]
        ).encode()
        assert parse_bubbles(body)[0].captured_at is None


class TestBubbleUrl:
    def test_it_centres_the_window_on_the_point(self) -> None:
        url = bubble_url(-0.127, 51.5085, half=0.01)
        assert url.startswith(BUBBLE_ENDPOINT)
        assert "north=51.5185" in url and "south=51.4985" in url
        assert "east=-0.117000" in url and "west=-0.137000" in url

    def test_it_requests_the_cap(self) -> None:
        assert "count=2048" in bubble_url(0.0, 0.0)


class TestChains:
    def make(self, *links: tuple[int, int | None, int | None]) -> list[Bubble]:
        return [
            Bubble(id=i, lon=0.0, lat=0.0, captured_at=None, prev=p, next=n) for i, p, n in links
        ]

    def test_a_straight_drive_is_one_ordered_chain(self) -> None:
        bubbles = self.make((1, None, 2), (2, 1, 3), (3, 2, None))
        runs = list(chains(bubbles))
        assert len(runs) == 1
        assert [b.id for b in runs[0]] == [1, 2, 3]

    def test_a_chain_ends_where_the_next_bubble_is_absent(self) -> None:
        # The trimmed-fixture case: a link pointing outside the collected set.
        bubbles = self.make((1, None, 2), (2, 1, 99))
        runs = list(chains(bubbles))
        assert [b.id for b in runs[0]] == [1, 2]

    def test_two_separate_drives_are_two_chains(self) -> None:
        bubbles = self.make((1, None, 2), (2, 1, None), (10, None, 11), (11, 10, None))
        runs = sorted(chains(bubbles), key=lambda r: r[0].id)
        assert [[b.id for b in r] for r in runs] == [[1, 2], [10, 11]]

    def test_the_result_is_order_independent(self) -> None:
        forward = self.make((1, None, 2), (2, 1, 3), (3, 2, None))
        shuffled = [forward[2], forward[0], forward[1]]
        assert [b.id for b in next(iter(chains(forward)))] == [
            b.id for b in next(iter(chains(shuffled)))
        ]

    def test_a_cycle_is_walked_once_from_its_lowest_id(self) -> None:
        # A closed loop has no head; it must not spin forever, and must start
        # deterministically.
        bubbles = self.make((1, 3, 2), (2, 1, 3), (3, 2, 1))
        runs = list(chains(bubbles))
        assert len(runs) == 1
        assert runs[0][0].id == 1
        assert len(runs[0]) == 3

    def test_the_real_fixture_chains_without_loss_or_duplication(self) -> None:
        bubbles = parse_bubbles(RURAL)
        runs = list(chains(bubbles))
        chained = [b.id for run in runs for b in run]
        assert len(chained) == len(bubbles)
        assert len(set(chained)) == len(bubbles)


class StubGetter:
    def __init__(self, body: bytes) -> None:
        self.body = body
        self.urls: list[str] = []

    async def get(self, url: str, *, missing_ok: bool = False) -> bytes | None:
        self.urls.append(url)
        return self.body


class TestGetbubbles:
    async def test_it_calls_the_endpoint_and_parses(self) -> None:
        getter = StubGetter(RURAL)
        bubbles = await getbubbles(getter, -1.1, 51.45)
        assert len(bubbles) == 120
        assert getter.urls[0].startswith(BUBBLE_ENDPOINT)

    async def test_an_empty_answer_is_no_bubbles(self) -> None:
        assert await getbubbles(StubGetter(EMPTY), -30.0, 45.0) == []


class TestDeclarations:
    def test_streetside_dates_to_the_second(self) -> None:
        assert CAPTURE_PRECISION == "second"

    def test_the_certify_fraction_is_the_conservative_measured_floor(self) -> None:
        # 0.30 rural was the worst of the three strata; a sweep is safe only at
        # the worst. Measured, not proven — see ADR 0016.
        assert CERTIFY_FRACTION == 0.30

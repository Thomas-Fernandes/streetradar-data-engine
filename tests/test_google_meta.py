"""Parsing Google's per-panorama metadata: the date and the historical stack.

The fixture is a recorded real by-id response, so it pins the wire format as it
is — the paths into Google's deeply nested arrays are exactly the kind of thing a
synthetic fixture would get subtly wrong.
"""

from __future__ import annotations

import datetime
import pathlib

import pytest

from streetradar.google_meta import (
    METADATA_PRECISION,
    Metadata,
    getmetadata,
    metadata_url,
    parse_metadata,
)

FIXTURE = (pathlib.Path(__file__).parent / "fixtures" / "google-meta-byid.json").read_bytes()


def year_month(ms: int) -> tuple[int, int]:
    moment = datetime.datetime.fromtimestamp(ms / 1000, datetime.UTC)
    return moment.year, moment.month


class TestParse:
    def test_the_current_panorama_is_dated(self) -> None:
        meta = parse_metadata(FIXTURE)
        assert isinstance(meta, Metadata)
        assert meta.captured_at is not None
        assert year_month(meta.captured_at) == (2012, 5)

    def test_the_position_is_read(self) -> None:
        meta = parse_metadata(FIXTURE)
        assert meta is not None
        assert 51.0 < meta.lat < 52.0 and -1.0 < meta.lon < 0.0

    def test_the_full_history_comes_back_with_dates(self) -> None:
        # This recorded panorama has two past layers; the whole point of Google.
        meta = parse_metadata(FIXTURE)
        assert meta is not None
        dates = {year_month(h.captured_at) for h in meta.history}
        assert (2019, 9) in dates
        assert (2023, 7) in dates

    def test_each_historical_layer_is_positioned(self) -> None:
        meta = parse_metadata(FIXTURE)
        assert meta is not None
        assert all(51.0 < h.lat < 52.0 and -1.0 < h.lon < 0.0 for h in meta.history)

    def test_the_current_panorama_is_not_in_its_own_history(self) -> None:
        meta = parse_metadata(FIXTURE)
        assert meta is not None
        assert all(h.id != meta.id for h in meta.history)

    @pytest.mark.parametrize("body", [None, b"", b")]}'\n[]", b")]}'\n[[],[]]"])
    def test_no_panorama_is_none_not_a_raise(self, body: bytes | None) -> None:
        assert parse_metadata(body) is None

    def test_malformed_json_is_none(self) -> None:
        assert parse_metadata(b")]}'\n<not json>") is None

    def test_a_month_date_is_rendered_at_the_first_of_the_month(self) -> None:
        # Precision is month; the day is a placeholder the front end must not show.
        meta = parse_metadata(FIXTURE)
        assert meta is not None and meta.captured_at is not None
        assert datetime.datetime.fromtimestamp(meta.captured_at / 1000, datetime.UTC).day == 1


class TestUrl:
    def test_the_id_is_carried_verbatim(self) -> None:
        url = metadata_url("w3iJm_ZtXrj6VlqfsVpbog")
        assert "!2sw3iJm_ZtXrj6VlqfsVpbog!" in url
        assert "photometa/v1" in url


class StubGetter:
    def __init__(self, body: bytes | None) -> None:
        self.body = body
        self.urls: list[str] = []

    async def get(self, url: str, *, missing_ok: bool = False) -> bytes | None:
        self.urls.append(url)
        return self.body


class TestGetmetadata:
    async def test_it_fetches_by_id_and_parses(self) -> None:
        getter = StubGetter(FIXTURE)
        meta = await getmetadata(getter, "w3iJm_ZtXrj6VlqfsVpbog")
        assert meta is not None and meta.captured_at is not None
        assert "!2sw3iJm_ZtXrj6VlqfsVpbog!" in getter.urls[0]

    async def test_an_unknown_id_is_none(self) -> None:
        assert await getmetadata(StubGetter(b")]}'\n[[],[]]"), "nope") is None


class TestDeclaration:
    def test_google_dates_to_the_month(self) -> None:
        assert METADATA_PRECISION == "month"

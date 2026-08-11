"""The bounded probe.

Driven against a stub fetcher with a settable ceiling, because the properties
worth pinning are the ones that protect the host: that the budget binds before
the ladder does, that a level which starts erroring stops the climb, and that a
refused level is never reported as the sustainable rate.
"""

from __future__ import annotations

import asyncio
from collections.abc import Iterator, Mapping

import pytest

from streetradar.probe import LevelResult, climb, probe_level
from streetradar.tiles import Tile, TileSource

ZOOM = 17


def source() -> TileSource:
    return TileSource(
        id="test",
        url_template="https://example.invalid/{z}/{x}/{y}",
        min_zoom=0,
        max_zoom=20,
        bounds=(5.7, 49.4, 6.6, 50.2),
    )


def tiles(count: int = 1_000_000) -> Iterator[Tile]:
    """A stream long enough that no level runs dry, and never repeating."""
    for i in range(count):
        yield Tile(ZOOM, 67756 + i % 4096, 44565 + i // 4096)


class StubFetcher:
    """Answers instantly below `ceiling` concurrent calls, errors above it."""

    def __init__(self, ceiling: int = 10**9, *, empty_every: int = 0) -> None:
        self.ceiling = ceiling
        self.empty_every = empty_every
        self.inflight = 0
        self.peak = 0
        self.served = 0

    @property
    def concurrency(self) -> int:
        return 8

    async def fetch_tile(self, url: str, headers: Mapping[str, str]) -> bytes | None:
        self.inflight += 1
        self.peak = max(self.peak, self.inflight)
        try:
            await asyncio.sleep(0.002)
            self.served += 1
            if self.inflight > self.ceiling:
                raise ConnectionError("refused")
            if self.empty_every and self.served % self.empty_every == 0:
                return None
            return b"x" * 100
        finally:
            self.inflight -= 1


class TestLevelResult:
    def test_rate_is_requests_over_elapsed(self) -> None:
        level = LevelResult(
            concurrency=8, seconds=2.0, requests=100, errors=0, timeouts=0, empty=0, bytes_in=0
        )
        assert level.rate == 50.0

    def test_timeouts_count_against_the_error_rate(self) -> None:
        # A host that stops answering is refusing just as surely as one that
        # returns 429, and it is the more common shape.
        level = LevelResult(
            concurrency=8, seconds=1.0, requests=100, errors=1, timeouts=4, empty=0, bytes_in=0
        )
        assert level.error_rate == pytest.approx(0.05)
        assert level.refused

    def test_a_clean_level_is_not_refused(self) -> None:
        # The previous engine measured 0.09% errors over 16.2M requests against
        # this provider, so 1% is still comfortably noise.
        level = LevelResult(
            concurrency=8, seconds=1.0, requests=1000, errors=10, timeouts=0, empty=0, bytes_in=0
        )
        assert not level.refused

    def test_percentiles_survive_an_empty_sample(self) -> None:
        level = LevelResult(
            concurrency=8, seconds=1.0, requests=0, errors=0, timeouts=0, empty=0, bytes_in=0
        )
        assert level.percentile(0.95) == 0.0
        assert level.rate == 0.0


class TestProbeLevel:
    async def test_it_holds_the_concurrency_it_was_asked_for(self) -> None:
        fetcher = StubFetcher()
        level = await probe_level(
            fetcher, source(), tiles(), concurrency=16, seconds=0.05, budget=200
        )
        assert fetcher.peak <= 16
        assert level.requests > 0

    def test_the_budget_binds_before_the_clock(self) -> None:
        async def run() -> None:
            level = await probe_level(
                StubFetcher(), source(), tiles(), concurrency=8, seconds=30.0, budget=50
            )
            # Workers check the budget between requests, so a few may overshoot.
            assert 50 <= level.requests <= 50 + 8

        asyncio.run(run())

    async def test_an_exhausted_tile_stream_ends_the_level(self) -> None:
        level = await probe_level(
            StubFetcher(), source(), tiles(12), concurrency=4, seconds=30.0, budget=10_000
        )
        assert level.requests == 12

    async def test_empty_responses_are_counted_apart_from_errors(self) -> None:
        # Mapy answers 302 -> ./default for a tile with no coverage. That is a
        # successful answer meaning "nothing here", not a failure.
        level = await probe_level(
            StubFetcher(empty_every=2), source(), tiles(), concurrency=4, seconds=0.05, budget=100
        )
        assert level.empty > 0
        assert level.error_rate == 0.0

    async def test_errors_do_not_stop_the_level(self) -> None:
        # The probe's job is to survive and count; stopping on the first error
        # would report a ceiling one request wide.
        level = await probe_level(
            StubFetcher(ceiling=1), source(), tiles(), concurrency=8, seconds=0.05, budget=100
        )
        assert level.errors > 0
        assert level.requests > level.errors // 2


class TestClimb:
    async def test_it_stops_when_the_host_starts_refusing(self) -> None:
        fetcher = StubFetcher(ceiling=20)
        result = await climb(
            fetcher,
            source(),
            tiles(),
            ladder=(8, 16, 32, 64),
            seconds=0.2,
            budget=1_000_000,
            pause=0.0,
        )
        assert result.stopped_early
        assert result.levels[-1].refused
        assert len(result.levels) < 4

    async def test_the_reported_rate_is_never_a_refused_level(self) -> None:
        result = await climb(
            StubFetcher(ceiling=20),
            source(),
            tiles(),
            ladder=(8, 16, 32, 64),
            seconds=0.2,
            budget=1_000_000,
            pause=0.0,
        )
        assert result.best is not None
        assert not result.best.refused

    async def test_the_budget_is_a_hard_ceiling_across_levels(self) -> None:
        # 2,000 is what the project's own rule allows a bounded probe. A ladder
        # that quietly spent more would make the rule decorative.
        fetcher = StubFetcher()
        result = await climb(
            fetcher,
            source(),
            tiles(),
            ladder=(8, 16, 32, 64, 128),
            seconds=30.0,
            budget=300,
            pause=0.0,
        )
        assert sum(level.requests for level in result.levels) <= 300 + 128

    async def test_a_clean_climb_reaches_the_top_rung(self) -> None:
        result = await climb(
            StubFetcher(),
            source(),
            tiles(),
            ladder=(4, 8),
            seconds=0.05,
            budget=1_000_000,
            pause=0.0,
        )
        assert not result.stopped_early
        assert [level.concurrency for level in result.levels] == [4, 8]

    async def test_the_report_names_a_rate_and_a_concurrency(self) -> None:
        result = await climb(
            StubFetcher(), source(), tiles(), ladder=(4,), seconds=0.05, budget=200, pause=0.0
        )
        text = result.report()
        assert "sustainable" in text
        assert "req/s" in text

    async def test_tiles_are_never_reused_across_levels(self) -> None:
        # A repeated tile is served from the host's cache and inflates the rate.
        # Mapy's coverage layer caches for 24 hours, so this is not theoretical.
        seen: list[Tile] = []

        class Recording(StubFetcher):
            async def fetch_tile(self, url: str, headers: Mapping[str, str]) -> bytes | None:
                z, x, y = (int(p) for p in url.rstrip("/").split("/")[-3:])
                seen.append(Tile(z, x, y))
                return await super().fetch_tile(url, headers)

        await climb(
            Recording(), source(), tiles(), ladder=(4, 8), seconds=0.05, budget=400, pause=0.0
        )
        assert len(seen) == len(set(seen))

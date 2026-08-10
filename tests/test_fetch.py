"""The outbound path: budget, policy validation, statistics, and the guard.

No network here either. What is worth testing is the accounting and the
refusals, since those are what make a long run diagnosable and a stray local
command harmless.
"""

from __future__ import annotations

import asyncio

import pytest

from streetradar.fetch import (
    DEFAULT_USER_AGENT,
    Fetcher,
    FetchPolicy,
    OutboundBlocked,
    RateBudget,
    Stats,
    outbound_allowed,
)


class TestFetchPolicy:
    def test_defaults_are_conservative(self) -> None:
        # The previous engine defaulted Naver to 1000 concurrent requests against
        # an undocumented endpoint. The default here has to be defensible.
        policy = FetchPolicy()
        assert policy.max_concurrency <= 8
        assert policy.user_agent == DEFAULT_USER_AGENT
        assert "streetradar.app" in policy.user_agent

    def test_there_is_no_unlimited_concurrency_escape_hatch(self) -> None:
        with pytest.raises(ValueError, match="at least 1"):
            FetchPolicy(max_concurrency=0)

    def test_a_zero_hourly_budget_is_a_mistake_not_unlimited(self) -> None:
        with pytest.raises(ValueError, match="positive or None"):
            FetchPolicy(requests_per_hour=0)

    def test_no_proxy_configuration_exists(self) -> None:
        # Deliberate: routing across several IPs only makes per-IP limits not
        # apply, which is evasion rather than politeness. Asserted so that
        # re-adding it is a visible decision rather than a quiet one.
        assert not any("prox" in f.lower() for f in FetchPolicy.__dataclass_fields__)


class TestRateBudget:
    async def test_unlimited_budget_never_parks(self) -> None:
        budget = RateBudget(None)
        assert await budget.acquire() == 0.0
        assert budget.spent == 0

    async def test_spends_up_to_the_limit_without_waiting(self) -> None:
        budget = RateBudget(3)
        for _ in range(3):
            assert await budget.acquire() == 0.0
        assert budget.spent == 3

    async def test_parks_once_the_window_is_full(self) -> None:
        # A short window keeps the test fast; the behaviour under test is that
        # acquire blocks rather than letting the burst through.
        budget = RateBudget(2, window=0.15)
        await budget.acquire()
        await budget.acquire()
        waited = await asyncio.wait_for(budget.acquire(), timeout=5.0)
        assert waited > 0.0

    async def test_window_slides_so_capacity_returns(self) -> None:
        budget = RateBudget(1, window=0.05)
        await budget.acquire()
        await asyncio.sleep(0.08)
        assert await budget.acquire() == 0.0


class TestStats:
    def test_separates_absent_from_blocked(self) -> None:
        # The previous engine folded "already had it" and "provider returned 404"
        # into one skipped_tiles counter, which made coverage density unknowable.
        stats = Stats()
        stats.by_status.update({200: 10, 404: 5, 403: 2})
        stats.requests = 17
        assert stats.by_status[404] == 5
        assert stats.by_status[403] == 2
        summary = stats.summary()
        assert "404:5" in summary and "403:2" in summary

    def test_throughput_is_reported(self) -> None:
        stats = Stats()
        stats.requests = 100
        assert stats.per_second > 0.0
        assert "requests in" in stats.summary()


class TestOutboundGuard:
    async def test_fetcher_refuses_to_open_without_the_flag(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        monkeypatch.delenv("ENGINE_ALLOW_OUTBOUND", raising=False)
        assert not outbound_allowed()
        with pytest.raises(OutboundBlocked, match="ENGINE_ALLOW_OUTBOUND"):
            async with Fetcher():
                pass

    @pytest.mark.parametrize("value", ["0", "", "false", "False"])
    def test_falsy_flag_values_do_not_enable_outbound(
        self, monkeypatch: pytest.MonkeyPatch, value: str
    ) -> None:
        monkeypatch.setenv("ENGINE_ALLOW_OUTBOUND", value)
        assert not outbound_allowed()

    def test_flag_enables_outbound_when_set(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.setenv("ENGINE_ALLOW_OUTBOUND", "1")
        assert outbound_allowed()

    async def test_require_flag_can_be_waived_for_tests(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        monkeypatch.delenv("ENGINE_ALLOW_OUTBOUND", raising=False)
        async with Fetcher(require_flag=False) as fetcher:
            assert fetcher.concurrency == FetchPolicy().max_concurrency

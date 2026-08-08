import asyncio

import pytest

from streetradar.http import (
    HttpPolicy,
    ProxyPool,
    RateBudget,
    _parse_retry_after,
    _redact,
)


class TestProxyPool:
    async def test_empty_pool_means_direct_connection(self):
        pool = ProxyPool([])
        assert await pool.acquire() is None

    async def test_rotates_round_robin(self):
        pool = ProxyPool(["http://a", "http://b", "http://c"])
        picked = [await pool.acquire() for _ in range(6)]
        assert picked == ["http://a", "http://b", "http://c"] * 2

    async def test_benched_proxy_is_skipped(self):
        pool = ProxyPool(["http://a", "http://b"], cooldown=60)
        await pool.penalise("http://a")

        assert pool.available == 1
        assert {await pool.acquire() for _ in range(4)} == {"http://b"}

    async def test_bench_expires(self, monkeypatch):
        pool = ProxyPool(["http://a", "http://b"], cooldown=0.01)
        await pool.penalise("http://a")
        await asyncio.sleep(0.05)

        assert pool.available == 2

    async def test_reuses_a_benched_proxy_rather_than_stalling(self):
        """A fully benched pool must keep the pipeline moving: a slow request
        beats a dead one."""
        pool = ProxyPool(["http://a", "http://b"], cooldown=300)
        await pool.penalise("http://a")
        await pool.penalise("http://b")

        assert pool.available == 0
        assert await pool.acquire() in {"http://a", "http://b"}

    async def test_penalising_none_is_a_no_op(self):
        pool = ProxyPool(["http://a"])
        await pool.penalise(None)
        assert pool.available == 1


class TestRateBudget:
    async def test_unlimited_never_blocks(self):
        budget = RateBudget(None)
        for _ in range(50):
            await budget.acquire()

    async def test_allows_up_to_the_limit_without_waiting(self):
        budget = RateBudget(3, window=100)
        for _ in range(3):
            await asyncio.wait_for(budget.acquire(), timeout=0.5)

    async def test_blocks_once_the_window_is_full(self):
        budget = RateBudget(2, window=100)
        await budget.acquire()
        await budget.acquire()

        with pytest.raises(asyncio.TimeoutError):
            await asyncio.wait_for(budget.acquire(), timeout=0.2)

    async def test_window_slides(self):
        """Requests leaving the window free up allowance again."""
        budget = RateBudget(2, window=0.1)
        await budget.acquire()
        await budget.acquire()
        await asyncio.sleep(0.15)

        await asyncio.wait_for(budget.acquire(), timeout=0.5)


class TestRetryAfter:
    @pytest.mark.parametrize(
        "header,expected",
        [("30", 30.0), ("0", 0.0), ("1.5", 1.5), (None, None), ("", None), ("soon", None)],
    )
    def test_parsing(self, header, expected):
        assert _parse_retry_after(header) == expected

    def test_negative_is_clamped(self):
        assert _parse_retry_after("-5") == 0.0


class TestRedact:
    def test_strips_credentials(self):
        assert _redact("http://user:pass@proxy.example:8080") == "proxy.example:8080"

    def test_leaves_credential_free_urls_alone(self):
        assert _redact("http://proxy.example:8080") == "http://proxy.example:8080"


class TestPolicyFromEnv:
    def test_reads_prefixed_variables(self, monkeypatch):
        monkeypatch.setenv("SCRAPE_NAVER_MAX_CONCURRENCY", "9")
        monkeypatch.setenv("SCRAPE_NAVER_REQUESTS_PER_HOUR", "1200")
        monkeypatch.setenv("SCRAPE_NAVER_PROXIES", "http://a, http://b ,")

        policy = HttpPolicy.from_env("SCRAPE_NAVER")

        assert policy.max_concurrency == 9
        assert policy.requests_per_hour == 1200
        assert policy.proxies == ("http://a", "http://b")

    def test_zero_requests_per_hour_means_unlimited(self, monkeypatch):
        monkeypatch.setenv("SCRAPE_X_REQUESTS_PER_HOUR", "0")
        assert HttpPolicy.from_env("SCRAPE_X").requests_per_hour is None

    def test_overrides_win(self, monkeypatch):
        monkeypatch.setenv("SCRAPE_X_MAX_CONCURRENCY", "2")
        assert HttpPolicy.from_env("SCRAPE_X", max_concurrency=16).max_concurrency == 16

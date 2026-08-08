"""The single outbound HTTP path for every collector.

Providers are scraped through undocumented endpoints. They rate-limit, they ban
IPs, and they change without notice. Concentrating every request here means
politeness is a property of the pipeline rather than something each collector
remembers to reimplement — today `naver` retries and `mapy` does not, and
neither has ever used a proxy.

What this gives you over a bare `aiohttp.ClientSession`:

* a rotating proxy pool that benches a proxy after it fails, so one dead exit
  node does not poison every subsequent request
* exponential backoff with jitter, which honours `Retry-After` when the server
  bothers to send one
* a requests-per-hour budget, so a long run cannot accelerate into a ban
* a concurrency ceiling shared across every caller of the same client

Usage:

    policy = HttpPolicy(max_concurrency=4, requests_per_hour=5_000)
    async with HttpClient(policy) as http:
        payload = await http.get_bytes(url)
"""

from __future__ import annotations

import asyncio
import random
import time
from collections import deque
from collections.abc import Sequence
from dataclasses import dataclass, field
from typing import Any

import aiohttp

from .config import env_int, env_str
from .logging import get_logger

__all__ = [
    "HttpPolicy",
    "HttpClient",
    "ProxyPool",
    "RateBudget",
    "HttpError",
    "RateLimited",
    "RetriesExhausted",
]

logger = get_logger(__name__)

DEFAULT_USER_AGENT = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/126.0.0.0 Safari/537.36"
)

# 408 and 425 are transient client-side conditions; 429 is explicit rate
# limiting; 5xx is the server's problem and usually passes.
RETRY_STATUSES = frozenset({408, 425, 429, 500, 502, 503, 504})


class HttpError(RuntimeError):
    """Base class for failures raised by this module."""


class RateLimited(HttpError):
    """The server explicitly asked us to slow down."""

    def __init__(self, url: str, retry_after: float | None) -> None:
        super().__init__(f"Rate limited on {url} (retry-after={retry_after})")
        self.url = url
        self.retry_after = retry_after


class RetriesExhausted(HttpError):
    """Every attempt failed. Carries the last error for context."""

    def __init__(self, url: str, attempts: int, last_error: BaseException | None) -> None:
        super().__init__(f"{attempts} attempts failed for {url}: {last_error}")
        self.url = url
        self.attempts = attempts
        self.last_error = last_error


@dataclass(frozen=True)
class HttpPolicy:
    """How hard we are willing to push a provider."""

    max_concurrency: int = 4
    requests_per_hour: int | None = None
    max_retries: int = 4
    backoff_base: float = 0.5
    backoff_max: float = 60.0
    timeout: float = 30.0
    user_agent: str = DEFAULT_USER_AGENT
    proxies: Sequence[str] = ()
    proxy_cooldown: float = 300.0
    headers: dict[str, str] = field(default_factory=dict)

    @classmethod
    def from_env(cls, prefix: str = "SCRAPE", **overrides: Any) -> HttpPolicy:
        """Build a policy from `{prefix}_*` variables.

        Recognised: MAX_CONCURRENCY, REQUESTS_PER_HOUR, MAX_RETRIES, TIMEOUT,
        USER_AGENT, PROXIES (comma-separated), PROXY_COOLDOWN.
        """
        raw_proxies = env_str(f"{prefix}_PROXIES", "") or ""
        proxies = tuple(p.strip() for p in raw_proxies.split(",") if p.strip())

        rph = env_int(f"{prefix}_REQUESTS_PER_HOUR", 0)

        defaults: dict[str, Any] = {
            "max_concurrency": env_int(f"{prefix}_MAX_CONCURRENCY", 4),
            "requests_per_hour": rph or None,
            "max_retries": env_int(f"{prefix}_MAX_RETRIES", 4),
            "timeout": float(env_int(f"{prefix}_TIMEOUT", 30)),
            "user_agent": env_str(f"{prefix}_USER_AGENT", DEFAULT_USER_AGENT) or DEFAULT_USER_AGENT,
            "proxies": proxies,
            "proxy_cooldown": float(env_int(f"{prefix}_PROXY_COOLDOWN", 300)),
        }
        defaults.update(overrides)
        return cls(**defaults)


class ProxyPool:
    """Round-robin proxy rotation with a penalty box.

    A proxy that fails is benched for `cooldown` seconds instead of being
    dropped: exit nodes recover, and a pool that only ever shrinks ends up
    empty halfway through a long collection.

    An empty pool is valid and means "connect directly".
    """

    def __init__(self, proxies: Sequence[str], cooldown: float = 300.0) -> None:
        self._proxies = list(proxies)
        self._cooldown = cooldown
        self._benched: dict[str, float] = {}
        self._cursor = 0
        self._lock = asyncio.Lock()

    def __len__(self) -> int:
        return len(self._proxies)

    @property
    def available(self) -> int:
        now = time.monotonic()
        return sum(1 for p in self._proxies if self._benched.get(p, 0.0) <= now)

    async def acquire(self) -> str | None:
        """Next usable proxy, or None to connect directly.

        When every proxy is benched the least-recently-benched one is reused
        rather than stalling the pipeline — a slow request beats a dead one.
        """
        if not self._proxies:
            return None

        async with self._lock:
            now = time.monotonic()
            for _ in range(len(self._proxies)):
                proxy = self._proxies[self._cursor % len(self._proxies)]
                self._cursor += 1
                if self._benched.get(proxy, 0.0) <= now:
                    return proxy

            soonest = min(self._proxies, key=lambda p: self._benched.get(p, 0.0))
            logger.warning("every proxy is benched; reusing %s early", _redact(soonest))
            return soonest

    async def penalise(self, proxy: str | None, seconds: float | None = None) -> None:
        """Bench a proxy after a failure."""
        if not proxy:
            return
        async with self._lock:
            self._benched[proxy] = time.monotonic() + (seconds or self._cooldown)
        logger.debug("benched proxy %s for %.0fs", _redact(proxy), seconds or self._cooldown)


class RateBudget:
    """Sliding-window limiter, in requests per hour.

    A sliding window rather than a token bucket because provider limits are
    expressed as "N per hour" and a bucket lets you spend the whole hour's
    allowance in the first minute — which is exactly the burst that gets an IP
    banned.
    """

    def __init__(self, requests_per_hour: int | None, window: float = 3600.0) -> None:
        self._limit = requests_per_hour
        self._window = window
        self._events: deque[float] = deque()
        self._lock = asyncio.Lock()

    async def acquire(self) -> None:
        if not self._limit:
            return

        while True:
            async with self._lock:
                now = time.monotonic()
                while self._events and now - self._events[0] >= self._window:
                    self._events.popleft()

                if len(self._events) < self._limit:
                    self._events.append(now)
                    return

                wait = self._window - (now - self._events[0])

            logger.info("request budget reached, pausing %.1fs", wait)
            await asyncio.sleep(min(wait, 60.0))


class HttpClient:
    """Async HTTP client applying a `HttpPolicy` to every request."""

    def __init__(self, policy: HttpPolicy | None = None) -> None:
        self.policy = policy or HttpPolicy()
        self.proxies = ProxyPool(self.policy.proxies, self.policy.proxy_cooldown)
        self.budget = RateBudget(self.policy.requests_per_hour)
        self._semaphore = asyncio.Semaphore(self.policy.max_concurrency)
        self._session: aiohttp.ClientSession | None = None

    async def __aenter__(self) -> HttpClient:
        headers = {"User-Agent": self.policy.user_agent, **self.policy.headers}
        self._session = aiohttp.ClientSession(
            timeout=aiohttp.ClientTimeout(total=self.policy.timeout),
            headers=headers,
        )
        logger.debug(
            "http client ready (concurrency=%d, proxies=%d, budget=%s/h)",
            self.policy.max_concurrency,
            len(self.proxies),
            self.policy.requests_per_hour or "unlimited",
        )
        return self

    async def __aexit__(self, *exc_info: object) -> None:
        if self._session is not None:
            await self._session.close()
            self._session = None

    async def get_bytes(self, url: str, **kwargs: Any) -> bytes:
        """GET a URL and return the body, retrying per policy."""
        return await self._request("GET", url, **kwargs)

    async def get_json(self, url: str, **kwargs: Any) -> Any:
        """GET a URL and parse the body as JSON."""
        import json

        return json.loads(await self._request("GET", url, **kwargs))

    async def _request(self, method: str, url: str, **kwargs: Any) -> bytes:
        if self._session is None:
            raise HttpError("HttpClient must be used as an async context manager")

        last_error: BaseException | None = None
        attempts = self.policy.max_retries + 1

        for attempt in range(attempts):
            await self.budget.acquire()
            proxy = await self.proxies.acquire()

            try:
                async with self._semaphore:
                    async with self._session.request(
                        method, url, proxy=proxy, **kwargs
                    ) as response:
                        if response.status in RETRY_STATUSES:
                            retry_after = _parse_retry_after(response.headers.get("Retry-After"))
                            # A 429 means this identity is too hot, so bench the
                            # proxy for as long as the server asked.
                            if response.status == 429:
                                await self.proxies.penalise(proxy, retry_after)
                            last_error = RateLimited(url, retry_after)
                            await self._sleep_before_retry(attempt, retry_after)
                            continue

                        response.raise_for_status()
                        return await response.read()

            except (TimeoutError, aiohttp.ClientError) as exc:
                last_error = exc
                await self.proxies.penalise(proxy)
                await self._sleep_before_retry(attempt, None)

        raise RetriesExhausted(url, attempts, last_error)

    async def _sleep_before_retry(self, attempt: int, retry_after: float | None) -> None:
        """Exponential backoff with jitter, or whatever the server asked for.

        The jitter matters: without it, a batch of concurrent requests that all
        get 429'd retry in lockstep and trip the limit again together.
        """
        if attempt >= self.policy.max_retries:
            return

        if retry_after is not None:
            delay = min(retry_after, self.policy.backoff_max)
        else:
            delay = min(self.policy.backoff_base * (2**attempt), self.policy.backoff_max)
            delay *= 0.5 + random.random()  # noqa: S311 — jitter, not cryptography

        logger.debug("retry %d in %.2fs", attempt + 1, delay)
        await asyncio.sleep(delay)


def _parse_retry_after(value: str | None) -> float | None:
    """Parse a Retry-After header. Only the delta-seconds form is handled."""
    if not value:
        return None
    try:
        return max(0.0, float(value))
    except ValueError:
        return None


def _redact(proxy: str) -> str:
    """Strip credentials so proxy URLs are safe to log."""
    if "@" in proxy:
        return proxy.split("@", 1)[1]
    return proxy

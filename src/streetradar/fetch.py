"""The single outbound HTTP path.

Every request the engine makes goes through `Fetcher`. It is the only place that
knows about concurrency ceilings, request budgets, retries and per-host
statistics, so a collector that reaches for `aiohttp` directly silently opts out
of all of them.

Three deliberate omissions:

* **No proxy support.** Distributing requests across several IPs has exactly one
  effect — making per-IP limits not apply — which is detection evasion rather
  than politeness. Rate limiting is the supported way to be a good citizen here.
* **No unbounded concurrency.** `max_concurrency` has no "0 means unlimited"
  escape hatch; the previous engine defaulted to 1000 against an undocumented
  endpoint.
* **No silent success.** A request that exhausts its retries raises. Callers that
  treat "absent" as ordinary say so with `missing_ok=True`.
"""

from __future__ import annotations

import asyncio
import os
import random
import time
from collections import Counter, deque
from collections.abc import AsyncIterator, Mapping
from contextlib import asynccontextmanager
from dataclasses import dataclass, field
from types import TracebackType
from typing import Any, Self

import aiohttp

__all__ = [
    "DEFAULT_USER_AGENT",
    "FetchError",
    "FetchPolicy",
    "Fetcher",
    "OutboundBlocked",
    "RateBudget",
    "RetriesExhausted",
    "Stats",
    "StatusError",
]

DEFAULT_USER_AGENT = "StreetRadarBot/1.0 (+https://streetradar.app/bot)"

# 408 and 425 are transient client-side conditions, 429 is explicit rate
# limiting, 5xx is the server's problem and usually passes. Everything else
# outside 2xx is definitive and retrying it only burns budget.
RETRY_STATUSES = frozenset({408, 425, 429, 500, 502, 503, 504})

_OUTBOUND_FLAG = "ENGINE_ALLOW_OUTBOUND"


class FetchError(RuntimeError):
    """Base class for failures raised by this module."""


class OutboundBlocked(FetchError):
    """Outbound requests are not enabled in this environment."""

    def __init__(self) -> None:
        super().__init__(
            f"Outbound requests are disabled. Set {_OUTBOUND_FLAG}=1 to enable them. "
            "This guard exists so that a stray command on a development machine "
            "cannot reach a provider: one outbound identity, one budget."
        )


class StatusError(FetchError):
    """A definitive non-retryable HTTP status, such as 403 or 404."""

    def __init__(self, url: str, status: int) -> None:
        super().__init__(f"HTTP {status} for {url}")
        self.url = url
        self.status = status


class RetriesExhausted(FetchError):
    """Every attempt failed. Carries the last error for context."""

    def __init__(self, url: str, attempts: int, last_error: BaseException | None) -> None:
        super().__init__(f"{attempts} attempts failed for {url}: {last_error}")
        self.url = url
        self.attempts = attempts
        self.last_error = last_error


def outbound_allowed() -> bool:
    """Whether this environment may issue provider requests."""
    return os.getenv(_OUTBOUND_FLAG, "0").strip() not in ("", "0", "false", "False")


@dataclass(frozen=True)
class FetchPolicy:
    """How hard we are willing to push one host."""

    max_concurrency: int = 8
    #: Sliding-window ceiling. None means "concurrency is the only limit".
    requests_per_hour: int | None = None
    max_retries: int = 3
    backoff_base: float = 0.5
    backoff_max: float = 30.0
    timeout: float = 20.0
    user_agent: str = DEFAULT_USER_AGENT
    headers: Mapping[str, str] = field(default_factory=dict)

    def __post_init__(self) -> None:
        if self.max_concurrency < 1:
            raise ValueError("max_concurrency must be at least 1")
        if self.requests_per_hour is not None and self.requests_per_hour < 1:
            raise ValueError("requests_per_hour must be positive or None")


class RateBudget:
    """Sliding-window limiter, in requests per hour.

    A sliding window rather than a token bucket because provider limits are
    expressed as "N per hour", and a bucket lets you spend the whole hour's
    allowance in the first minute — which is exactly the burst that gets an IP
    banned.
    """

    def __init__(self, requests_per_hour: int | None, window: float = 3600.0) -> None:
        self._limit = requests_per_hour
        self._window = window
        self._events: deque[float] = deque()
        self._lock = asyncio.Lock()

    @property
    def spent(self) -> int:
        """Requests recorded inside the current window."""
        return len(self._events)

    async def acquire(self) -> float:
        """Block until a request is permitted. Returns seconds spent waiting."""
        if self._limit is None:
            return 0.0

        waited = 0.0
        while True:
            async with self._lock:
                now = time.monotonic()
                while self._events and now - self._events[0] >= self._window:
                    self._events.popleft()

                if len(self._events) < self._limit:
                    self._events.append(now)
                    return waited

                wait = self._window - (now - self._events[0])

            pause = min(wait, 30.0)
            waited += pause
            await asyncio.sleep(pause)


@dataclass
class Stats:
    """What a run did, in enough detail to diagnose a slow one without logs.

    `by_status` separates "the tile is not there" from "we are being blocked",
    which the previous engine conflated into a single `skipped_tiles` counter and
    thereby made coverage density unknowable.
    """

    by_status: Counter[int] = field(default_factory=Counter)
    requests: int = 0
    retries: int = 0
    timeouts: int = 0
    errors: int = 0
    empty: int = 0
    bytes_in: int = 0
    rate_limited: int = 0
    seconds_parked: float = 0.0
    started_at: float = field(default_factory=time.monotonic)

    @property
    def elapsed(self) -> float:
        return time.monotonic() - self.started_at

    @property
    def per_second(self) -> float:
        return self.requests / self.elapsed if self.elapsed > 0 else 0.0

    def summary(self) -> str:
        statuses = " ".join(f"{s}:{n}" for s, n in sorted(self.by_status.items()))
        return (
            f"{self.requests} requests in {self.elapsed:.1f}s "
            f"({self.per_second:.1f}/s) | {statuses or 'no responses'} | "
            f"empty={self.empty} retries={self.retries} timeouts={self.timeouts} "
            f"errors={self.errors} 429s={self.rate_limited} "
            f"parked={self.seconds_parked:.1f}s | "
            f"{self.bytes_in / 1_048_576:.1f} MiB"
        )


class Fetcher:
    """Async HTTP client applying one `FetchPolicy` to every request."""

    def __init__(self, policy: FetchPolicy | None = None, *, require_flag: bool = True) -> None:
        self.policy = policy or FetchPolicy()
        self.stats = Stats()
        self.budget = RateBudget(self.policy.requests_per_hour)
        self._semaphore = asyncio.Semaphore(self.policy.max_concurrency)
        self._session: aiohttp.ClientSession | None = None
        self._require_flag = require_flag

    async def __aenter__(self) -> Self:
        if self._require_flag and not outbound_allowed():
            raise OutboundBlocked
        headers = {"User-Agent": self.policy.user_agent, **dict(self.policy.headers)}
        self._session = aiohttp.ClientSession(
            timeout=aiohttp.ClientTimeout(total=self.policy.timeout),
            headers=headers,
            # Bounded to the policy so aiohttp's own pool cannot become a second,
            # invisible concurrency limit.
            connector=aiohttp.TCPConnector(limit=self.policy.max_concurrency),
        )
        self.stats = Stats()
        return self

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        tb: TracebackType | None,
    ) -> None:
        if self._session is not None:
            await self._session.close()
            self._session = None

    @property
    def concurrency(self) -> int:
        """Part of the narrow surface `streetradar.tiles` depends on."""
        return self.policy.max_concurrency

    async def fetch_tile(self, url: str, headers: Mapping[str, str]) -> bytes | None:
        """GET a tile, treating 404 as an ordinary absent tile.

        This and `concurrency` are the whole contract the tile harvesters need,
        which keeps them testable without a network or a real session.
        """
        return await self.get(url, missing_ok=True, headers=dict(headers))

    async def post(self, url: str, body: bytes, *, headers: Mapping[str, str]) -> bytes:
        """POST a body and return the response.

        Here because Mapy speaks FRPC — a binary RPC over HTTP POST — and the
        alternative was letting `pyfrpc` own its own session, which would put one
        provider outside the budget, the retries and the metrics that every other
        request passes through. The codec stays upstream's; the request is ours.
        """
        return await self._request("POST", url, data=body, headers=dict(headers))

    @asynccontextmanager
    async def guard(self) -> AsyncIterator[None]:
        """Apply the budget and concurrency ceiling to a call we do not issue."""
        self.stats.seconds_parked += await self.budget.acquire()
        async with self._semaphore:
            yield

    async def get(self, url: str, *, missing_ok: bool = False, **kwargs: Any) -> bytes | None:
        """GET a URL and return the body.

        With `missing_ok`, a 404 returns None instead of raising. Tile grids are
        sparse — most coordinates in a bounding box hold no imagery — so for a
        tile sweep "not there" is an ordinary answer, not a failure.
        """
        try:
            return await self._request("GET", url, **kwargs)
        except StatusError as exc:
            if missing_ok and exc.status == 404:
                return None
            raise

    async def _request(self, method: str, url: str, **kwargs: Any) -> bytes:
        if self._session is None:
            raise FetchError("Fetcher must be used as an async context manager")

        last_error: BaseException | None = None
        attempts = self.policy.max_retries + 1

        for attempt in range(attempts):
            self.stats.seconds_parked += await self.budget.acquire()

            try:
                async with self._semaphore:
                    self.stats.requests += 1
                    async with self._session.request(method, url, **kwargs) as response:
                        self.stats.by_status[response.status] += 1

                        if response.status in RETRY_STATUSES:
                            if response.status == 429:
                                self.stats.rate_limited += 1
                            retry_after = _parse_retry_after(response.headers.get("Retry-After"))
                            last_error = StatusError(url, response.status)
                            self.stats.retries += 1
                            await self._sleep_before_retry(attempt, retry_after)
                            continue

                        if response.status >= 400:
                            raise StatusError(url, response.status)

                        body = await response.read()
                        self.stats.bytes_in += len(body)
                        if not body:
                            self.stats.empty += 1
                        return body

            except TimeoutError as exc:
                last_error = exc
                self.stats.timeouts += 1
                self.stats.retries += 1
                await self._sleep_before_retry(attempt, None)
            except aiohttp.ClientError as exc:
                last_error = exc
                self.stats.errors += 1
                self.stats.retries += 1
                await self._sleep_before_retry(attempt, None)

        raise RetriesExhausted(url, attempts, last_error)

    async def _sleep_before_retry(self, attempt: int, retry_after: float | None) -> None:
        """Exponential backoff with jitter, or whatever the server asked for.

        The jitter matters: without it a batch of concurrent requests that all
        get 429'd retry in lockstep and trip the limit again together.
        """
        if attempt >= self.policy.max_retries:
            return

        if retry_after is not None:
            delay = min(retry_after, self.policy.backoff_max)
        else:
            delay = min(self.policy.backoff_base * (2**attempt), self.policy.backoff_max)
            delay *= 0.5 + random.random()  # noqa: S311 — jitter, not cryptography

        self.stats.seconds_parked += delay
        await asyncio.sleep(delay)


def _parse_retry_after(value: str | None) -> float | None:
    """Parse a Retry-After header. Only the delta-seconds form is handled."""
    if not value:
        return None
    try:
        return max(0.0, float(value))
    except ValueError:
        return None

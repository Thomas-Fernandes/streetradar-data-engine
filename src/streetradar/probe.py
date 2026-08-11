"""Finding out what a host actually tolerates, before a collector assumes it.

Invariant 8: a bounded probe precedes each collector. This is that probe, made
reusable — every provider so far has had its enumeration strategy measured rather
than inherited, and the sustainable request rate is the one number that has
always been guessed instead.

## Why this cannot be answered from a development machine

Rate limits are per-IP. A ceiling measured from a laptop describes that laptop's
IP, not the VPS's, and the two differ in reputation, geography and latency to the
edge. Worse, the only way to find a ceiling is to push until something breaks —
so the measurement spends exactly the resource it is trying to protect. Doing
that from a second machine means burning an IP that will never run the
collection, and it leaves the provider seeing two identities where the whole
outbound design promises one.

So this runs where the collector runs, once, with a budget.

## What it measures

An escalating ladder of concurrency levels, each held long enough that the
average is not a warm-up artefact. Per level:

* sustained requests per second, over the steady portion only
* error and timeout rates, which is where a ceiling announces itself
* the share of responses that are empty — for Mapy the coverage layer answers
  `302 -> ./default` for a tile with nothing in it, which costs 9 bytes and is
  worth counting separately from a real miss
* latency percentiles, because throttling usually shows up as latency long
  before it shows up as errors

It stops early on the first clear sign of refusal rather than continuing to the
budget, since the point is to find the edge, not to stand on it.
"""

from __future__ import annotations

import asyncio
import time
from collections.abc import Iterable, Sequence
from dataclasses import dataclass, field
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from .tiles import Tile, TileFetcher, TileSource

__all__ = ["LadderResult", "LevelResult", "climb", "probe_level"]

#: Concurrency levels to try, each roughly doubling. Starts below the rate the
#: previous engine already sustained against Mapy (142/s over 31.7 hours) so the
#: first level is known-safe and calibrates the measurement.
DEFAULT_LADDER: Sequence[int] = (8, 16, 32, 64, 128, 256, 512)

#: Long enough that connection setup and TLS handshakes are amortised. The
#: earlier Apple measurement was wrong by 26x precisely because it timed 150
#: requests per level and measured handshakes rather than throughput.
DEFAULT_SECONDS = 30.0


@dataclass
class LevelResult:
    """What one rung of the ladder produced."""

    concurrency: int
    seconds: float
    requests: int
    errors: int
    timeouts: int
    empty: int
    bytes_in: int
    latencies: list[float] = field(default_factory=list)

    @property
    def rate(self) -> float:
        return self.requests / self.seconds if self.seconds else 0.0

    @property
    def error_rate(self) -> float:
        return (self.errors + self.timeouts) / self.requests if self.requests else 0.0

    @property
    def empty_rate(self) -> float:
        return self.empty / self.requests if self.requests else 0.0

    def percentile(self, p: float) -> float:
        if not self.latencies:
            return 0.0
        ordered = sorted(self.latencies)
        return ordered[min(len(ordered) - 1, int(len(ordered) * p))]

    @property
    def refused(self) -> bool:
        """Whether this level looks like the host pushing back.

        A 2% error rate is generous for a CDN serving static tiles — the previous
        engine measured 0.09% against Mapy over 16.2 million requests — so
        anything above it is a signal rather than noise.
        """
        return self.error_rate > 0.02


@dataclass
class LadderResult:
    levels: list[LevelResult] = field(default_factory=list)
    stopped_early: bool = False

    @property
    def best(self) -> LevelResult | None:
        """The fastest level that did not look refused."""
        usable = [level for level in self.levels if not level.refused]
        return max(usable, key=lambda level: level.rate, default=None)

    def report(self) -> str:
        rows = [
            f"{'conc':>6} {'req/s':>9} {'requests':>10} {'err':>7} {'empty':>7} "
            f"{'p50 ms':>8} {'p95 ms':>8}"
        ]
        rows.append("-" * 60)
        for level in self.levels:
            rows.append(
                f"{level.concurrency:>6} {level.rate:>9,.1f} {level.requests:>10,} "
                f"{level.error_rate:>6.2%} {level.empty_rate:>6.1%} "
                f"{level.percentile(0.5) * 1000:>8.0f} {level.percentile(0.95) * 1000:>8.0f}"
            )
        if self.best is not None:
            rows.append(
                f"\nsustainable: {self.best.rate:,.0f} req/s at concurrency {self.best.concurrency}"
            )
        if self.stopped_early:
            rows.append("stopped early: the host started refusing")
        return "\n".join(rows)


async def probe_level(
    fetcher: TileFetcher,
    source: TileSource,
    tiles: Iterable[Tile],
    *,
    concurrency: int,
    seconds: float = DEFAULT_SECONDS,
    budget: int | None = None,
) -> LevelResult:
    """Hold one concurrency level for `seconds`, and report what it sustained.

    Tiles are consumed from `tiles`; supply a stream long enough that the level
    never runs dry, and one that does not repeat, or the host's cache will
    flatter the result.
    """
    result = LevelResult(
        concurrency=concurrency, seconds=0.0, requests=0, errors=0, timeouts=0, empty=0, bytes_in=0
    )
    stream = iter(tiles)
    deadline = time.monotonic() + seconds
    lock = asyncio.Lock()
    started = time.monotonic()

    async def worker() -> None:
        while time.monotonic() < deadline:
            if budget is not None and result.requests >= budget:
                return
            async with lock:
                tile = next(stream, None)
            if tile is None:
                return
            began = time.monotonic()
            try:
                body = await fetcher.fetch_tile(source.url(tile), source.headers(tile))
            except TimeoutError:
                result.timeouts += 1
                result.requests += 1
                continue
            except Exception:  # noqa: BLE001 — the probe's job is to survive and count
                result.errors += 1
                result.requests += 1
                continue
            result.latencies.append(time.monotonic() - began)
            result.requests += 1
            if body:
                result.bytes_in += len(body)
            else:
                result.empty += 1

    await asyncio.gather(*(worker() for _ in range(concurrency)))
    result.seconds = time.monotonic() - started
    return result


async def climb(
    fetcher: TileFetcher,
    source: TileSource,
    tiles: Iterable[Tile],
    *,
    ladder: Sequence[int] = DEFAULT_LADDER,
    seconds: float = DEFAULT_SECONDS,
    budget: int = 2000,
    pause: float = 5.0,
) -> LadderResult:
    """Climb the ladder until the host refuses or the budget runs out.

    `budget` is a hard ceiling on total requests, defaulting to the 2,000 that
    the project's own rule allows a bounded probe. A ladder of seven levels at
    30 seconds each will reach it long before it reaches the top rung on any host
    worth probing, which is the intended behaviour: the budget binds, not the
    ladder.

    `pause` between levels lets a sliding-window limiter recover, so a level is
    not judged on the previous level's debt.
    """
    outcome = LadderResult()
    spent = 0
    stream = iter(tiles)

    for concurrency in ladder:
        if spent >= budget:
            break
        level = await probe_level(
            fetcher,
            source,
            stream,
            concurrency=concurrency,
            seconds=seconds,
            budget=budget - spent,
        )
        outcome.levels.append(level)
        spent += level.requests

        if level.refused:
            outcome.stopped_early = True
            break
        if not level.requests:
            break
        await asyncio.sleep(pause)

    return outcome

"""The run loop: one (provider, scope) job, from collection to a published archive.

This is the reusable unit a Prefect task wraps — the whole of one country's work
for one provider, in the order the invariants require. A collector is driven to
write parquet parts, the parts are tiled into one archive, the archive passes the
publish gate, and only then does `latest.json` point at it. Nothing about *which*
provider or *which* endpoints lives here: the caller wires the collector and the
feature stream, so the same loop drives Já's pyramid descent, Apple's flood and
Google's per-country jobs unchanged.

## The order is the safety property

Parts are written and made durable before anything downstream runs; for a flood
provider the `snapshot` callback persists collection state *after* each part is on
disk, so a crash resumes from the last durable part rather than re-fetching the
country (design invariant 3). Tiling reads the whole run's parts locally — no
round trip through R2 — and the gate recomputes the stats off what tippecanoe
actually wrote before the pointer moves (invariants 4 and 5).

## Disk stays bounded

A country's parts fit on the VPS, but the world does not, so once the archive is
published — its durable copy now on R2 — the local parts and the local archive are
discarded. Steady state is one country's working set, not an accumulation, and a
part is re-derivable from the packed raw if a tiling bug is ever found (ADR 0005).
Tiling and publishing are offloaded to threads so a job that is tiling does not
stall another job that is still fetching.
"""

from __future__ import annotations

import asyncio
from dataclasses import dataclass
from typing import TYPE_CHECKING

from .publish import publish
from .tileset import build_tileset

if TYPE_CHECKING:
    from collections.abc import AsyncIterator, Callable, Iterable
    from pathlib import Path

    from .publish import PublishResult
    from .r2 import R2Client
    from .tileset import TilesetSpec, TilesetStats

__all__ = ["RunResult", "run_job"]


@dataclass(frozen=True)
class RunResult:
    """What one job did — enough for the orchestrator to log and for status.json.

    `published` is None when the country yielded no coverage: a legitimate
    outcome, not a failure, so it is reported rather than raised. A run that
    collected coverage but failed the gate raises instead of returning.
    """

    provider: str
    scope: str
    run_id: str
    parts: int
    published: PublishResult | None
    stats: TilesetStats | None
    note: str


def _discard(parts_dir: Path, archive: Path) -> None:
    for part in parts_dir.glob("*.parquet"):
        part.unlink(missing_ok=True)
    archive.unlink(missing_ok=True)


async def run_job(
    r2: R2Client,
    *,
    provider: str,
    scope: str,
    run_id: str,
    parts: AsyncIterator[Path],
    parts_dir: Path,
    features: Callable[[Path], Iterable[bytes]],
    spec: TilesetSpec,
    archive_dir: Path,
    published_at: str,
    snapshot: Callable[[], None] | None = None,
) -> RunResult:
    """Drive one collector to a published tileset. Returns what it did.

    `parts` is the wired collector, writing its parquet into `parts_dir`;
    `features` turns that directory into the tippecanoe feature stream
    (`line_features` for Já and Naver, `observation_features` for the point
    providers). `snapshot`, when given, persists collection state after each part
    is durable — the flood providers pass it, Já does not need it.
    """
    collected = 0
    async for _part in parts:
        collected += 1
        if snapshot is not None:
            snapshot()

    if not any(parts_dir.glob("*.parquet")):
        return RunResult(provider, scope, run_id, 0, None, None, "no coverage collected")

    archive_dir.mkdir(parents=True, exist_ok=True)
    archive = archive_dir / f"{provider}-{scope}-{run_id}.pmtiles"
    stats = await asyncio.to_thread(build_tileset, features(parts_dir), archive, spec)
    result = await asyncio.to_thread(
        publish,
        r2,
        provider=provider,
        scope=scope,
        run_id=run_id,
        archive=archive,
        stats=stats,
        spec=spec,
        published_at=published_at,
    )
    _discard(parts_dir, archive)
    return RunResult(provider, scope, run_id, collected, result, stats, "published")

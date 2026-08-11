"""Per-provider job wiring: assemble one provider's collector onto the run loop.

`run.run_job` is provider-agnostic — it drives a collector, tiles, gates and
publishes. This is where each provider is bolted onto it: which collector, which
source, which feature stream, which scope. The result is a coroutine per provider
that the CLI and the orchestrator both call with the same shared context.

Only Já is wired here so far — it is the smallest provider and the one the shared
infrastructure is validated against end to end. Naver (its line twin) and the
point providers (Apple, Mapy, Bing, Google) attach the same way: swap the
collector and the feature stream, and per-country providers pass a real scope
instead of the single national one.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING

from .lines import LineBatch, collect_lines
from .run import run_job
from .sources import JA_360, resolve_source
from .store import PartWriter
from .tileset import TilesetSpec, line_features, read_line_parts

if TYPE_CHECKING:
    from collections.abc import Iterable
    from pathlib import Path

    from .fetch import Fetcher
    from .r2 import R2Client
    from .run import RunResult

__all__ = ["JobContext", "UnknownProvider", "run_provider"]

#: Já and Naver serve standard MVT, whose default coordinate extent is 4096; the
#: decoder confirms it per tile and the writer refuses a tile that disagrees.
_MVT_EXTENT = 4096

#: The line layer is cut over the full serving range; the collection zoom (the
#: provider's own maximum) is a separate, deeper number.
_LINE_SPEC = TilesetSpec(layer="coverage", min_zoom=0, max_zoom=16)


class UnknownProvider(RuntimeError):
    """No job is wired for this provider id."""


@dataclass(frozen=True)
class JobContext:
    """Everything a job needs that is not specific to the provider.

    The fetcher is already entered (its outbound gate has passed), the R2 client
    is configured, and `run_id`/`published_at` are stamped once by the caller so a
    run and its published record agree on their identity and time.
    """

    fetcher: Fetcher
    r2: R2Client
    data_dir: Path
    run_id: str
    published_at: str


def _line_features(directory: Path) -> Iterable[bytes]:
    return line_features(read_line_parts(directory))


async def run_ja(ctx: JobContext, scope: str = "is") -> RunResult:
    """Collect Já's driveline coverage for Iceland and publish it.

    Já publishes a tile pyramid, so the collector is a clipped descent with no
    resume state — re-running it re-derives the country from the source. The scope
    is the whole nation; Já has no sub-national split.
    """
    provider = JA_360
    resolved = await resolve_source(ctx.fetcher, provider)
    run_dir = ctx.data_dir / provider.id / scope / ctx.run_id
    parts_dir = run_dir / "parts"

    writer = PartWriter(
        parts_dir,
        LineBatch(
            provider=provider.id,
            run_id=ctx.run_id,
            zoom=provider.target_zoom,
            extent=_MVT_EXTENT,
        ),
    )
    parts = collect_lines(
        ctx.fetcher,
        resolved.source,
        layer=resolved.layer,
        target_zoom=provider.target_zoom,
        writer=writer,
        start_zoom=provider.descent_start,
    )
    return await run_job(
        ctx.r2,
        provider=provider.id,
        scope=scope,
        run_id=ctx.run_id,
        parts=parts,
        parts_dir=parts_dir,
        features=_line_features,
        spec=_LINE_SPEC,
        archive_dir=run_dir,
        published_at=ctx.published_at,
    )


async def run_provider(ctx: JobContext, provider_id: str, scope: str | None = None) -> RunResult:
    """Dispatch to the wired job for a provider, or refuse an unwired one."""
    if provider_id == "ja":
        return await run_ja(ctx, scope or "is")
    raise UnknownProvider(f"no job is wired for {provider_id!r}; only 'ja' is wired so far")

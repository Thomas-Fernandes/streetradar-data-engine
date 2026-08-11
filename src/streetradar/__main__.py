"""The engine's command line: collect one provider's coverage and publish it.

    python -m streetradar collect ja
    python -m streetradar collect google --scope lu

One invocation is one (provider, scope) run — the same unit the orchestrator
schedules, runnable by hand for a single country. It stamps the run id and time
once, opens the one gated outbound path, and hands off to the wired job.

Nothing here bypasses a guard. The fetcher refuses to open unless
`ENGINE_ALLOW_OUTBOUND` is set, so on a dev machine this command stops before it
touches a provider and says why; the real run is the VPS's. The R2 credentials
and the tiling binaries are likewise reported by name when missing, rather than
failing deep in a traceback.
"""

from __future__ import annotations

import argparse
import asyncio
import os
import sys
from datetime import UTC, datetime
from pathlib import Path

from dotenv import load_dotenv

from . import __version__
from .fetch import Fetcher, OutboundBlocked
from .jobs import JobContext, UnknownProvider, run_provider
from .r2 import R2Client, R2Error
from .tileset import ToolMissing


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="streetradar", description=__doc__)
    parser.add_argument("--version", action="version", version=f"streetradar {__version__}")
    sub = parser.add_subparsers(dest="command", required=True)

    collect = sub.add_parser("collect", help="collect and publish one provider and scope")
    collect.add_argument("provider", help="provider id, e.g. 'ja'")
    collect.add_argument(
        "--scope",
        default=None,
        help="country scope for a per-country provider; the provider's default if omitted",
    )
    return parser


async def _collect(provider_id: str, scope: str | None) -> int:
    data_dir = Path(os.getenv("STREETRADAR_DATA_DIR", "data"))
    now = datetime.now(UTC)
    run_id = now.strftime("%Y%m%dT%H%M%SZ")
    published_at = now.isoformat()

    try:
        async with Fetcher() as fetcher:
            r2 = R2Client()
            ctx = JobContext(fetcher, r2, data_dir, run_id, published_at)
            result = await run_provider(ctx, provider_id, scope)
    except OutboundBlocked:
        print(
            "outbound is disabled here: set ENGINE_ALLOW_OUTBOUND=1 to issue provider "
            "traffic. This is expected on a dev machine; collection runs on the VPS.",
            file=sys.stderr,
        )
        return 2
    except (R2Error, UnknownProvider, ToolMissing) as error:
        print(str(error), file=sys.stderr)
        return 1

    if result.published is None:
        print(f"{provider_id}/{result.scope} {run_id}: {result.note}")
        return 0
    print(
        f"{provider_id}/{result.scope} {run_id}: published "
        f"{result.stats.tiles if result.stats else 0} tiles -> {result.published.url}"
    )
    if result.published.pruned:
        print(f"  pruned {len(result.published.pruned)} superseded archive(s)")
    return 0


def main(argv: list[str] | None = None) -> int:
    load_dotenv()
    args = _build_parser().parse_args(argv)
    if args.command == "collect":
        return asyncio.run(_collect(args.provider, args.scope))
    return 1  # unreachable: argparse requires a known subcommand


if __name__ == "__main__":
    sys.exit(main())

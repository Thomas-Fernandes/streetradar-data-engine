"""The Prefect flows: the engine's scheduled shape (ADR 0003, ADR 0018).

One flow run is one (provider, scope) job — exactly the unit `jobs.run_provider`
defines and the CLI runs by hand. Prefect adds what a bare command lacks: state,
timeouts, retries on deliberate schedules, an audit trail in the UI, and alert
hooks that fire on failure. The flow body stays thin on purpose; everything real
lives in `jobs` and below, which is why the engine remains runnable without an
orchestrator at all (`streetradar collect ja`).

## Topology

`prefect-server` (SQLite backend) and a runner executing `python -m
streetradar.flows` are two compose services built from the one engine image.
`main` *serves* the deployments — Prefect 3's `serve()` polls the API from
inside the runner, so there is no work pool and no worker registration, the
lightest self-hosted shape. Deployments carry no schedule yet: collections are
deliberate campaigns, triggered from the UI or `prefect deployment run`.

## Where the shard model fits

ADR 0003's rule — a collection is never a single long flow run — binds the big
flood providers (Apple, Google), whose collectors carry resume state for exactly
that reason. Their sharded flows arrive when they are wired into `jobs`. Já has
no resume state because a full run is minutes of work, so its flow simply runs
the country end to end, inside the same 45-minute ceiling a shard gets.
"""

from __future__ import annotations

import os
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, cast

from prefect import Flow, flow, serve
from prefect.client.schemas.objects import FlowRun, State
from prefect.flows import FlowStateHook

from .alerts import notify
from .fetch import Fetcher
from .jobs import JobContext, run_provider
from .r2 import R2Client

__all__ = ["collect", "main"]

#: ADR 0003's ceiling for one bounded run. A hung fetch or a wedged tippecanoe
#: is killed here and reported, rather than holding the schedule forever.
SHARD_TIMEOUT_SECONDS = 45 * 60


def _alert_failure(flow_object: Flow[..., Any], flow_run: FlowRun, state: State[Any]) -> None:
    """Failure hook: one Discord line naming the job and what the state says."""
    parameters = flow_run.parameters or {}
    job = f"{parameters.get('provider', '?')}/{parameters.get('scope') or 'default'}"
    notify(
        f":rotating_light: **{job}** {state.type.value.lower()}: {state.message or 'no message'}"
    )


# The hook signature matches FlowStateHook.__call__ exactly; the cast exists
# because the protocol also demands `__name__`, which mypy cannot see on a bare
# callable inside a list even though every plain function carries one.
_ALERT_HOOKS: list[FlowStateHook[Any, Any]] = [cast("FlowStateHook[Any, Any]", _alert_failure)]


@flow(
    name="collect",
    timeout_seconds=SHARD_TIMEOUT_SECONDS,
    # Retries stay at zero: a failed collection is investigated, not hammered
    # against a provider. Re-running is a deliberate act, like deploying.
    retries=0,
    on_failure=_ALERT_HOOKS,
    on_crashed=_ALERT_HOOKS,
)
async def collect(provider: str, scope: str | None = None) -> str:
    """Collect one (provider, scope) and publish it. Returns a one-line summary.

    The same body as the CLI's collect command: stamp the run identity once,
    open the one gated outbound path, hand off to the wired job. Success is
    announced on Discord too — runs are rare, deliberate campaigns, so a ping
    per publish is signal, not noise.
    """
    data_dir = Path(os.getenv("STREETRADAR_DATA_DIR", "data"))
    now = datetime.now(UTC)
    run_id = now.strftime("%Y%m%dT%H%M%SZ")

    async with Fetcher() as fetcher:
        ctx = JobContext(fetcher, R2Client(), data_dir, run_id, now.isoformat())
        result = await run_provider(ctx, provider, scope)

    if result.published is None:
        summary = f"{provider}/{result.scope} {run_id}: {result.note}"
    else:
        tiles = result.stats.tiles if result.stats else 0
        summary = (
            f"{provider}/{result.scope} {run_id}: published {tiles} tiles -> {result.published.url}"
        )
    notify(f":white_check_mark: {summary}")
    return summary


def main() -> None:
    """Serve the deployments; blocks. This is the runner container's process.

    One deployment per wired provider. Per-country providers will add one
    deployment per (provider, country) here, which is the owner's intended
    shape: jobs by provider, and by country where the provider is large.
    """
    from prefect.deployments.runner import RunnerDeployment

    # `to_deployment` is sync/async dual-dispatch; called from a plain sync
    # entry point it returns the deployment itself, which the isinstance both
    # asserts and narrows.
    deployment = collect.to_deployment(name="ja", parameters={"provider": "ja"})
    assert isinstance(deployment, RunnerDeployment)
    serve(deployment)


if __name__ == "__main__":
    main()

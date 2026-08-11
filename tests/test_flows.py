"""The Prefect flows, tested without a Prefect server.

`collect.fn` is the undecorated coroutine, so the body runs directly — no API,
no ephemeral server, fast. What the orchestration layer itself would do (states,
timeouts) is Prefect's to test; what is ours is the wiring: the flow builds the
context once, hands it to `run_provider`, and tells Discord what happened.
"""

from __future__ import annotations

import re
from pathlib import Path
from types import SimpleNamespace, TracebackType
from typing import Any, cast

import pytest
from prefect.client.schemas.objects import FlowRun
from prefect.states import Failed

from streetradar import flows as flows_module
from streetradar.flows import SHARD_TIMEOUT_SECONDS, _alert_failure, collect
from streetradar.publish import PublishResult
from streetradar.run import RunResult
from streetradar.tileset import TilesetStats, ZoomStats


class FakeFetcher:
    async def __aenter__(self) -> FakeFetcher:
        return self

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        tb: TracebackType | None,
    ) -> None:
        return None


def _wire(
    monkeypatch: pytest.MonkeyPatch,
    result_for: Any,
) -> tuple[list[Any], list[str]]:
    """Replace the flow's collaborators; return what it captured and notified."""
    captured: list[Any] = []
    messages: list[str] = []

    async def fake_run_provider(ctx: Any, provider: str, scope: str | None = None) -> Any:
        captured.append((ctx, provider, scope))
        return result_for

    def fake_notify(message: str) -> bool:
        messages.append(message)
        return True

    monkeypatch.setattr(flows_module, "Fetcher", FakeFetcher)
    monkeypatch.setattr(flows_module, "R2Client", lambda: object())
    monkeypatch.setattr(flows_module, "run_provider", fake_run_provider)
    monkeypatch.setattr(flows_module, "notify", fake_notify)
    return captured, messages


class TestCollectFlow:
    async def test_it_stamps_one_identity_and_hands_off(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        monkeypatch.setenv("STREETRADAR_DATA_DIR", str(tmp_path))
        result = RunResult("ja", "is", "r", 0, None, None, "no coverage collected")
        captured, messages = _wire(monkeypatch, result)

        summary = await collect.fn("ja")

        ctx, provider, scope = captured[0]
        assert provider == "ja" and scope is None
        assert ctx.data_dir == tmp_path
        # One identity, stamped once: the run id is a UTC timestamp and the
        # published_at carries the same moment.
        assert re.fullmatch(r"\d{8}T\d{6}Z", ctx.run_id)
        assert ctx.published_at.startswith(ctx.run_id[:4])
        assert "no coverage collected" in summary
        assert messages and messages[0].startswith(":white_check_mark:")

    async def test_a_published_run_reports_tiles_and_url(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        monkeypatch.setenv("STREETRADAR_DATA_DIR", str(tmp_path))
        stats = TilesetStats(tiles=59, bytes=1000, by_zoom=(ZoomStats(16, 59, 1000, 100),))
        published = PublishResult(
            key="tiles/ja/is/r.pmtiles",
            url="https://tiles.streetradar.app/tiles/ja/is/r.pmtiles",
            previous_key=None,
            pruned=(),
        )
        result = RunResult("ja", "is", "r", 1, published, stats, "published")
        _, messages = _wire(monkeypatch, result)

        summary = await collect.fn("ja", "is")

        assert "59 tiles" in summary
        assert summary.endswith(published.url)
        assert messages[0].endswith(published.url)


class TestOrchestrationShape:
    def test_the_flow_is_bounded_by_the_shard_ceiling(self) -> None:
        assert collect.timeout_seconds == SHARD_TIMEOUT_SECONDS == 45 * 60

    def test_a_failed_run_is_never_retried_against_the_provider(self) -> None:
        assert collect.retries == 0

    def test_failure_and_crash_both_alert(self) -> None:
        assert _alert_failure in collect.on_failure_hooks
        assert _alert_failure in collect.on_crashed_hooks


class TestFailureHook:
    def test_it_names_the_job_and_the_state(self, monkeypatch: pytest.MonkeyPatch) -> None:
        messages: list[str] = []

        def fake_notify(message: str) -> bool:
            messages.append(message)
            return True

        monkeypatch.setattr(flows_module, "notify", fake_notify)

        flow_run = cast(FlowRun, SimpleNamespace(parameters={"provider": "ja", "scope": None}))
        _alert_failure(collect, flow_run, Failed(message="the gate refused the archive"))

        assert "ja/default" in messages[0]
        assert "failed" in messages[0]
        assert "the gate refused the archive" in messages[0]

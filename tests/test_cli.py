"""The command line and the per-provider job wiring.

The CLI tests are synchronous on purpose: `main` calls `asyncio.run`, which cannot
run inside another loop, so a plain `def` test gives it the clean loop it needs.
The wiring test stubs resolution, collection and the run loop, so `run_ja` is
checked for what it hands down — extent, zoom, layer, scope, spec — without a
network or a provider.
"""

from __future__ import annotations

from pathlib import Path
from typing import Any

import pytest
from fakes import make_client

from streetradar import __main__ as cli
from streetradar import jobs as jobs_module
from streetradar.fetch import Fetcher
from streetradar.jobs import JobContext, UnknownProvider, run_ja, run_provider
from streetradar.run import RunResult
from streetradar.sources import JA_360
from streetradar.tilejson import ResolvedSource
from streetradar.tiles import TileSource


def make_ctx(tmp_path: Path) -> JobContext:
    return JobContext(
        fetcher=Fetcher(require_flag=False),
        r2=make_client()[0],
        data_dir=tmp_path,
        run_id="RID",
        published_at="2026-08-11T09:00:00+00:00",
    )


class TestCli:
    def test_collect_stops_at_the_outbound_gate_on_a_dev_machine(
        self, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
    ) -> None:
        monkeypatch.setattr(cli, "load_dotenv", lambda: None)
        monkeypatch.delenv("ENGINE_ALLOW_OUTBOUND", raising=False)
        code = cli.main(["collect", "ja"])
        assert code == 2
        assert "outbound is disabled" in capsys.readouterr().err

    def test_version_exits_cleanly(self) -> None:
        with pytest.raises(SystemExit) as caught:
            cli.main(["--version"])
        assert caught.value.code == 0

    def test_no_subcommand_is_a_usage_error(self) -> None:
        with pytest.raises(SystemExit):
            cli.main([])


class TestDispatch:
    async def test_an_unwired_provider_is_refused(self, tmp_path: Path) -> None:
        with pytest.raises(UnknownProvider, match="bing"):
            await run_provider(make_ctx(tmp_path), "bing")


class TestJaWiring:
    async def test_run_ja_hands_the_right_pieces_to_the_run_loop(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        captured: dict[str, Any] = {}

        async def fake_resolve(fetcher: object, provider: object) -> ResolvedSource:
            source = TileSource(
                id="ja",
                url_template="https://tiles.example/{z}/{x}/{y}",
                min_zoom=6,
                max_zoom=15,
                bounds=(-24.6, 63.3, -13.4, 66.6),
            )
            return ResolvedSource(source=source, layer="driveline", fields={})

        def fake_collect_lines(fetcher: object, source: object, **kw: Any) -> Any:
            captured["collect"] = {"source": source, **kw}

            async def gen() -> Any:
                return
                yield  # pragma: no cover

            return gen()

        async def fake_run_job(r2: object, **kw: Any) -> RunResult:
            captured.update(kw)
            return RunResult(kw["provider"], kw["scope"], kw["run_id"], 0, None, None, "stub")

        monkeypatch.setattr(jobs_module, "resolve_source", fake_resolve)
        monkeypatch.setattr(jobs_module, "collect_lines", fake_collect_lines)
        monkeypatch.setattr(jobs_module, "run_job", fake_run_job)

        result = await run_ja(make_ctx(tmp_path))

        assert result.note == "stub"
        assert captured["provider"] == "ja" and captured["scope"] == "is"
        assert captured["spec"].layer == "coverage" and captured["spec"].max_zoom == 16
        assert captured["parts_dir"] == tmp_path / "ja" / "is" / "RID" / "parts"

        collect = captured["collect"]
        assert collect["layer"] == "driveline"
        assert collect["target_zoom"] == JA_360.target_zoom
        assert collect["start_zoom"] == JA_360.descent_start
        # The writer must record the MVT extent and the collection zoom, or the
        # collector refuses every tile / the tiler misplaces every vertex.
        assert collect["writer"].batch.extent == 4096
        assert collect["writer"].batch.zoom == JA_360.target_zoom

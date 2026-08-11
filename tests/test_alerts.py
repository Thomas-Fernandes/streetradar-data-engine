"""The Discord alerter: no webhook means off, delivery failures stay contained."""

from __future__ import annotations

import json
import urllib.error
import urllib.request
from types import TracebackType
from typing import Any

import pytest

from streetradar.alerts import notify


class _Response:
    def __enter__(self) -> _Response:
        return self

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        tb: TracebackType | None,
    ) -> None:
        return None


def test_no_webhook_configured_is_off_and_touches_nothing(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.delenv("ENGINE_DISCORD_WEBHOOK", raising=False)

    def explode(*args: Any, **kwargs: Any) -> Any:
        raise AssertionError("urlopen must not be called without a webhook")

    monkeypatch.setattr(urllib.request, "urlopen", explode)
    assert notify("hello") is False


def test_a_message_is_posted_to_the_webhook(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("ENGINE_DISCORD_WEBHOOK", "https://discord.example/api/webhooks/1/t")
    seen: list[urllib.request.Request] = []

    def fake_urlopen(request: urllib.request.Request, timeout: float = 0) -> _Response:
        seen.append(request)
        return _Response()

    monkeypatch.setattr(urllib.request, "urlopen", fake_urlopen)
    assert notify("run published") is True

    request = seen[0]
    assert request.full_url == "https://discord.example/api/webhooks/1/t"
    assert request.get_method() == "POST"
    assert isinstance(request.data, bytes)
    assert json.loads(request.data) == {"content": "run published"}


def test_an_oversized_message_is_truncated_for_discord(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("ENGINE_DISCORD_WEBHOOK", "https://discord.example/api/webhooks/1/t")
    seen: list[urllib.request.Request] = []

    def fake_urlopen(request: urllib.request.Request, timeout: float = 0) -> _Response:
        seen.append(request)
        return _Response()

    monkeypatch.setattr(urllib.request, "urlopen", fake_urlopen)
    notify("x" * 5000)
    assert isinstance(seen[0].data, bytes)
    assert len(json.loads(seen[0].data)["content"]) <= 1900


def test_a_delivery_failure_returns_false_not_a_second_incident(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("ENGINE_DISCORD_WEBHOOK", "https://discord.example/api/webhooks/1/t")

    def fake_urlopen(request: urllib.request.Request, timeout: float = 0) -> _Response:
        raise urllib.error.URLError("connection refused")

    monkeypatch.setattr(urllib.request, "urlopen", fake_urlopen)
    assert notify("the alert that could not be sent") is False

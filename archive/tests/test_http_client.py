"""End-to-end behaviour of HttpClient against a real local server.

Mocking aiohttp would test the mock. These spin up an actual server so the
retry, backoff and status handling run against real responses.
"""

from __future__ import annotations

import pytest
from aiohttp import web
from aiohttp.test_utils import TestServer

from streetradar.http import (
    HttpClient,
    HttpPolicy,
    HttpStatusError,
    RetriesExhausted,
)


class Recorder:
    """A tiny server that counts hits and replays a scripted set of responses."""

    def __init__(self) -> None:
        self.hits: dict[str, int] = {}

    def _count(self, name: str) -> int:
        self.hits[name] = self.hits.get(name, 0) + 1
        return self.hits[name]

    async def ok(self, request: web.Request) -> web.Response:
        self._count("ok")
        return web.Response(body=b"payload")

    async def missing(self, request: web.Request) -> web.Response:
        self._count("missing")
        return web.Response(status=404, body=b"nope")

    async def forbidden(self, request: web.Request) -> web.Response:
        self._count("forbidden")
        return web.Response(status=403)

    async def broken(self, request: web.Request) -> web.Response:
        self._count("broken")
        return web.Response(status=503)

    async def flaky(self, request: web.Request) -> web.Response:
        """Fails twice, then succeeds."""
        if self._count("flaky") <= 2:
            return web.Response(status=503)
        return web.Response(body=b"recovered")

    async def throttled(self, request: web.Request) -> web.Response:
        """429 once with a Retry-After, then succeeds."""
        if self._count("throttled") == 1:
            return web.Response(status=429, headers={"Retry-After": "0"})
        return web.Response(body=b"allowed")


@pytest.fixture
async def server():
    recorder = Recorder()
    app = web.Application()
    app.router.add_get("/ok", recorder.ok)
    app.router.add_get("/missing", recorder.missing)
    app.router.add_get("/forbidden", recorder.forbidden)
    app.router.add_get("/broken", recorder.broken)
    app.router.add_get("/flaky", recorder.flaky)
    app.router.add_get("/throttled", recorder.throttled)

    test_server = TestServer(app)
    await test_server.start_server()
    try:
        yield test_server, recorder
    finally:
        await test_server.close()


def url_for(test_server: TestServer, path: str) -> str:
    return str(test_server.make_url(path))


FAST = HttpPolicy(max_retries=3, backoff_base=0.001, backoff_max=0.01, timeout=5)


async def test_successful_request_returns_the_body(server):
    test_server, _ = server
    async with HttpClient(FAST) as http:
        assert await http.get_bytes(url_for(test_server, "/ok")) == b"payload"


async def test_404_with_missing_ok_returns_none(server):
    test_server, _ = server
    async with HttpClient(FAST) as http:
        assert await http.get_bytes(url_for(test_server, "/missing"), missing_ok=True) is None


async def test_404_without_missing_ok_raises(server):
    test_server, _ = server
    async with HttpClient(FAST) as http:
        with pytest.raises(HttpStatusError) as exc:
            await http.get_bytes(url_for(test_server, "/missing"))
    assert exc.value.status == 404


async def test_404_is_not_retried(server):
    """A sparse tile grid answers 404 constantly. Retrying each one would burn
    the request budget and bench a perfectly good proxy."""
    test_server, recorder = server
    async with HttpClient(FAST) as http:
        await http.get_bytes(url_for(test_server, "/missing"), missing_ok=True)

    assert recorder.hits["missing"] == 1


async def test_403_is_not_retried(server):
    test_server, recorder = server
    async with HttpClient(FAST) as http:
        with pytest.raises(HttpStatusError):
            await http.get_bytes(url_for(test_server, "/forbidden"))

    assert recorder.hits["forbidden"] == 1


async def test_503_is_retried_until_the_policy_gives_up(server):
    test_server, recorder = server
    async with HttpClient(FAST) as http:
        with pytest.raises(RetriesExhausted):
            await http.get_bytes(url_for(test_server, "/broken"))

    assert recorder.hits["broken"] == FAST.max_retries + 1


async def test_transient_failure_recovers(server):
    test_server, recorder = server
    async with HttpClient(FAST) as http:
        assert await http.get_bytes(url_for(test_server, "/flaky")) == b"recovered"

    assert recorder.hits["flaky"] == 3


async def test_429_is_retried_and_then_succeeds(server):
    test_server, recorder = server
    async with HttpClient(FAST) as http:
        assert await http.get_bytes(url_for(test_server, "/throttled")) == b"allowed"

    assert recorder.hits["throttled"] == 2


async def test_client_must_be_used_as_a_context_manager():
    from streetradar.http import HttpError

    client = HttpClient(FAST)
    with pytest.raises(HttpError, match="context manager"):
        await client.get_bytes("http://example.invalid/")

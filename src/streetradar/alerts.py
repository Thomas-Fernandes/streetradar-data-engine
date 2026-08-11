"""Operational alerts to the maintainer, over a Discord webhook.

One function: post a message. The webhook URL comes from
`ENGINE_DISCORD_WEBHOOK`; when it is not set, alerting is off and `notify`
says so by returning False — a machine without the secret (CI, a test run, a
fresh clone) must never make alerting a crash of its own.

Like `streetradar.r2`, this is our own infrastructure traffic, not provider
traffic: it does not go through `streetradar.fetch` and is not gated by
`ENGINE_ALLOW_OUTBOUND` (design invariant 1 is about provider requests). It is
deliberately synchronous and dependency-free (`urllib`), because it is called
from Prefect state hooks, which run outside the flow's event loop.
"""

from __future__ import annotations

import json
import os
import urllib.request

__all__ = ["notify"]

_WEBHOOK_VARIABLE = "ENGINE_DISCORD_WEBHOOK"

#: Discord rejects message content over 2000 characters; leave headroom.
_MAX_CONTENT = 1900


def notify(message: str) -> bool:
    """Post one message to the Discord webhook. True only if it was delivered.

    A delivery failure returns False rather than raising: the alert path runs
    inside failure hooks, and an alerter that throws turns one incident into
    two. The caller can log the False; there is nobody else to tell.
    """
    url = os.environ.get(_WEBHOOK_VARIABLE)
    if not url:
        return False
    request = urllib.request.Request(  # noqa: S310 — the URL comes from our own config
        url,
        data=json.dumps({"content": message[:_MAX_CONTENT]}).encode(),
        headers={"Content-Type": "application/json", "User-Agent": "streetradar-engine"},
        method="POST",
    )
    try:
        with urllib.request.urlopen(request, timeout=10):  # noqa: S310
            return True
    except OSError:
        return False

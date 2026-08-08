"""Shared building blocks for the StreetRadar collection pipelines.

Everything in here is provider-agnostic. Code that only makes sense for one
provider belongs in `scripts/<provider>/`.

The rule that matters most: every outbound HTTP request goes through
`streetradar.http`. It is the only place that knows about proxies, backoff and
rate limits, so a provider that calls `aiohttp` or `requests` directly silently
opts out of all of them.
"""

__all__ = ["__version__"]

__version__ = "0.1.0"

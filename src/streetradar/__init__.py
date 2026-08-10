"""StreetRadar data engine.

The engine that collects street-level imagery *coverage* — where panoramas
exist, and when they were captured — for a declared set of providers, and
publishes it as dated vector tiles.

Rebuilt from scratch; the previous engine is preserved under `archive/` as a
reference and a regression oracle. See `docs/adr/0001-clean-break.md`.

The rule that matters most, and the reason the previous engine is not reused:
**every outbound HTTP request goes through `streetradar.fetch`.** It is the only
place that knows about `robots.txt`, per-host request budgets, retries and
metrics. Code that calls `aiohttp`, `requests`, or a `streetlevel`
`find_panorama_*` wrapper directly opts out of all of them silently.
"""

__all__ = ["__version__"]

__version__ = "0.2.0.dev0"

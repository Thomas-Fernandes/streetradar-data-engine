"""Declared tile sources, one per provider.

Everything here is data. Adding a provider that publishes a tile pyramid means
adding an entry, not writing a collector — which is the whole point of
`streetradar.tiles`.

Verified sources carry the evidence for their numbers. Unverified ones are
absent rather than guessed: a wrong bound silently under-collects a country.
"""

from __future__ import annotations

import os

from .tiles import TileSource

__all__ = ["JA360", "naver_coverage"]


#: Já 360, Iceland.
#:
#: Verified 2026-08-10 from the source's own TileJSON at
#: https://tiles.aws-prod.ja.is/tilesets/v15/ja360 — bounds, zoom range and the
#: `driveline` layer name are published there, along with the detail that Já
#: generate it with tippecanoe v2.72.0 from a `ja360.geojson`.
#:
#: Note the host: `tiles.aws-prod.ja.is`, not `ja.is`. It publishes no
#: robots.txt (404), whereas `ja.is/robots.txt` disallows `/kort/scene/` and
#: `/kort/closest/` — the two endpoints the previous engine used. The coverage
#: geometry is therefore both cheaper and less encumbered than per-panorama
#: collection.
#:
#: What it does NOT carry is a capture date. The TileJSON declares exactly two
#: fields, `length` and `min_zoom`, and at z14 features arrive with no attributes
#: at all. A time dimension for Já needs the per-panorama endpoint, which is a
#: separate and far more expensive decision.
JA360 = TileSource(
    id="ja",
    url_template="https://tiles.aws-prod.ja.is/tilesets/v15/ja360/tiles/{z}/{x}/{y}.pbf",
    min_zoom=4,
    max_zoom=14,
    bounds=(-24.01349, 63.399712, -13.656186, 66.552265),
)


def naver_coverage(api_key: str | None = None) -> TileSource:
    """Naver Street View coverage, South Korea.

    A factory rather than a constant because it needs a key, and the key must not
    have a default. The previous engine hardcoded
    `c9d936c2-23bf-4a91-82fc-5d03e9b6ad01` as a fallback in a tracked file — a
    credential lifted from Naver's own web client. Using a third party's API key
    is a materially different posture from fetching a public URL, so running
    without one has to be a deliberate act.

    Bounds and zoom range are **unverified**. The previous engine only ever
    swept z17 and derived its area from a hand-drawn `boundings.geojson`; whether
    the endpoint serves lower zooms is the open question worth probing, because a
    z14 mask would prune the 66% of z17 requests that came back empty.
    """
    key = api_key or os.getenv("NAVER_API_KEY")
    if not key:
        raise ValueError(
            "NAVER_API_KEY is required and has no default. See the docstring: the "
            "previous engine shipped a borrowed key, which is not a posture to inherit."
        )
    return TileSource(
        id="naver",
        url_template=(
            f"https://map.pstatic.net/nvbpc/wmts/panorama/{key}/getTile/{{x}}/{{y}}/{{z}}/pbf"
        ),
        min_zoom=14,
        max_zoom=17,
        # Approximate mainland South Korea. Replace with a measured bound before
        # any full run.
        bounds=(126.0, 33.0, 129.7, 38.7),
    )

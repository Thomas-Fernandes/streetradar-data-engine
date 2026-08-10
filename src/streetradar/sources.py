"""Declared providers, one entry each.

Everything here is data. Adding a provider that publishes a tile pyramid means
adding an entry — no collector to write.

Nothing declares bounds, zoom ranges or URL templates inline. Those come from the
provider's own metadata document at resolve time, because that is what stops a
constant going stale: Naver's tile URL embeds a dataset version that the previous
engine hardcoded, and a year later that value no longer matches what the provider
serves. What *is* declared here is the part that cannot be read from a document —
which layer we want, and how deep to go.
"""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass

from .fetch import Fetcher
from .tilejson import ResolvedSource, resolve

__all__ = ["JA_360", "NAVER_PANORAMA", "PROVIDERS", "ProviderTiles", "resolve_source"]


@dataclass(frozen=True)
class ProviderTiles:
    """A provider's coverage pyramid, and the choices we make about reading it."""

    id: str
    label: str
    metadata_url: str
    #: Layer to decode out of each tile.
    layer: str
    #: Zoom to collect geometry at. Deeper is not automatically better — see the
    #: per-provider notes, both of which are measured rather than assumed.
    target_zoom: int
    #: Zoom to begin the hierarchical descent from. Shallow enough that empty
    #: regions are pruned cheaply, deep enough that the provider's own
    #: generalisation is not dropping features from the mask.
    descent_start: int
    #: True when the tiles carry a capture date. Neither provider does, which is
    #: why a time dimension for either needs a different collection entirely.
    has_dates: bool = False


#: Já 360, Iceland.
#:
#: Note the host: `tiles.aws-prod.ja.is`, not `ja.is`. It publishes no robots.txt
#: (404), whereas `ja.is/robots.txt` disallows `/kort/scene/` and
#: `/kort/closest/` — the two endpoints the previous engine used, ~800,000 times.
#: The coverage geometry is both cheaper and less encumbered than per-panorama
#: collection.
#:
#: target_zoom is the source's own maximum. descent_start of 11 is arbitrary
#: within reason: measured over the capital region, descending from z11 cost 771
#: requests against 1,184 for a flat sweep, for identical geometry — 13,665 lines
#: and 1866.6 km either way.
JA_360 = ProviderTiles(
    id="ja",
    label="Já 360",
    metadata_url="https://tiles.aws-prod.ja.is/tilesets/v15/ja360",
    layer="driveline",
    target_zoom=14,
    descent_start=11,
)


#: Naver Street View coverage, South Korea.
#:
#: target_zoom is 15, and this one is worth stating precisely because the
#: previous engine used 17 and it cost about twelve times more for nothing.
#:
#: Naver's metadata declares a per-level `layerFilter`, and the filters for
#: levels 12-14 and 15-22 are identical strings — so the *feature set* is
#: complete from z12 up. What changes below z15 is geometric generalisation.
#: Measured over 100 z14 tiles sampled across the country, 1,518 km of linework:
#:
#:     z14 vs z15    -1.4% to -2.1% of length   (real: corners are cut)
#:     z16 vs z15    median -0.009%, worst -0.245%, aggregate -0.014%
#:     z17 vs z15    median -0.011%, worst -0.249%, aggregate -0.014%
#:
#: Zero of the 100 tiles differed by more than 1% between z15 and z17, and the
#: sign is negative — a genuinely finer geometry would be *longer*, never
#: shorter, so the residual is clipping noise. z15 is the first complete tier and
#: z16/z17 add nothing but coordinate precision (0.24 m per unit at z15, already
#: far below a capture vehicle's own GPS error).
#:
#: descent_start of 6 is the source's minimum. Measured over the whole country,
#: the descent prunes hard early — 44% of z6 tiles have content, rising to 92% by
#: z12 — and cost 38,937 requests to reach z14 against 278,765 for a flat sweep.
NAVER_PANORAMA = ProviderTiles(
    id="naver",
    label="Naver Street View",
    metadata_url="https://map.pstatic.net/nvbpc/wmts/panorama/getMetadata",
    layer="street_link_l",
    target_zoom=15,
    descent_start=6,
)


PROVIDERS: Mapping[str, ProviderTiles] = {
    provider.id: provider for provider in (JA_360, NAVER_PANORAMA)
}


async def resolve_source(fetcher: Fetcher, provider: ProviderTiles) -> ResolvedSource:
    """Read a provider's metadata and build its tile source.

    One request, and it is what keeps the bounds, the zoom range and any embedded
    version identifier current.
    """
    resolved = await resolve(fetcher, provider.metadata_url, id=provider.id, layer=provider.layer)
    if resolved.source.max_zoom < provider.target_zoom:
        raise ValueError(
            f"{provider.id}: target zoom {provider.target_zoom} is deeper than the "
            f"{resolved.source.max_zoom} the provider now publishes"
        )
    return resolved

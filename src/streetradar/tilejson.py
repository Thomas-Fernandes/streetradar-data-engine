"""Resolve a tile source from the provider's own metadata document.

Both providers publish TileJSON, which turns out to be the single most valuable
thing to read before collecting anything:

* **Já**, at `https://tiles.aws-prod.ja.is/tilesets/v15/ja360` — bounds, zoom
  range, and the `driveline` layer name.
* **Naver**, at `https://map.pstatic.net/nvbpc/wmts/panorama/getMetadata` —
  the same, plus a `versionCode` that the tile URL embeds and a per-level
  `layerFilter` declaring where the generalisation tiers are.

Reading it is not a nicety. The previous engine hardcoded Naver's `versionCode`
as `c9d936c2-…` and called it an API key; the live value is now
`c315d97e-…`, so that constant had gone stale and the collector was pinned to a
dataset a year old. It was never a credential — it is a dataset version that the
provider publishes, unauthenticated, to anyone loading their map.

The document also carries the field list, which is how we know, from the
provider's own declaration rather than by sampling, that neither source has a
capture date.
"""

from __future__ import annotations

import json
from collections.abc import Mapping
from dataclasses import dataclass
from typing import Any

from .fetch import Fetcher
from .tiles import Bounds, TileSource

__all__ = [
    "ResolvedSource",
    "TileJSONError",
    "parse_tilejson",
    "resolve",
]


class TileJSONError(ValueError):
    """The metadata document was missing something we depend on."""


@dataclass(frozen=True)
class ResolvedSource:
    """A tile source plus the provenance of the document it came from."""

    source: TileSource
    layer: str
    #: Attribute names the provider declares, mapped to their types. Empty for a
    #: document that declares no vector layers.
    fields: Mapping[str, str]
    #: Provider-specific version identifiers, when published. Naver has both;
    #: Já has neither.
    dataset: str | None = None
    version: str | None = None
    attribution: str | None = None

    @property
    def has_temporal_field(self) -> bool:
        """Whether any declared field plausibly carries a capture date.

        A heuristic, and deliberately a loud one: if this ever turns True for a
        source we treat as undated, the time-dimension conclusion for that
        provider needs revisiting.
        """
        needles = ("date", "time", "year", "month", "captured", "photo", "ts")
        return any(needle in name.lower() for name in self.fields for needle in needles)


def parse_tilejson(
    document: Mapping[str, Any],
    *,
    id: str,
    layer: str | None = None,
) -> ResolvedSource:
    """Turn a TileJSON document into a `TileSource`.

    `layer` may be omitted when the document declares exactly one vector layer,
    which is the case for both providers today.
    """
    templates = document.get("tiles")
    if not isinstance(templates, list) or not templates:
        raise TileJSONError(f"{id}: document declares no tile templates")
    template = str(templates[0])

    # Já's document advertises http://. Silently downgrading a request to
    # cleartext because a JSON field said so is not a thing to do.
    if template.startswith("http://"):
        template = "https://" + template[len("http://") :]

    for placeholder in ("{z}", "{x}", "{y}"):
        if placeholder not in template:
            raise TileJSONError(f"{id}: tile template lacks {placeholder}: {template}")

    bounds = _bounds(document, id)
    min_zoom, max_zoom = _zooms(document, id)
    layers = document.get("vector_layers")
    fields: Mapping[str, str] = {}
    resolved_layer = layer

    if isinstance(layers, list) and layers:
        names = [str(entry.get("id")) for entry in layers if isinstance(entry, dict)]
        if resolved_layer is None:
            if len(names) != 1:
                raise TileJSONError(
                    f"{id}: document declares {len(names)} layers {names}; name one explicitly"
                )
            resolved_layer = names[0]
        elif resolved_layer not in names:
            raise TileJSONError(f"{id}: layer {resolved_layer!r} not among {names}")
        for entry in layers:
            if isinstance(entry, dict) and str(entry.get("id")) == resolved_layer:
                declared = entry.get("fields")
                if isinstance(declared, dict):
                    fields = {str(k): str(v) for k, v in declared.items()}
                break

    if resolved_layer is None:
        raise TileJSONError(f"{id}: no layer named and none declared in the document")

    return ResolvedSource(
        source=TileSource(
            id=id,
            url_template=template,
            min_zoom=min_zoom,
            max_zoom=max_zoom,
            bounds=bounds,
        ),
        layer=resolved_layer,
        fields=fields,
        dataset=_optional_str(document.get("dataset")),
        # `versionCode` only, deliberately. TileJSON's own `version` field means
        # the version of the tileset *format* — Já publishes "2" — and reporting
        # that as a dataset version would be misleading. Já simply has no dataset
        # version, and None says so.
        version=_optional_str(document.get("versionCode")),
        attribution=_optional_str(document.get("attribution")),
    )


async def resolve(
    fetcher: Fetcher,
    metadata_url: str,
    *,
    id: str,
    layer: str | None = None,
) -> ResolvedSource:
    """Fetch a metadata document and parse it.

    One request, before any collection, and it is what keeps the version code,
    the bounds and the zoom range current rather than frozen in a constant.
    """
    body = await fetcher.get(metadata_url)
    if not body:
        raise TileJSONError(f"{id}: metadata document at {metadata_url} was empty")
    try:
        document = json.loads(body)
    except json.JSONDecodeError as exc:
        raise TileJSONError(f"{id}: metadata document is not JSON: {exc}") from exc
    if not isinstance(document, dict):
        raise TileJSONError(f"{id}: metadata document is not an object")
    return parse_tilejson(document, id=id, layer=layer)


def _bounds(document: Mapping[str, Any], id: str) -> Bounds:
    raw = document.get("bounds")
    if not isinstance(raw, list) or len(raw) != 4:
        raise TileJSONError(f"{id}: bounds must be a list of four numbers, got {raw!r}")
    try:
        west, south, east, north = (float(value) for value in raw)
    except (TypeError, ValueError) as exc:
        raise TileJSONError(f"{id}: bounds are not numeric: {raw!r}") from exc
    return (west, south, east, north)


def _zooms(document: Mapping[str, Any], id: str) -> tuple[int, int]:
    try:
        min_zoom = int(document["minzoom"])
        max_zoom = int(document["maxzoom"])
    except (KeyError, TypeError, ValueError) as exc:
        raise TileJSONError(f"{id}: document lacks a usable minzoom/maxzoom: {exc}") from exc
    return min_zoom, max_zoom


def _optional_str(value: Any) -> str | None:
    return str(value) if isinstance(value, str | int | float) else None

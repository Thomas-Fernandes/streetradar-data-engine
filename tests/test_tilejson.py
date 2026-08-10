"""Resolving a tile source from the provider's own metadata document.

Both fixtures are the real thing, captured 2026-08-10. Parsing them is what pins
the numbers the collection plan rests on — bounds, zoom ranges, layer names, and
the declared field lists that say neither provider carries a capture date.
"""

from __future__ import annotations

import json
import pathlib
from typing import Any

import pytest

from streetradar.sources import JA_360, NAVER_PANORAMA, PROVIDERS
from streetradar.tilejson import TileJSONError, parse_tilejson
from streetradar.tiles import Tile

FIXTURES = pathlib.Path(__file__).parent / "fixtures"


def document(name: str) -> dict[str, Any]:
    loaded: dict[str, Any] = json.loads((FIXTURES / name).read_text())
    return loaded


def ja_doc() -> dict[str, Any]:
    return document("ja360-tilejson.json")


def naver_doc() -> dict[str, Any]:
    return document("naver-getmetadata.json")


class TestJa:
    def test_resolves_bounds_zoom_and_layer(self) -> None:
        resolved = parse_tilejson(ja_doc(), id="ja", layer="driveline")
        assert resolved.layer == "driveline"
        assert resolved.source.min_zoom == 4
        assert resolved.source.max_zoom == 14
        assert resolved.source.bounds == pytest.approx(
            (-24.01349, 63.399712, -13.656186, 66.552265)
        )

    def test_http_template_is_upgraded_to_https(self) -> None:
        # Já's document advertises http://. Silently downgrading to cleartext
        # because a JSON field said so is not a thing to do.
        assert ja_doc()["tiles"][0].startswith("http://")
        resolved = parse_tilejson(ja_doc(), id="ja")
        assert resolved.source.url_template.startswith("https://")

    def test_z14_sweep_matches_the_number_the_plan_rests_on(self) -> None:
        # 160,952 is the flat-sweep cost that the "5x fewer requests than the old
        # engine" claim is measured against.
        resolved = parse_tilejson(ja_doc(), id="ja")
        assert resolved.source.tile_count_at(14) == 160_952

    def test_targets_the_tile_host_not_the_website(self) -> None:
        # ja.is/robots.txt disallows the per-panorama endpoints; the tile host
        # publishes no robots.txt at all. Confusing the two would matter.
        resolved = parse_tilejson(ja_doc(), id="ja")
        assert "tiles.aws-prod.ja.is" in resolved.source.url(Tile(14, 7195, 4355))
        assert "//ja.is" not in resolved.source.url_template

    def test_single_layer_is_inferred_without_being_named(self) -> None:
        assert parse_tilejson(ja_doc(), id="ja").layer == "driveline"


class TestNaver:
    def test_resolves_the_published_zoom_range(self) -> None:
        # The previous engine only ever collected z17 and never knew z6-z22
        # existed, which is why it never considered a hierarchical descent.
        resolved = parse_tilejson(naver_doc(), id="naver")
        assert resolved.source.min_zoom == 6
        assert resolved.source.max_zoom == 22

    def test_version_code_comes_from_the_document_not_a_constant(self) -> None:
        # The heart of it: the previous engine hardcoded c9d936c2-… and called it
        # an API key. It is a dataset version, it has since changed, and reading
        # it is the only way to stay current.
        resolved = parse_tilejson(naver_doc(), id="naver")
        assert resolved.version
        assert "c9d936c2" not in resolved.source.url_template
        assert resolved.version in resolved.source.url_template
        assert resolved.dataset and resolved.dataset.startswith("R_")

    def test_tile_template_takes_x_y_z_in_naver_order(self) -> None:
        resolved = parse_tilejson(naver_doc(), id="naver")
        url = resolved.source.url(Tile(15, 27941, 12689))
        assert url.endswith("/getTile/27941/12689/15/pbf")

    def test_declares_the_street_link_layer(self) -> None:
        resolved = parse_tilejson(naver_doc(), id="naver")
        assert resolved.layer == "street_link_l"
        assert set(resolved.fields) >= {"dp_link_k", "dp_level", "dp_road_k", "road_k"}


class TestTemporalFields:
    @pytest.mark.parametrize("name", ["ja360-tilejson.json", "naver-getmetadata.json"])
    def test_neither_provider_declares_a_capture_date(self, name: str) -> None:
        # Load-bearing negative result, taken from the providers' own field
        # declarations rather than by sampling tiles. If this ever fails, the
        # time-dimension conclusion for that provider is worth revisiting — which
        # is exactly why it is asserted rather than assumed.
        resolved = parse_tilejson(document(name), id="probe")
        assert not resolved.has_temporal_field

    def test_the_heuristic_would_notice_a_date_field(self) -> None:
        doc = naver_doc()
        doc["vector_layers"][0]["fields"]["photodate"] = "String"
        assert parse_tilejson(doc, id="probe").has_temporal_field


class TestValidation:
    def test_missing_tile_template_is_refused(self) -> None:
        doc = ja_doc()
        del doc["tiles"]
        with pytest.raises(TileJSONError, match="no tile templates"):
            parse_tilejson(doc, id="ja")

    def test_template_without_all_placeholders_is_refused(self) -> None:
        doc = ja_doc()
        doc["tiles"] = ["https://example.invalid/{z}/{x}.pbf"]
        with pytest.raises(TileJSONError, match=r"lacks \{y\}"):
            parse_tilejson(doc, id="ja")

    def test_malformed_bounds_are_refused(self) -> None:
        doc = ja_doc()
        doc["bounds"] = [1.0, 2.0]
        with pytest.raises(TileJSONError, match="four numbers"):
            parse_tilejson(doc, id="ja")

    def test_missing_zooms_are_refused(self) -> None:
        doc = ja_doc()
        del doc["maxzoom"]
        with pytest.raises(TileJSONError, match="minzoom/maxzoom"):
            parse_tilejson(doc, id="ja")

    def test_unknown_layer_is_refused(self) -> None:
        with pytest.raises(TileJSONError, match="not among"):
            parse_tilejson(ja_doc(), id="ja", layer="nope")


class TestRegistry:
    def test_both_providers_are_registered(self) -> None:
        assert set(PROVIDERS) == {"ja", "naver"}

    def test_no_provider_declares_bounds_or_a_url_inline(self) -> None:
        # The point of the rewrite: nothing is frozen in a constant. Bounds, zoom
        # range and any embedded version come from the metadata document.
        for provider in PROVIDERS.values():
            assert not hasattr(provider, "bounds")
            assert provider.metadata_url.startswith("https://")

    def test_target_zooms_are_the_measured_ones(self) -> None:
        # Já: the source's own maximum. Naver: measured over 100 tiles, z15 is
        # the first complete tier and z16/z17 add nothing (aggregate -0.014%).
        assert JA_360.target_zoom == 14
        assert NAVER_PANORAMA.target_zoom == 15

    def test_target_zoom_is_within_the_published_range(self) -> None:
        for provider, name in (
            (JA_360, "ja360-tilejson.json"),
            (NAVER_PANORAMA, "naver-getmetadata.json"),
        ):
            resolved = parse_tilejson(document(name), id=provider.id, layer=provider.layer)
            assert resolved.source.min_zoom <= provider.descent_start
            assert provider.descent_start <= provider.target_zoom <= resolved.source.max_zoom

    def test_neither_provider_claims_to_carry_dates(self) -> None:
        assert not any(provider.has_dates for provider in PROVIDERS.values())

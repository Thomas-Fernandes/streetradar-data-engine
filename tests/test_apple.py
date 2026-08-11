"""Decoding Apple's coverage tiles.

The anchor is `apple-coverage-z17-114000-51000.oracle.json`: the same bytes run
through `streetlevel.lookaround.parse_coverage_tile`, whose reverse engineering
of this format has been validated against real Apple tiles for years. Our decoder
must agree with it exactly, to the ninth decimal — that is what makes vendoring
the schema instead of the library safe.

`streetlevel` produces the oracle but is never a dependency, the way `archive/`
is the oracle for `osm_match` without being importable from `src/`.

**These fixtures are synthetic**, encoded with the reference schema. They prove
the decoder matches the reference implementation's semantics. They do NOT prove
the schema still matches what Apple serves today — that needs a recorded tile,
which needs an outbound request, which happens on the VPS. Invariant 4 is
therefore only half satisfied here, deliberately and visibly.
"""

from __future__ import annotations

import json
import pathlib

import pytest

from streetradar.apple import (
    APPLE_ZOOM,
    COVERAGE_TYPES,
    CoverageTile,
    TokenMissing,
    apple_source,
    decode_coverage_tile,
)
from streetradar.proto import apple_coverage_pb2 as pb
from streetradar.tiles import Tile

FIXTURES = pathlib.Path(__file__).parent / "fixtures"
TILE = Tile(17, 114000, 51000)
BODY = (FIXTURES / "apple-coverage-z17-114000-51000.pb").read_bytes()
ORACLE = json.loads((FIXTURES / "apple-coverage-z17-114000-51000.oracle.json").read_text())
DEFAULT_TYPE_BODY = (FIXTURES / "apple-coverage-default-type.pb").read_bytes()

BOUNDS = (-10.0, 35.0, 5.0, 50.0)
TOKEN = "test-token"


def decoded() -> CoverageTile:
    return decode_coverage_tile(BODY, TILE)


class TestAgainstTheOracle:
    def test_every_panorama_is_found(self) -> None:
        assert len(decoded()) == len(ORACLE) == 291

    def test_positions_match_to_the_ninth_decimal(self) -> None:
        # The whole vendoring decision rests on this: same schema, same maths,
        # same answer. A millimetre of drift here means the projection is wrong.
        tile = decoded()
        assert [round(v, 9) for v in tile.lat] == [row[1] for row in ORACLE]
        assert [round(v, 9) for v in tile.lon] == [row[2] for row in ORACLE]

    def test_identifiers_timestamps_and_builds_match(self) -> None:
        tile = decoded()
        assert list(tile.pano_id) == [row[0] for row in ORACLE]
        assert list(tile.captured_at) == [row[3] for row in ORACLE]
        assert list(tile.build_id) == [row[4] for row in ORACLE]

    def test_coverage_types_match(self) -> None:
        assert list(decoded().coverage_type) == [COVERAGE_TYPES[row[5]] for row in ORACLE]


class TestGeometry:
    def test_every_panorama_lands_inside_its_tile(self) -> None:
        west, south, east, north = TILE.bounds
        tile = decoded()
        assert all(west <= lon <= east for lon in tile.lon)
        assert all(south <= lat <= north for lat in tile.lat)

    def test_the_extreme_offsets_land_on_the_tile_s_own_corners(self) -> None:
        # Pins the two constants the projection turns on: positions are
        # sixty-fourths of a pixel, and a tile is 255 of them across — not 256.
        # Off by one here and every panorama drifts by a quarter of a tile.
        message = pb.GroundMetadataTile()
        message.tile_coordinate.x, message.tile_coordinate.y = TILE.x, TILE.y
        message.tile_coordinate.z = APPLE_ZOOM
        for x, y in ((0, 0), (255 * 64, 255 * 64)):
            pano = message.pano.add()
            pano.panoid, pano.timestamp = 1, 1554163200000
            pano.tile_position.x, pano.tile_position.y = x, y
        tile = decode_coverage_tile(message.SerializeToString(), TILE)
        west, south, east, north = TILE.bounds
        assert (tile.lon[0], tile.lat[0]) == pytest.approx((west, south))
        assert (tile.lon[1], tile.lat[1]) == pytest.approx((east, north))

    def test_the_columns_are_all_the_same_length(self) -> None:
        tile = decoded()
        widths = {
            len(tile.pano_id),
            len(tile.lon),
            len(tile.lat),
            len(tile.captured_at),
            len(tile.coverage_type),
            len(tile.build_id),
        }
        assert len(widths) == 1


class TestEmptyAndBroken:
    @pytest.mark.parametrize("body", [None, b""])
    def test_no_body_is_no_coverage_rather_than_an_error(self, body: bytes | None) -> None:
        # Over a country sweep "nothing here" is the common answer.
        assert len(decode_coverage_tile(body, TILE)) == 0

    def test_a_message_without_panoramas_is_empty(self) -> None:
        assert len(decode_coverage_tile(pb.GroundMetadataTile().SerializeToString(), TILE)) == 0

    def test_a_tile_answering_for_somewhere_else_is_refused(self) -> None:
        # Trusting the request over the message would place every panorama in
        # this tile somewhere else entirely, which no later stage could detect.
        with pytest.raises(ValueError, match="answered with coordinates"):
            decode_coverage_tile(BODY, Tile(17, 999, 999))

    def test_a_build_index_past_the_table_keeps_the_position(self) -> None:
        # The tile disagrees with itself, but where the panorama is remains good.
        message = pb.GroundMetadataTile()
        message.ParseFromString(BODY)
        message.pano[0].build_table_idx = 99
        tile = decode_coverage_tile(message.SerializeToString(), TILE)
        assert len(tile) == 291
        assert tile.build_id[0] is None
        assert tile.coverage_type[0] is None


class TestCoverageTypes:
    def test_the_default_type_decodes_rather_than_raising(self) -> None:
        # streetlevel raises `ValueError: 0 is not a valid CoverageType` on this
        # exact fixture, although its own schema declares DEFAULT = 0.
        tile = decode_coverage_tile(DEFAULT_TYPE_BODY, Tile(17, 114001, 51001))
        assert len(tile) == 4
        assert set(tile.coverage_type) == {"default"}

    def test_an_unknown_type_becomes_unknown_rather_than_wrong(self) -> None:
        message = pb.GroundMetadataTile()
        message.ParseFromString(BODY)
        # 7 is deliberately outside the schema's enum, which is the case under
        # test: proto3 enums are open, so Apple can add a value tomorrow and the
        # generated stub would not know about it. Set through the descriptor
        # because that is the only API that accepts an undeclared value — the
        # generated `CoverageType` is not callable despite what the stub says.
        build = message.build_table[0]
        setattr(build, build.DESCRIPTOR.fields_by_name["coverage_type"].name, 7)
        tile = decode_coverage_tile(message.SerializeToString(), TILE)
        assert None in tile.coverage_type

    def test_the_declared_types_are_the_schema_s_own(self) -> None:
        assert COVERAGE_TYPES == {0: "default", 2: "car", 3: "backpack"}


class TestSource:
    def test_the_coordinates_travel_in_headers(self) -> None:
        # Apple's endpoint URL is constant; z/x/y go in maps-tile-*.
        source = apple_source(BOUNDS, token=TOKEN)
        headers = source.headers(TILE)
        assert (headers["maps-tile-x"], headers["maps-tile-y"], headers["maps-tile-z"]) == (
            "114000",
            "51000",
            "17",
        )
        assert source.url(TILE) == source.url(Tile(17, 1, 1))

    def test_the_token_is_sent(self) -> None:
        assert apple_source(BOUNDS, token=TOKEN).headers(TILE)["maps-auth-token"] == TOKEN

    def test_the_token_can_come_from_the_environment(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.setenv("ENGINE_APPLE_TOKEN", "from-env")
        assert apple_source(BOUNDS).headers(TILE)["maps-auth-token"] == "from-env"

    def test_a_missing_token_is_refused_by_name(self, monkeypatch: pytest.MonkeyPatch) -> None:
        # Every route returns 403 without it, so failing here beats failing on
        # the first of several hundred thousand requests.
        monkeypatch.delenv("ENGINE_APPLE_TOKEN", raising=False)
        with pytest.raises(TokenMissing, match="ENGINE_APPLE_TOKEN"):
            apple_source(BOUNDS)

    def test_only_one_zoom_is_served(self) -> None:
        # Apple rejects every zoom but 17, which is why the pyramid descent that
        # works for Já and Naver cannot work here.
        source = apple_source(BOUNDS, token=TOKEN)
        assert source.min_zoom == source.max_zoom == APPLE_ZOOM
        with pytest.raises(ValueError, match="outside published range"):
            source.tile_count_at(16)

    def test_the_token_is_not_committed(self) -> None:
        # A public showcase does not republish someone else's credential.
        source = pathlib.Path(__file__).parent.parent / "src" / "streetradar" / "apple.py"
        assert "maps-auth-token" in source.read_text()
        assert "w31CPGRO" not in source.read_text()

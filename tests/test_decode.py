"""MVT decoding: axis direction, Mercator, and tile-edge clipping.

The integration tests run against a real Já tile committed as a fixture
(`tests/fixtures/ja360-z14-7195-4355.pbf`, 5.7 KB of Reykjavík linework). Its
numbers were cross-checked two independent ways when the decoder was written —
planar metres-per-MVT-unit scaling gave 29.70 km, inverse Mercator plus haversine
gave 29.67 km — so they are worth pinning.
"""

from __future__ import annotations

import math
import pathlib

import pytest

from streetradar.decode import (
    _clip_to_tile,
    _to_wgs84,
    decode_tile,
    geodesic_length_m,
)
from streetradar.tiles import Tile

FIXTURE = pathlib.Path(__file__).parent / "fixtures" / "ja360-z14-7195-4355.pbf"
REYKJAVIK_TILE = Tile(14, 7195, 4355)
EXTENT = 4096


def fixture_body() -> bytes:
    return FIXTURE.read_bytes()


class TestAbsentAndEmpty:
    def test_none_body_decodes_to_nothing(self) -> None:
        # Já signals "no coverage here" with HTTP 204 and Naver with a
        # zero-length 200; both must arrive here as an ordinary empty answer.
        assert decode_tile(None, REYKJAVIK_TILE, layer="driveline").lines == ()

    def test_empty_body_decodes_to_nothing(self) -> None:
        assert decode_tile(b"", REYKJAVIK_TILE, layer="driveline").lines == ()

    def test_unknown_layer_decodes_to_nothing(self) -> None:
        result = decode_tile(fixture_body(), REYKJAVIK_TILE, layer="not-a-layer")
        assert result.lines == ()


class TestAxisAndProjection:
    def test_decoded_y_points_north(self) -> None:
        # mapbox_vector_tile defaults to y_coord_down=False and applies
        # y = extent - y. Getting this backwards mirrors every road about the
        # tile's horizontal centre line, which looks plausible and is wrong.
        _, south, _, north = REYKJAVIK_TILE.bounds
        _, lat_at_y0 = _to_wgs84(0.0, 0.0, REYKJAVIK_TILE, EXTENT)
        _, lat_at_top = _to_wgs84(0.0, float(EXTENT), REYKJAVIK_TILE, EXTENT)
        assert lat_at_y0 == pytest.approx(south, abs=1e-9)
        assert lat_at_top == pytest.approx(north, abs=1e-9)
        assert lat_at_top > lat_at_y0

    def test_tile_corners_map_to_tile_bounds(self) -> None:
        west, south, east, north = REYKJAVIK_TILE.bounds
        assert _to_wgs84(0.0, 0.0, REYKJAVIK_TILE, EXTENT) == pytest.approx((west, south))
        assert _to_wgs84(float(EXTENT), float(EXTENT), REYKJAVIK_TILE, EXTENT) == pytest.approx(
            (east, north)
        )

    def test_latitude_is_not_linear_within_the_tile(self) -> None:
        # A tile spans equal Web Mercator, not equal degrees. Linear
        # interpolation is the tempting shortcut and it is measurably wrong.
        _, south, _, north = REYKJAVIK_TILE.bounds
        _, lat_mid = _to_wgs84(0.0, EXTENT / 2.0, REYKJAVIK_TILE, EXTENT)
        linear_mid = (south + north) / 2.0
        assert lat_mid != pytest.approx(linear_mid, abs=1e-9)
        # At 64°N the shortcut's error is small but far from negligible: it is
        # the difference between the right and the wrong side of a building.
        error_m = abs(lat_mid - linear_mid) * 111_320.0
        assert 0.01 < error_m < 5.0

    def test_longitude_is_linear_within_the_tile(self) -> None:
        west, _, east, _ = REYKJAVIK_TILE.bounds
        lon_mid, _ = _to_wgs84(EXTENT / 2.0, 0.0, REYKJAVIK_TILE, EXTENT)
        assert lon_mid == pytest.approx((west + east) / 2.0)


class TestClipping:
    def test_geometry_fully_inside_is_untouched(self) -> None:
        ring = [(100.0, 100.0), (200.0, 200.0), (300.0, 150.0)]
        assert _clip_to_tile(ring, EXTENT) == [ring]

    def test_geometry_fully_outside_is_dropped(self) -> None:
        ring = [(-500.0, -500.0), (-400.0, -400.0)]
        assert _clip_to_tile(ring, EXTENT) == []

    def test_geometry_crossing_an_edge_is_cut_at_the_edge(self) -> None:
        runs = _clip_to_tile([(-100.0, 50.0), (100.0, 50.0)], EXTENT)
        assert len(runs) == 1
        assert runs[0][0] == pytest.approx((0.0, 50.0))
        assert runs[0][-1] == pytest.approx((100.0, 50.0))

    def test_line_that_leaves_and_re_enters_elsewhere_becomes_two_runs(self) -> None:
        # This is why clipping returns a list rather than one polyline: a road
        # that exits the tile and comes back at a different point is two separate
        # pieces of this tile's linework, and joining them would invent a
        # shortcut across the gap.
        ring = [(100.0, 50.0), (-50.0, 50.0), (-50.0, 200.0), (100.0, 200.0)]
        runs = _clip_to_tile(ring, EXTENT)
        assert len(runs) == 2
        assert runs[0][-1] == pytest.approx((0.0, 50.0))
        assert runs[1][0] == pytest.approx((0.0, 200.0))

    def test_line_that_doubles_back_through_the_same_point_stays_one_run(self) -> None:
        # Degenerate but real: exiting and re-entering at the identical point is
        # a continuous path inside the tile, so chaining is correct and the
        # length is preserved. Pinned because the opposite is a tempting reading.
        ring = [(100.0, 50.0), (-50.0, 50.0), (200.0, 50.0)]
        runs = _clip_to_tile(ring, EXTENT)
        assert len(runs) == 1
        assert runs[0] == [(100.0, 50.0), (0.0, 50.0), (200.0, 50.0)]

    def test_buffer_geometry_is_what_clipping_removes(self) -> None:
        # Tippecanoe emits a buffer past each edge, so the same sliver of road
        # appears in two neighbouring tiles. Summing unclipped tiles across a
        # country double-counts every boundary.
        body = fixture_body()
        loose = decode_tile(body, REYKJAVIK_TILE, layer="driveline", clip=False)
        tight = decode_tile(body, REYKJAVIK_TILE, layer="driveline", clip=True)
        assert tight.length_m < loose.length_m
        overcount = (loose.length_m - tight.length_m) / tight.length_m
        assert 0.05 < overcount < 0.10  # measured at 7.6% on this tile


class TestRealTile:
    def test_clipped_geometry_is_entirely_within_the_tile(self) -> None:
        result = decode_tile(fixture_body(), REYKJAVIK_TILE, layer="driveline")
        west, south, east, north = REYKJAVIK_TILE.bounds
        for line in result.lines:
            for lon, lat in line.coords:
                assert west <= lon <= east
                assert south <= lat <= north

    def test_unclipped_length_matches_the_independent_estimate(self) -> None:
        # 29.70 km was computed from metres-per-MVT-unit before this module
        # existed. Agreement to 0.1% is what validates the projection and the
        # length function together.
        result = decode_tile(fixture_body(), REYKJAVIK_TILE, layer="driveline", clip=False)
        assert result.length_m / 1000.0 == pytest.approx(29.70, abs=0.1)

    def test_clipped_totals_are_pinned(self) -> None:
        result = decode_tile(fixture_body(), REYKJAVIK_TILE, layer="driveline")
        assert result.extent == EXTENT
        assert len(result.lines) == 236
        assert result.vertices == 1149
        assert result.length_m / 1000.0 == pytest.approx(27.42, abs=0.05)

    def test_the_tile_lands_in_reykjavik(self) -> None:
        result = decode_tile(fixture_body(), REYKJAVIK_TILE, layer="driveline")
        lons = [lon for line in result.lines for lon, _ in line.coords]
        lats = [lat for line in result.lines for _, lat in line.coords]
        assert sum(lats) / len(lats) == pytest.approx(64.13, abs=0.02)
        assert sum(lons) / len(lons) == pytest.approx(-21.90, abs=0.02)

    def test_ja_tiles_carry_no_attributes_at_max_zoom(self) -> None:
        # Load-bearing negative result: the coverage tiles have no capture date,
        # so a time dimension for Já needs the per-panorama endpoint instead.
        # If this ever fails, that conclusion is worth revisiting.
        result = decode_tile(fixture_body(), REYKJAVIK_TILE, layer="driveline")
        assert all(line.properties == {} for line in result.lines)


class TestGeodesicLength:
    def test_a_degree_of_latitude_is_about_111_km(self) -> None:
        assert geodesic_length_m([(0.0, 0.0), (0.0, 1.0)]) == pytest.approx(111_195.0, rel=0.001)

    def test_longitude_shrinks_with_latitude(self) -> None:
        at_equator = geodesic_length_m([(0.0, 0.0), (1.0, 0.0)])
        at_iceland = geodesic_length_m([(0.0, 64.0), (1.0, 64.0)])
        assert at_iceland == pytest.approx(at_equator * math.cos(math.radians(64.0)), rel=0.01)

    def test_a_single_point_has_no_length(self) -> None:
        assert geodesic_length_m([(0.0, 0.0)]) == 0.0

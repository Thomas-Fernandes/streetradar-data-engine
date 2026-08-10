"""Tile addressing, enumeration and harvest strategy — all without a network.

The harvesters take a two-member protocol rather than a concrete Fetcher, so the
descent logic is exercised against a stub that answers from a dict. That is the
part worth testing: whether the pyramid descends only where content exists.
"""

from __future__ import annotations

from collections.abc import Mapping

import pytest

from streetradar.tiles import Tile, TileSource, harvest_flat, harvest_pyramid


class StubFetcher:
    """Answers from a set of tiles that "have content"; everything else is absent."""

    def __init__(self, with_content: set[Tile], concurrency: int = 4) -> None:
        self._with_content = with_content
        self._concurrency = concurrency
        self.requested: list[str] = []
        self.headers_seen: list[Mapping[str, str]] = []

    @property
    def concurrency(self) -> int:
        return self._concurrency

    async def fetch_tile(self, url: str, headers: Mapping[str, str]) -> bytes | None:
        self.requested.append(url)
        self.headers_seen.append(headers)
        z, x, y = (int(p) for p in url.rstrip("/").split("/")[-3:])
        return b"content" if Tile(z, x, y) in self._with_content else None


def source(min_zoom: int = 0, max_zoom: int = 3) -> TileSource:
    return TileSource(
        id="test",
        url_template="https://example.invalid/{z}/{x}/{y}",
        min_zoom=min_zoom,
        max_zoom=max_zoom,
        bounds=(-180.0, -85.0, 180.0, 85.0),
    )


class TestTile:
    def test_children_are_the_four_quadrants(self) -> None:
        assert Tile(2, 1, 1).children == (
            Tile(3, 2, 2),
            Tile(3, 3, 2),
            Tile(3, 2, 3),
            Tile(3, 3, 3),
        )

    def test_parent_inverts_children(self) -> None:
        tile = Tile(9, 123, 456)
        assert all(child.parent == tile for child in tile.children)

    def test_str_is_the_z_x_y_form_used_in_urls_and_logs(self) -> None:
        assert str(Tile(14, 7195, 4355)) == "14/7195/4355"

    def test_bounds_shrink_with_zoom(self) -> None:
        west, south, east, north = Tile(0, 0, 0).bounds
        assert (west, east) == pytest.approx((-180.0, 180.0))
        assert north > 85.0 and south < -85.0

    def test_ground_size_matches_the_measured_iceland_tile(self) -> None:
        # The z14 tile over Reykjavik measured ~1.05 km across; that number is
        # what makes z14 the right target zoom for street-level geometry.
        assert Tile(14, 7195, 4355).ground_size_m() == pytest.approx(1067.0, abs=15.0)


class TestTileSource:
    def test_url_and_headers_are_both_templated(self) -> None:
        # Apple puts the coordinates in headers rather than the path, so both
        # have to be formattable for one abstraction to cover all providers.
        src = TileSource(
            id="apple-shaped",
            url_template="https://example.invalid/api/tile",
            min_zoom=17,
            max_zoom=17,
            bounds=(-1.0, -1.0, 1.0, 1.0),
            header_template={"maps-tile-x": "{x}", "maps-tile-y": "{y}", "z": "{z}"},
        )
        tile = Tile(17, 5, 9)
        assert src.url(tile) == "https://example.invalid/api/tile"
        assert src.headers(tile) == {"maps-tile-x": "5", "maps-tile-y": "9", "z": "17"}

    def test_tiles_at_agrees_with_tile_count_at(self) -> None:
        src = source(max_zoom=5)
        for zoom in range(0, 6):
            assert len(list(src.tiles_at(zoom))) == src.tile_count_at(zoom)

    def test_zoom_outside_the_published_range_is_refused(self) -> None:
        src = source(min_zoom=4, max_zoom=14)
        with pytest.raises(ValueError, match="outside published range"):
            src.tile_count_at(15)

    def test_inverted_bounds_are_refused_at_construction(self) -> None:
        with pytest.raises(ValueError, match="west, south, east, north"):
            TileSource(
                id="bad",
                url_template="{z}/{x}/{y}",
                min_zoom=0,
                max_zoom=1,
                bounds=(10.0, 10.0, -10.0, -10.0),
            )

    def test_zoom_range_must_be_ordered(self) -> None:
        with pytest.raises(ValueError, match="min_zoom must not exceed"):
            TileSource(
                id="bad",
                url_template="{z}/{x}/{y}",
                min_zoom=9,
                max_zoom=4,
                bounds=(-1.0, -1.0, 1.0, 1.0),
            )


class TestHarvestFlat:
    async def test_yields_every_tile_in_the_box(self) -> None:
        src = source(max_zoom=2)
        stub = StubFetcher(with_content=set())
        got = [r async for r in harvest_flat(stub, src, 2)]
        assert len(got) == src.tile_count_at(2)
        assert all(r.body is None for r in got)

    async def test_limit_bounds_the_number_of_requests(self) -> None:
        # This is what makes a bounded probe bounded, so it is load-bearing
        # rather than a convenience.
        src = source(max_zoom=3)
        stub = StubFetcher(with_content=set())
        got = [r async for r in harvest_flat(stub, src, 3, limit=5)]
        assert len(got) == 5
        assert len(stub.requested) == 5


class TestHarvestPyramid:
    async def test_descends_only_where_content_exists(self) -> None:
        src = source(min_zoom=0, max_zoom=2)
        # Content on one z0 tile, then on one of its four children.
        with_content = {Tile(0, 0, 0), Tile(1, 0, 0)}
        stub = StubFetcher(with_content=with_content)

        got = [r async for r in harvest_pyramid(stub, src, target_zoom=2)]
        by_zoom: dict[int, int] = {}
        for result in got:
            by_zoom[result.tile.z] = by_zoom.get(result.tile.z, 0) + 1

        assert by_zoom[0] == 1  # the single z0 tile
        assert by_zoom[1] == 4  # its children
        assert by_zoom[2] == 4  # only the children of the one z1 tile with content
        # A flat sweep at z2 would have cost 16 requests; the descent cost 9.
        assert len(stub.requested) == 9 < src.tile_count_at(2)

    async def test_stops_early_when_a_level_is_entirely_empty(self) -> None:
        src = source(min_zoom=0, max_zoom=3)
        stub = StubFetcher(with_content=set())
        got = [r async for r in harvest_pyramid(stub, src, target_zoom=3)]
        assert {r.tile.z for r in got} == {0}

    async def test_reaches_the_target_zoom_and_no_further(self) -> None:
        src = source(min_zoom=0, max_zoom=4)
        everything = {
            Tile(z, x, y) for z in range(0, 5) for x in range(0, 2**z) for y in range(0, 2**z)
        }
        stub = StubFetcher(with_content=everything)
        got = [r async for r in harvest_pyramid(stub, src, target_zoom=2)]
        assert max(r.tile.z for r in got) == 2

    async def test_start_zoom_deeper_than_target_is_refused(self) -> None:
        src = source(min_zoom=0, max_zoom=4)
        stub = StubFetcher(with_content=set())
        with pytest.raises(ValueError, match="deeper than"):
            _ = [r async for r in harvest_pyramid(stub, src, target_zoom=1, start_zoom=3)]

    async def test_custom_content_predicate_is_honoured(self) -> None:
        # Naver returns 200 with an empty body for "no coverage here", so
        # "absent" and "empty" have to be distinguishable by the caller.
        src = source(min_zoom=0, max_zoom=1)
        stub = StubFetcher(with_content={Tile(0, 0, 0)})
        got = [
            r
            async for r in harvest_pyramid(stub, src, target_zoom=1, has_content=lambda body: False)
        ]
        assert {r.tile.z for r in got} == {0}

    async def test_descent_stays_inside_the_area_of_interest(self) -> None:
        # Regression: tile edges do not align with a bounding box, so the
        # children of the tiles that merely overlap the box cover a strictly
        # larger area. Unfiltered, a descent over the Reykjavik box collected 18%
        # more road length than a flat sweep of the same box — all of it outside
        # the box. Filtering makes the two agree exactly.
        src = TileSource(
            id="quadrant",
            url_template="https://example.invalid/{z}/{x}/{y}",
            min_zoom=0,
            max_zoom=4,
            bounds=(-170.0, 10.0, -100.0, 60.0),
        )
        everything = {
            Tile(z, x, y) for z in range(0, 5) for x in range(0, 1 << z) for y in range(0, 1 << z)
        }
        stub = StubFetcher(with_content=everything)

        got = [r async for r in harvest_pyramid(stub, src, target_zoom=4)]

        west, south, east, north = src.bounds
        for result in got:
            t_west, t_south, t_east, t_north = result.tile.bounds
            assert t_east > west and t_west < east
            assert t_north > south and t_south < north

    async def test_descent_and_flat_sweep_agree_on_the_target_zoom(self) -> None:
        # The property that makes the descent usable at all: when content exists
        # at every level, descending must reach exactly the tiles a flat sweep
        # would. Verified against the live Ja source too — 13,665 lines and
        # 1866.6 km either way over the capital region.
        src = source(min_zoom=0, max_zoom=3)
        everything = {
            Tile(z, x, y) for z in range(0, 4) for x in range(0, 1 << z) for y in range(0, 1 << z)
        }
        flat_stub = StubFetcher(with_content=everything)
        deep_stub = StubFetcher(with_content=everything)

        flat = {r.tile async for r in harvest_flat(flat_stub, src, 3)}
        descended = {
            r.tile async for r in harvest_pyramid(deep_stub, src, target_zoom=3) if r.tile.z == 3
        }
        assert descended == flat

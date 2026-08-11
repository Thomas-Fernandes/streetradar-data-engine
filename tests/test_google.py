"""Decoding Google Street View coverage tiles, and the raster detector.

The fixtures are recorded: a real z17 coverage tile (trimmed to 40 panoramas so
it commits small — the trim leaves some link indices pointing past the end, which
is exactly the cross-tile-link case the graph builder must survive), a real
no-coverage tile, and a real per-panorama metadata response for the dates pass.
"""

from __future__ import annotations

import pathlib

import pytest

from streetradar.google import (
    EMPTY_RASTER_BYTES,
    TILE_ZOOM,
    Panorama,
    chain_graph,
    coverage_raster_url,
    getpanos,
    has_coverage,
    links_to_edges,
    parse_tile,
    tile_url,
)

FIXTURES = pathlib.Path(__file__).parent / "fixtures"
TILE = (FIXTURES / "google-tile-london.json").read_bytes()
EMPTY = (FIXTURES / "google-tile-empty.json").read_bytes()


class TestParseTile:
    def test_the_real_tile_decodes_to_positioned_panoramas(self) -> None:
        panos = parse_tile(TILE)
        assert len(panos) == 40
        assert all(-1.0 < p.lon < 1.0 and 51.0 < p.lat < 52.0 for p in panos)
        assert all(isinstance(p.id, str) and len(p.id) >= 20 for p in panos)

    def test_elevation_and_links_are_carried(self) -> None:
        panos = parse_tile(TILE)
        assert any(p.elevation is not None for p in panos)
        # Every current panorama sits on the road graph, so it links to a neighbour.
        assert all(p.links for p in panos)

    def test_a_no_coverage_tile_is_empty(self) -> None:
        # The rural fixture has data[1] == null. Over a sweep this is most tiles.
        assert parse_tile(EMPTY) == []

    @pytest.mark.parametrize("body", [None, b"", b")]}'\n[]"])
    def test_absent_or_trivial_bodies_are_empty(self, body: bytes | None) -> None:
        assert parse_tile(body) == []

    def test_the_xssi_prefix_is_stripped(self) -> None:
        # Google guards its JSON with )]}' ; without stripping it, nothing parses.
        assert TILE.startswith(b")]}'")
        assert parse_tile(TILE)

    def test_malformed_json_is_empty_not_a_raise(self) -> None:
        assert parse_tile(b")]}'\n<not json>") == []

    def test_a_broken_entry_does_not_lose_the_tile(self) -> None:
        import json

        text = TILE.decode()[len(")]}'\n") :]
        data = json.loads(text)
        data[1][1].insert(0, [["broken"]])  # a malformed entry at the front
        rebuilt = (")]}'\n" + json.dumps(data)).encode()
        assert len(parse_tile(rebuilt)) == 40  # the 40 good ones survive


class TestRasterDetector:
    def test_the_empty_tile_size_reads_as_no_coverage(self) -> None:
        assert not has_coverage(b"x" * EMPTY_RASTER_BYTES)
        assert not has_coverage(b"x" * (EMPTY_RASTER_BYTES - 10))

    def test_a_larger_tile_reads_as_coverage(self) -> None:
        assert has_coverage(b"x" * (EMPTY_RASTER_BYTES + 1))

    def test_no_body_is_no_coverage(self) -> None:
        assert not has_coverage(None)


class TestUrls:
    def test_the_tile_url_carries_the_coordinates_and_zoom_17(self) -> None:
        url = tile_url(65489, 43584)
        assert "1i65489" in url and "2i43584" in url and "3i17" in url
        assert TILE_ZOOM == 17

    def test_the_raster_url_carries_x_y_z(self) -> None:
        url = coverage_raster_url(129, 85, 8)
        assert "x=129" in url and "y=85" in url and "z=8" in url
        assert "lyrs=svv" in url


class TestLinksToEdges:
    def test_it_builds_the_undirected_road_graph(self) -> None:
        panos = parse_tile(TILE)
        edges = links_to_edges(panos)
        assert edges
        # Undirected: an edge is stored one way only.
        assert all(a < b for a, b in edges)

    def test_links_past_the_tile_are_dropped(self) -> None:
        # The trimmed fixture has links to indices >= 40 (panos in the real tile
        # that were trimmed away). Those must not raise or invent an edge.
        panos = parse_tile(TILE)
        assert any(any(i >= len(panos) for i in p.links) for p in panos)
        links_to_edges(panos)  # does not raise

    def test_a_two_panorama_link_is_one_edge(self) -> None:
        a = Panorama(id="aaa", lon=0.0, lat=0.0, elevation=None, links=(1,))
        b = Panorama(id="bbb", lon=0.0, lat=0.0, elevation=None, links=(0,))
        assert links_to_edges([a, b]) == {("aaa", "bbb")}


class TestChainGraph:
    def make(self, *links: tuple[int, ...]) -> list[Panorama]:
        # Each panorama at a distinct position so the polylines are traceable.
        return [
            Panorama(id=f"p{i}", lon=float(i), lat=0.0, elevation=None, links=links[i])
            for i in range(len(links))
        ]

    def test_a_straight_road_is_one_polyline(self) -> None:
        panos = self.make((1,), (0, 2), (1, 3), (2,))
        lines = chain_graph(panos)
        assert len(lines) == 1
        assert [lon for lon, _ in lines[0]] == [0.0, 1.0, 2.0, 3.0]

    def test_a_junction_splits_the_roads(self) -> None:
        # A cross: node 0 links to 1, 2, 3, 4 — four spurs from one junction.
        panos = self.make((1, 2, 3, 4), (0,), (0,), (0,), (0,))
        lines = chain_graph(panos)
        assert len(lines) == 4
        assert all(len(line) == 2 for line in lines)

    def test_a_closed_loop_is_walked_once(self) -> None:
        panos = self.make((1, 2), (0, 2), (0, 1))  # triangle, all degree 2
        lines = chain_graph(panos)
        assert len(lines) == 1
        assert len(lines[0]) >= 3

    def test_cross_tile_links_are_dropped(self) -> None:
        # A link to an index past the tile (a road leaving into the next tile)
        # must not raise or invent a vertex.
        panos = self.make((1, 99), (0,))
        lines = chain_graph(panos)
        assert lines == [[(0.0, 0.0), (1.0, 0.0)]]

    def test_the_real_tile_chains_every_edge_once(self) -> None:
        panos = parse_tile(TILE)
        lines = chain_graph(panos)
        assert lines
        # Every graph edge appears in exactly one polyline (as a consecutive pair).
        from collections import Counter

        pairs: Counter[frozenset[str]] = Counter()
        pos = {(p.lon, p.lat): p.id for p in panos}
        for line in lines:
            for a, b in zip(line, line[1:], strict=False):
                pairs[frozenset((pos[a], pos[b]))] += 1
        assert pairs and all(count == 1 for count in pairs.values())


class StubGetter:
    def __init__(self, body: bytes) -> None:
        self.body = body
        self.urls: list[str] = []

    async def get(self, url: str, *, missing_ok: bool = False) -> bytes | None:
        self.urls.append(url)
        return self.body


class TestGetpanos:
    async def test_it_fetches_and_parses(self) -> None:
        getter = StubGetter(TILE)
        panos = await getpanos(getter, 65489, 43584)
        assert len(panos) == 40
        assert "1i65489" in getter.urls[0]

    async def test_an_empty_tile_is_no_panoramas(self) -> None:
        assert await getpanos(StubGetter(EMPTY), 66628, 46101) == []


def test_the_fixture_is_a_real_google_response() -> None:
    assert TILE.startswith(b")]}'")

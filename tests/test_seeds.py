"""Where a flood starts.

Two seed sources with different jobs: everything the last run found, and a
regular lattice underneath it to catch what grew with no connection to it. The
properties that matter are that the first loses nothing to duplication across
parts, and that the second is anchored to the tile grid — a lattice that shifted
with the bounding box would re-probe a different sample every run.
"""

from __future__ import annotations

from pathlib import Path

import pytest

from streetradar.seeds import SAMPLING_STEP, sampling_seeds, seeds_from_parts
from streetradar.store import PartWriter
from streetradar.tiles import Tile, TileSource

ZOOM = 17


def source(bounds: tuple[float, float, float, float] = (5.7, 49.4, 6.6, 50.2)) -> TileSource:
    return TileSource(
        id="test",
        url_template="https://example.invalid/{z}/{x}/{y}",
        min_zoom=0,
        max_zoom=20,
        bounds=bounds,
    )


def write_parts(directory: Path, tiles: list[list[Tile]]) -> None:
    """One part per inner list, so cross-part duplicates can be exercised."""
    writer = PartWriter(directory, provider="apple", run_id="r", capture_precision="second")
    for part in tiles:
        for tile in part:
            writer.batch.add(
                tile_x=tile.x,
                tile_y=tile.y,
                pano_id=[1],
                lon=[6.0],
                lat=[49.6],
                captured_at=[1554163200000],
            )
        writer.flush()


class TestSeedsFromParts:
    def test_every_tile_written_comes_back(self, tmp_path: Path) -> None:
        tiles = [Tile(ZOOM, 67756, 44565), Tile(ZOOM, 67757, 44565)]
        write_parts(tmp_path, [tiles])
        assert set(seeds_from_parts(tmp_path, ZOOM)) == set(tiles)

    def test_a_tile_seen_many_times_is_seeded_once(self, tmp_path: Path) -> None:
        tile = Tile(ZOOM, 67756, 44565)
        write_parts(tmp_path, [[tile, tile, tile]])
        assert list(seeds_from_parts(tmp_path, ZOOM)) == [tile]

    def test_duplicates_across_parts_are_removed(self, tmp_path: Path) -> None:
        # Parts are independent files, so nothing but this deduplicates them.
        tile = Tile(ZOOM, 67756, 44565)
        other = Tile(ZOOM, 67760, 44570)
        write_parts(tmp_path, [[tile, other], [tile], [other, tile]])
        assert sorted(seeds_from_parts(tmp_path, ZOOM)) == sorted([tile, other])

    def test_no_parts_is_no_seeds(self, tmp_path: Path) -> None:
        assert list(seeds_from_parts(tmp_path, ZOOM)) == []

    def test_the_zoom_asked_for_is_the_zoom_returned(self, tmp_path: Path) -> None:
        # Parts store tile_x/tile_y without a zoom; it belongs to the run.
        write_parts(tmp_path, [[Tile(ZOOM, 67756, 44565)]])
        assert all(t.z == 15 for t in seeds_from_parts(tmp_path, 15))

    def test_it_streams_rather_than_materialising(self, tmp_path: Path) -> None:
        write_parts(tmp_path, [[Tile(ZOOM, 67756 + i, 44565) for i in range(5)]])
        seeds = seeds_from_parts(tmp_path, ZOOM)
        assert next(iter(seeds)) is not None


class TestSamplingSeeds:
    def test_the_lattice_is_anchored_to_the_tile_grid(self) -> None:
        # Not to the box: a lattice that shifted with the bounds would sample a
        # different set every run, so a resumed flood would re-probe fresh tiles
        # for ever and the net would never settle.
        wide = set(sampling_seeds(source((5.0, 49.0, 7.0, 50.5)), ZOOM))
        narrow = set(sampling_seeds(source((5.7, 49.4, 6.6, 50.2)), ZOOM))
        assert narrow
        assert narrow <= wide

    def test_every_sampled_tile_is_on_the_step(self) -> None:
        for tile in sampling_seeds(source(), ZOOM):
            assert tile.x % SAMPLING_STEP == 0
            assert tile.y % SAMPLING_STEP == 0

    def test_every_sampled_tile_is_inside_the_box(self) -> None:
        src = source()
        x0, y0, x1, y1 = src.tile_range(ZOOM)
        for tile in sampling_seeds(src, ZOOM):
            assert x0 <= tile.x < x1
            assert y0 <= tile.y < y1

    def test_it_thins_by_the_square_of_the_step(self) -> None:
        src = source()
        full = src.tile_count_at(ZOOM)
        sampled = sum(1 for _ in sampling_seeds(src, ZOOM))
        assert sampled == pytest.approx(full / SAMPLING_STEP**2, rel=0.1)

    def test_a_step_of_one_is_the_whole_box(self) -> None:
        src = source()
        assert sum(1 for _ in sampling_seeds(src, ZOOM, step=1)) == src.tile_count_at(ZOOM)

    def test_a_zero_step_is_refused(self) -> None:
        with pytest.raises(ValueError, match="step must be positive"):
            list(sampling_seeds(source(), ZOOM, step=0))

    def test_it_never_enumerates_the_box_it_samples(self) -> None:
        # Australia at zoom 17 is ~2x10^8 tiles and ~2x10^5 sampled ones. A
        # filtering implementation would be correct and unusably slow.
        australia = source((112.0, -44.0, 154.0, -10.0))
        assert australia.tile_count_at(ZOOM) > 100_000_000
        assert sum(1 for _ in sampling_seeds(australia, ZOOM)) < 1_000_000


class TestTileRange:
    def test_it_agrees_with_enumeration(self) -> None:
        src = source()
        x0, y0, x1, y1 = src.tile_range(14)
        assert (x1 - x0) * (y1 - y0) == src.tile_count_at(14)
        assert {(t.x, t.y) for t in src.tiles_at(14)} == {
            (x, y) for x in range(x0, x1) for y in range(y0, y1)
        }

    def test_a_zoom_outside_the_published_range_is_refused(self) -> None:
        with pytest.raises(ValueError, match="outside published range"):
            source().tile_range(21)

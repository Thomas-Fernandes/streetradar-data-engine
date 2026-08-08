#!/usr/bin/env python3
"""
Naver Raster Tile Generator (PNG → MBTiles → PMTiles)

Generates z/x/y PNG raster tiles from Naver processed parquet (LineStrings in GeoParquet):
- Input: data/naver/processed/{session}/naver_panoramas.parquet
- Reads geometry from column `geometry` (GeoParquet WKB), converts to Shapely (WGS84)
- Indexes geometries in WGS84; selects via WGS84 query polygon; projects clipped results to Web Mercator for rendering
- 512px tiles, zoom 1-16, variable line thickness, white halo
- Outputs MBTiles raster with PNG tiles and converts to PMTiles

Usage (default paths):
    python naver_tile_generator.py

"""

import io
import logging
import math
import sqlite3
import sys
import time
from collections.abc import Generator
from dataclasses import dataclass
from pathlib import Path

import cairo
import geopandas as gpd
import numpy as np
import pandas as pd
from PIL import Image
from pyproj import Transformer
from shapely import clip_by_rect
from shapely import wkb as shapely_wkb
from shapely.geometry import LineString, MultiLineString, box
from shapely.ops import transform
from shapely.strtree import STRtree

# CRS constants
WGS84_CRS = "EPSG:4326"
WEB_MERCATOR_CRS = "EPSG:3857"
TILE_SIZE = 512
DEFAULT_BUFFER_DEGREES = 0.002


@dataclass
class TileStyle:
    zoom_range: tuple[int, int]
    line_width: float
    halo_width: float
    line_color: tuple[int, int, int, int]
    halo_color: tuple[int, int, int, int]

    def applies_to_zoom(self, zoom: int) -> bool:
        return self.zoom_range[0] <= zoom <= self.zoom_range[1]


@dataclass
class TileConfig:
    min_zoom: int = 1
    max_zoom: int = 16
    tile_size: int = 512
    buffer_degrees: float = DEFAULT_BUFFER_DEGREES
    styles: list[TileStyle] = None
    generate_empty_tiles: bool = False
    optimize_png: bool = False

    def __post_init__(self):
        if self.styles is None:
            # Naver green #03cf5d → RGB (3, 207, 93)
            naver_rgb = (3, 207, 93)
            self.styles = [
                TileStyle((1, 3), 1.2, 0.0, (*naver_rgb, 255), (255, 255, 255, 0)),
                TileStyle((4, 6), 1.6, 0.0, (*naver_rgb, 255), (255, 255, 255, 0)),
                TileStyle((7, 9), 2.0, 0.0, (*naver_rgb, 255), (255, 255, 255, 0)),
                TileStyle((10, 11), 2.2, 0.0, (*naver_rgb, 255), (255, 255, 255, 0)),
                TileStyle((12, 16), 2.5, 0.0, (*naver_rgb, 255), (255, 255, 255, 0)),
            ]

    def get_style_for_zoom(self, zoom: int) -> TileStyle | None:
        for s in self.styles:
            if s.applies_to_zoom(zoom):
                return s
        return None


class WebMercatorTileCalculator:
    WM_ORIGIN_SHIFT = 20037508.342789244
    WM_SIZE = WM_ORIGIN_SHIFT * 2

    @staticmethod
    def deg2num(lat_deg: float, lon_deg: float, zoom: int) -> tuple[int, int]:
        lat_rad = math.radians(lat_deg)
        n = 2.0**zoom
        x_tile = int((lon_deg + 180.0) / 360.0 * n)
        y_tile = int((1.0 - math.asinh(math.tan(lat_rad)) / math.pi) / 2.0 * n)
        return x_tile, y_tile

    @staticmethod
    def num2deg(x_tile: int, y_tile: int, zoom: int) -> tuple[float, float]:
        n = 2.0**zoom
        lon_deg = x_tile / n * 360.0 - 180.0
        lat_rad = math.atan(math.sinh(math.pi * (1 - 2 * y_tile / n)))
        lat_deg = math.degrees(lat_rad)
        return lat_deg, lon_deg

    @staticmethod
    def tile_bbox_wgs84(x_tile: int, y_tile: int, zoom: int) -> tuple[float, float, float, float]:
        lat_nw, lon_nw = WebMercatorTileCalculator.num2deg(x_tile, y_tile, zoom)
        lat_se, lon_se = WebMercatorTileCalculator.num2deg(x_tile + 1, y_tile + 1, zoom)
        return lon_nw, lat_se, lon_se, lat_nw

    @staticmethod
    def tile_bbox_web_mercator(
        x_tile: int, y_tile: int, zoom: int
    ) -> tuple[float, float, float, float]:
        n = 2.0**zoom
        unit = WebMercatorTileCalculator.WM_SIZE / n
        minx = -WebMercatorTileCalculator.WM_ORIGIN_SHIFT + x_tile * unit
        maxx = -WebMercatorTileCalculator.WM_ORIGIN_SHIFT + (x_tile + 1) * unit
        miny = WebMercatorTileCalculator.WM_ORIGIN_SHIFT - (y_tile + 1) * unit
        maxy = WebMercatorTileCalculator.WM_ORIGIN_SHIFT - y_tile * unit
        return minx, miny, maxx, maxy

    @staticmethod
    def enumerate_tiles_for_bounds(
        minx: float, miny: float, maxx: float, maxy: float, zoom: int
    ) -> Generator[tuple[int, int], None, None]:
        x_min, y_max = WebMercatorTileCalculator.deg2num(miny, minx, zoom)
        x_max, y_min = WebMercatorTileCalculator.deg2num(maxy, maxx, zoom)
        n = 2**zoom
        x_min = max(0, min(x_min, n - 1))
        x_max = max(0, min(x_max, n - 1))
        y_min = max(0, min(y_min, n - 1))
        y_max = max(0, min(y_max, n - 1))
        for x in range(x_min, x_max + 1):
            for y in range(y_min, y_max + 1):
                yield x, y


class GeometryRenderer:
    def __init__(self, tile_size: int = TILE_SIZE):
        self.tile_size = tile_size

    def create_empty_tile(self) -> Image.Image:
        return Image.new("RGBA", (self.tile_size, self.tile_size), (0, 0, 0, 0))

    def web_mercator_to_pixel(
        self, x_m: float, y_m: float, bbox_m: tuple[float, float, float, float]
    ) -> tuple[float, float]:
        minx, miny, maxx, maxy = bbox_m
        px_norm = (x_m - minx) / (maxx - minx)
        py_norm = 1.0 - (y_m - miny) / (maxy - miny)
        return px_norm * self.tile_size, py_norm * self.tile_size

    def linestring_to_pixel_coords(
        self, linestring: LineString, bbox_m: tuple[float, float, float, float]
    ) -> list[tuple[float, float]]:
        coords = []
        for x, y in linestring.coords:
            px, py = self.web_mercator_to_pixel(x, y, bbox_m)
            coords.append((px, py))
        return coords

    def render_linestrings(
        self,
        linestrings: list[LineString],
        bbox_m: tuple[float, float, float, float],
        style: TileStyle,
    ) -> Image.Image:
        surface = cairo.ImageSurface(cairo.FORMAT_ARGB32, self.tile_size, self.tile_size)
        ctx = cairo.Context(surface)
        ctx.set_antialias(cairo.ANTIALIAS_BEST)
        ctx.set_line_join(cairo.LINE_JOIN_ROUND)
        ctx.set_line_cap(cairo.LINE_CAP_ROUND)

        for geom in linestrings:
            try:
                pixel_coords = self.linestring_to_pixel_coords(geom, bbox_m)
                if len(pixel_coords) < 2:
                    continue
                # Halo disabled via halo_width=0 in styles
                ctx.set_line_width(style.line_width)
                ctx.set_source_rgba(
                    style.line_color[0] / 255.0,
                    style.line_color[1] / 255.0,
                    style.line_color[2] / 255.0,
                    style.line_color[3] / 255.0,
                )
                ctx.new_path()
                ctx.move_to(pixel_coords[0][0], pixel_coords[0][1])
                for x, y in pixel_coords[1:]:
                    ctx.line_to(x, y)
                ctx.stroke()
            except Exception:
                continue

        buf = surface.get_data()
        img = Image.frombuffer("RGBA", (self.tile_size, self.tile_size), buf, "raw", "BGRA", 0, 1)
        return img

    def optimize_png(self, img: Image.Image) -> bytes:
        buffer = io.BytesIO()
        img.save(buffer, format="PNG", optimize=False, compress_level=1)
        return buffer.getvalue()


class NaverTileGenerator:
    def __init__(self, config: TileConfig = None):
        self.config = config or TileConfig()
        self.logger = self._setup_logging()

        # Store and index geometries in WGS84 for robust spatial querying
        self.lines_wgs84: list[LineString] = []
        self.lines_tree_wgs84: STRtree | None = None

        self.transformer_to_wm = Transformer.from_crs(WGS84_CRS, WEB_MERCATOR_CRS, always_xy=True)
        self.transformer_to_wgs84 = Transformer.from_crs(
            WEB_MERCATOR_CRS, WGS84_CRS, always_xy=True
        )

        self.renderer = GeometryRenderer(self.config.tile_size)
        self.stats = {
            "tiles_generated": 0,
            "tiles_empty": 0,
            "tiles_with_data": 0,
            "total_geometries_processed": 0,
        }

        self.logger.info("🗺️  Naver Raster Tile Generator initialized")
        self.logger.info(f"📐 Zoom range: {self.config.min_zoom}-{self.config.max_zoom}")
        self.logger.info(f"🖼️  Tile size: {self.config.tile_size}px")
        self.logger.info(f"🎨 Style layers: {len(self.config.styles)} zoom ranges")

    def _setup_logging(self) -> logging.Logger:
        logger = logging.getLogger("NaverTileGenerator")
        logger.setLevel(logging.INFO)
        logger.handlers.clear()
        console_handler = logging.StreamHandler()
        console_handler.setLevel(logging.INFO)
        formatter = logging.Formatter("%(message)s")
        console_handler.setFormatter(formatter)
        logger.addHandler(console_handler)
        return logger

    def load_parquet(self, parquet_file: str | Path):
        self.logger.info("📂 Loading Parquet data (GeoParquet geometry)…")
        start = time.time()
        p = Path(parquet_file)
        if not p.exists():
            raise FileNotFoundError(f"Parquet file not found: {p}")

        lines_wgs84: list[LineString] = []

        # Try GeoPandas GeoParquet path first (preferred)
        try:
            gdf = gpd.read_parquet(p)
            if "geometry" in gdf.columns and len(gdf) > 0:
                if "geometry_type" in gdf.columns:
                    mask = gdf["geometry_type"].astype(str).str.contains("LINESTRING", na=False)
                    gdf = gdf[mask]
                geoms = gdf.geometry
                for geom in geoms:
                    if isinstance(geom, LineString):
                        lines_wgs84.append(geom)
                    elif isinstance(geom, MultiLineString):
                        lines_wgs84.extend([ls for ls in geom.geoms if isinstance(ls, LineString)])
        except Exception:
            # Fallback: Pandas + WKB decode
            df = pd.read_parquet(p, columns=["geometry", "geometry_type"])
            df = df[df["geometry"].notna()]
            if "geometry_type" in df.columns:
                df = df[df["geometry_type"].astype(str).str.contains("LINESTRING", na=False)]
            for blob in df["geometry"].tolist():
                try:
                    geom = shapely_wkb.loads(blob)
                    if isinstance(geom, LineString):
                        lines_wgs84.append(geom)
                    elif isinstance(geom, MultiLineString):
                        lines_wgs84.extend([ls for ls in geom.geoms if isinstance(ls, LineString)])
                except Exception:
                    continue

        self.lines_wgs84 = lines_wgs84
        elapsed = time.time() - start
        self.logger.info(f"🛣️  Loaded {len(self.lines_wgs84):,} LineStrings in {elapsed:.1f}s")

    def build_spatial_index(self):
        self.logger.info("🗂️  Building spatial index…")
        start = time.time()
        if not self.lines_wgs84:
            raise ValueError("No lines loaded")
        self.lines_tree_wgs84 = STRtree(self.lines_wgs84)
        elapsed = time.time() - start
        self.logger.info(f"✅ Spatial index built in {elapsed:.1f}s")

    def get_data_bounds_wgs84(self) -> tuple[float, float, float, float]:
        if not self.lines_wgs84:
            return 124.6, 33.0, 131.0, 38.8
        minx = min(geom.bounds[0] for geom in self.lines_wgs84)
        miny = min(geom.bounds[1] for geom in self.lines_wgs84)
        maxx = max(geom.bounds[2] for geom in self.lines_wgs84)
        maxy = max(geom.bounds[3] for geom in self.lines_wgs84)
        return minx, miny, maxx, maxy

    def _resolve_candidate(self, candidate):
        # Support both shapely returning indices (np ints) and geometries
        if isinstance(candidate, (int, np.integer)):
            idx = int(candidate)
            if 0 <= idx < len(self.lines_wgs84):
                return self.lines_wgs84[idx]
            return None
        return candidate

    def select_geometries_for_tile(self, x_tile: int, y_tile: int, zoom: int) -> list[LineString]:
        # Build query polygon in WGS84 and use WGS84 index
        minx_w, miny_w, maxx_w, maxy_w = WebMercatorTileCalculator.tile_bbox_wgs84(
            x_tile, y_tile, zoom
        )
        buffer = self.config.buffer_degrees
        query_poly_wgs = box(minx_w - buffer, miny_w - buffer, maxx_w + buffer, maxy_w + buffer)

        if self.lines_tree_wgs84 is None:
            return []

        try:
            candidates = self.lines_tree_wgs84.query(query_poly_wgs)
            minx, miny, maxx, maxy = query_poly_wgs.bounds
            result_wm: list[LineString] = []

            def to_wm(x, y, z=None):
                return self.transformer_to_wm.transform(x, y)

            # Normalize candidates to geometries
            if len(candidates) == 0:
                return []
            sample = candidates[0]
            if isinstance(sample, (int, np.integer)):
                candidate_iter = (self._resolve_candidate(c) for c in candidates)
            else:
                candidate_iter = candidates

            for geom in candidate_iter:
                if geom is None:
                    continue
                try:
                    clipped = clip_by_rect(geom, minx, miny, maxx, maxy)
                    if not clipped.is_empty:
                        if isinstance(clipped, LineString):
                            projected = transform(to_wm, clipped)
                            if isinstance(projected, LineString):
                                result_wm.append(projected)
                        elif isinstance(clipped, MultiLineString):
                            for line in clipped.geoms:
                                if isinstance(line, LineString):
                                    projected = transform(to_wm, line)
                                    if isinstance(projected, LineString):
                                        result_wm.append(projected)
                except Exception:
                    # Fallback: project original geometry
                    if isinstance(geom, LineString):
                        projected = transform(to_wm, geom)
                        if isinstance(projected, LineString):
                            result_wm.append(projected)
                    elif isinstance(geom, MultiLineString):
                        for g in geom.geoms:
                            if isinstance(g, LineString):
                                projected = transform(to_wm, g)
                                if isinstance(projected, LineString):
                                    result_wm.append(projected)
            return result_wm
        except Exception:
            return []

    def generate_tile(self, x_tile: int, y_tile: int, zoom: int) -> bytes | None:
        style = self.config.get_style_for_zoom(zoom)
        if style is None:
            return None
        geoms = self.select_geometries_for_tile(x_tile, y_tile, zoom)
        has_data = len(geoms) > 0
        if not has_data and not self.config.generate_empty_tiles:
            return None
        bbox_m = WebMercatorTileCalculator.tile_bbox_web_mercator(x_tile, y_tile, zoom)
        img = (
            self.renderer.render_linestrings(geoms, bbox_m, style)
            if has_data
            else self.renderer.create_empty_tile()
        )
        if has_data:
            self.stats["tiles_with_data"] += 1
            self.stats["total_geometries_processed"] += len(geoms)
        else:
            self.stats["tiles_empty"] += 1
        buffer = io.BytesIO()
        img.save(buffer, format="PNG")
        self.stats["tiles_generated"] += 1
        return buffer.getvalue()

    def enumerate_all_tiles(self) -> Generator[tuple[int, int, int], None, None]:
        minx, miny, maxx, maxy = self.get_data_bounds_wgs84()
        self.logger.info(f"📊 Data bounds: ({miny:.4f}, {minx:.4f}) → ({maxy:.4f}, {maxx:.4f})")
        for zoom in range(self.config.min_zoom, self.config.max_zoom + 1):
            zoom_tiles = 0
            for x_tile, y_tile in WebMercatorTileCalculator.enumerate_tiles_for_bounds(
                minx, miny, maxx, maxy, zoom
            ):
                yield x_tile, y_tile, zoom
                zoom_tiles += 1
            self.logger.info(f"📏 Zoom {zoom}: {zoom_tiles:,} tiles")

    def generate_mbtiles(self, output_file: str | Path) -> dict:
        output_path = Path(output_file)
        self.logger.info(f"🗃️  Generating MBTiles to {output_path}")
        start_time = time.time()
        if output_path.exists():
            output_path.unlink()
        conn = sqlite3.connect(str(output_path))
        cursor = conn.cursor()
        cursor.execute("CREATE TABLE metadata (name text, value text);")
        cursor.execute(
            "CREATE TABLE tiles (zoom_level integer, tile_column integer, tile_row integer, tile_data blob);"
        )
        cursor.execute(
            "CREATE UNIQUE INDEX tile_index on tiles (zoom_level, tile_column, tile_row);"
        )

        bounds = self.get_data_bounds_wgs84()
        metadata = {
            "name": "Naver Coverage",
            "type": "overlay",
            "version": "1.0",
            "description": "Naver street coverage raster tiles",
            "format": "png",
            "bounds": f"{bounds[0]},{bounds[1]},{bounds[2]},{bounds[3]}",
            "minzoom": str(self.config.min_zoom),
            "maxzoom": str(self.config.max_zoom),
            "center": f"{(bounds[0] + bounds[2]) / 2},{(bounds[1] + bounds[3]) / 2},{self.config.max_zoom}",
        }
        for k, v in metadata.items():
            cursor.execute("INSERT INTO metadata (name, value) VALUES (?, ?)", (k, v))

        self.stats = {
            "tiles_generated": 0,
            "tiles_empty": 0,
            "tiles_with_data": 0,
            "total_geometries_processed": 0,
        }
        batch_size = 1000
        tiles_processed = 0
        batch_data = []
        for x_tile, y_tile, zoom in self.enumerate_all_tiles():
            png_bytes = self.generate_tile(x_tile, y_tile, zoom)
            if png_bytes is not None:
                y_tms = (2**zoom - 1) - y_tile
                batch_data.append((zoom, x_tile, y_tms, png_bytes))
            tiles_processed += 1
            if len(batch_data) >= batch_size:
                cursor.executemany(
                    "INSERT INTO tiles (zoom_level, tile_column, tile_row, tile_data) VALUES (?, ?, ?, ?)",
                    batch_data,
                )
                conn.commit()
                batch_data = []
        if batch_data:
            cursor.executemany(
                "INSERT INTO tiles (zoom_level, tile_column, tile_row, tile_data) VALUES (?, ?, ?, ?)",
                batch_data,
            )
            conn.commit()
        conn.close()
        elapsed = time.time() - start_time
        file_size_mb = output_path.stat().st_size / (1024 * 1024)
        self.logger.info(
            f"✅ MBTiles generation completed in {elapsed:.1f}s, size {file_size_mb:.1f} MB"
        )
        self.stats["generation_time_seconds"] = elapsed
        self.stats["tiles_per_second"] = tiles_processed / elapsed if elapsed > 0 else 0
        self.stats["file_size_mb"] = file_size_mb
        return self.stats

    def process_mbtiles(self, parquet_file: str | Path, output_file: str | Path) -> dict:
        total_start = time.time()
        try:
            self.load_parquet(parquet_file)
            self.build_spatial_index()
            stats = self.generate_mbtiles(output_file)
            total_elapsed = time.time() - total_start
            self.logger.info(f"\n🎉 Processing completed in {total_elapsed:.1f}s!")
            self.logger.info("📊 Statistics:")
            self.logger.info(f"   • {stats['tiles_generated']:,} tiles generated")
            self.logger.info(f"   • {stats['tiles_with_data']:,} tiles with data")
            self.logger.info(f"   • {stats['tiles_empty']:,} empty tiles")
            self.logger.info(f"   • {stats['total_geometries_processed']:,} geometries processed")
            self.logger.info(f"   • {stats['tiles_per_second']:.1f} tiles/second")
            return stats
        except Exception as e:
            self.logger.error(f"❌ Processing failed: {e}")
            raise


def main():
    print("🇰🇷 Naver Raster Tile Generator")
    print("=" * 50)

    base_dir = Path(__file__).parent.parent.parent / "data" / "naver"
    processed_dir = base_dir / "processed"
    output_dir = base_dir / "tile"

    # Use latest session directory if exists
    if processed_dir.exists():
        session_dirs = [d for d in processed_dir.iterdir() if d.is_dir() and d.name.isdigit()]
        if session_dirs:
            latest = max(session_dirs, key=lambda d: int(d.name))
            parquet_file = latest / "naver_panoramas.parquet"
        else:
            parquet_file = processed_dir / "naver_panoramas.parquet"
    else:
        parquet_file = processed_dir / "naver_panoramas.parquet"

    mbtiles_output = output_dir / "naver.mbtiles"
    pmtiles_output = output_dir / "naver.pmtiles"

    print(f"📂 Input parquet: {parquet_file}")
    print(f"📂 Output directory: {output_dir}")
    print("🔍 Zoom range: 1-16")
    print("📐 Tile size: 512px")

    if not parquet_file.exists():
        print(f"\n❌ Parquet file not found: {parquet_file}")
        print("💡 Run naver_processor.py first to generate processed data")
        sys.exit(1)

    output_dir.mkdir(parents=True, exist_ok=True)

    config = TileConfig(
        min_zoom=1, max_zoom=16, tile_size=512, generate_empty_tiles=False, optimize_png=False
    )
    generator = NaverTileGenerator(config)

    try:
        print("\n🗃️  Generating MBTiles…")
        stats = generator.process_mbtiles(parquet_file, mbtiles_output)
        print(f"\n✅ MBTiles generation completed: {mbtiles_output}")
        print(f"📊 File size: {stats['file_size_mb']:.1f} MB")
        print(f"🚀 Performance: {stats['tiles_per_second']:.1f} tiles/second")

        # Convert to PMTiles
        print("\n🔄 Converting MBTiles to PMTiles…")
        try:
            import shutil
            import subprocess

            pmtiles_cmd = shutil.which("pmtiles")
            if not pmtiles_cmd:
                print("⚠️  pmtiles not found. Install with: npm install -g pmtiles")
                print(f"💡 Then run: pmtiles convert {mbtiles_output} {pmtiles_output}")
                return
            subprocess.run(
                ["pmtiles", "convert", str(mbtiles_output), str(pmtiles_output)],
                check=True,
                capture_output=True,
                text=True,
            )
            pmtiles_size_mb = pmtiles_output.stat().st_size / (1024 * 1024)
            print(f"✅ PMTiles conversion completed: {pmtiles_output}")
            print(f"📊 PMTiles size: {pmtiles_size_mb:.1f} MB")
            print("\n🧹 Cleaning up MBTiles…")
            mbtiles_output.unlink(missing_ok=True)
        except subprocess.CalledProcessError as e:
            print(f"❌ PMTiles conversion failed: {e}")
            print(f"💡 Convert manually: pmtiles convert {mbtiles_output} {pmtiles_output}")
        except FileNotFoundError:
            print("❌ pmtiles command not found")
            print(f"💡 Convert manually: pmtiles convert {mbtiles_output} {pmtiles_output}")
    except KeyboardInterrupt:
        print("\n⚠️  Generation interrupted by user")
        sys.exit(1)
    except Exception as e:
        print(f"\n❌ Error: {e}")
        import traceback

        traceback.print_exc()
        sys.exit(1)


if __name__ == "__main__":
    main()

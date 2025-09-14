#!/usr/bin/env python3
"""
JA360 Raster Tile Generator

Generates z/x/y PNG raster tiles from JA360 coverage data (LineStrings) with:
- Web Mercator projection (EPSG:3857)
- 512px tiles for better rendering + overzoom capability
- Variable line thickness by zoom level
- Transparent PNG output with white halo for readability
- Spatial indexing for efficient geometry selection
- PNG optimization (palette compression)

Output formats:
- Option A (recommended): MBTiles → PMTiles → R2 → Worker → /z/x/y.png
- Option B (alternative): Static z/x/y.png files

Input data:
- coverage_segments.geojson: Detailed coverage segments 
- coverage_roads.geojson: Complete OSM roads with coverage

Configuration:
- Zoom levels: 1-18
- Tile size: 512px
- Style: Variable thickness, white halo, antialiasing
- Projection: Web Mercator (EPSG:3857)
- Format: PNG with transparency
"""

import os
import sys
import time
import math
import sqlite3
import json
import logging
from pathlib import Path
from typing import Dict, List, Tuple, Optional, Union, Generator
from datetime import datetime
from dataclasses import dataclass
from collections import defaultdict
import warnings

import numpy as np
import pandas as pd
import geopandas as gpd
from shapely.geometry import Point, LineString, MultiLineString, Polygon, box
from shapely.strtree import STRtree
from shapely.ops import transform
import pyproj
from pyproj import CRS, Transformer

from PIL import Image, ImageDraw, ImageOps
import io

# Suppress warnings for cleaner output
warnings.filterwarnings('ignore', category=UserWarning)
warnings.filterwarnings('ignore', category=FutureWarning)

# Constants
WGS84_CRS = 'EPSG:4326'
WEB_MERCATOR_CRS = 'EPSG:3857'
TILE_SIZE = 512
DEFAULT_BUFFER_DEGREES = 0.002  # Buffer to avoid edge cuts


@dataclass
class TileStyle:
    """Style configuration for different zoom levels"""
    zoom_range: Tuple[int, int]  # (min_zoom, max_zoom) inclusive
    line_width: float            # Line width in pixels
    halo_width: float           # Halo width in pixels
    line_color: Tuple[int, int, int, int]    # RGBA
    halo_color: Tuple[int, int, int, int]    # RGBA
    
    def applies_to_zoom(self, zoom: int) -> bool:
        """Check if this style applies to given zoom level"""
        return self.zoom_range[0] <= zoom <= self.zoom_range[1]


@dataclass
class TileConfig:
    """Configuration for tile generation"""
    # Zoom levels
    min_zoom: int = 1
    max_zoom: int = 11
    
    # Tile properties
    tile_size: int = 512
    buffer_degrees: float = DEFAULT_BUFFER_DEGREES
    
    # Layer selection - always use roads
    use_roads_only: bool = True  # Always use roads instead of segments
    
    # Styles by zoom level
    styles: List[TileStyle] = None
    
    # Output options
    generate_empty_tiles: bool = True  # Generate transparent PNG for empty tiles
    optimize_png: bool = True          # Use PNG palette optimization
    
    def __post_init__(self):
        if self.styles is None:
            # Convert hex color #fbbe01 to RGB
            yellow_rgb = (251, 190, 1)
            
            self.styles = [
                # Low zooms: thin lines, minimal halo
                TileStyle(
                    zoom_range=(1, 3),
                    line_width=1.0,
                    halo_width=0.3,  # Very thin halo
                    line_color=(*yellow_rgb, 255),      # Yellow
                    halo_color=(255, 255, 255, 150)     # White halo (reduced opacity)
                ),
                # Medium-low zooms: slightly thicker, minimal halo
                TileStyle(
                    zoom_range=(4, 6),
                    line_width=1.2,
                    halo_width=0.4,  # Thin halo
                    line_color=(*yellow_rgb, 255),
                    halo_color=(255, 255, 255, 180)
                ),
                # Medium zooms: normal thickness, normal halo
                TileStyle(
                    zoom_range=(7, 9),
                    line_width=1.6,
                    halo_width=0.6,  # Normal halo
                    line_color=(*yellow_rgb, 255),
                    halo_color=(255, 255, 255, 200)
                ),
                # High zooms: thicker lines, full halo
                TileStyle(
                    zoom_range=(10, 11),
                    line_width=2.0,
                    halo_width=0.8,  # Full halo
                    line_color=(*yellow_rgb, 255),
                    halo_color=(255, 255, 255, 200)
                )
            ]
    
    def get_style_for_zoom(self, zoom: int) -> Optional[TileStyle]:
        """Get style configuration for given zoom level"""
        for style in self.styles:
            if style.applies_to_zoom(zoom):
                return style
        return None


class WebMercatorTileCalculator:
    """Utilities for Web Mercator tile calculations"""
    
    # Web Mercator bounds
    WM_ORIGIN_SHIFT = 20037508.342789244
    WM_SIZE = WM_ORIGIN_SHIFT * 2
    
    @staticmethod
    def deg2num(lat_deg: float, lon_deg: float, zoom: int) -> Tuple[int, int]:
        """Convert lat/lon to tile numbers"""
        lat_rad = math.radians(lat_deg)
        n = 2.0 ** zoom
        x_tile = int((lon_deg + 180.0) / 360.0 * n)
        y_tile = int((1.0 - math.asinh(math.tan(lat_rad)) / math.pi) / 2.0 * n)
        return x_tile, y_tile
    
    @staticmethod
    def num2deg(x_tile: int, y_tile: int, zoom: int) -> Tuple[float, float]:
        """Convert tile numbers to lat/lon (northwest corner)"""
        n = 2.0 ** zoom
        lon_deg = x_tile / n * 360.0 - 180.0
        lat_rad = math.atan(math.sinh(math.pi * (1 - 2 * y_tile / n)))
        lat_deg = math.degrees(lat_rad)
        return lat_deg, lon_deg
    
    @staticmethod
    def tile_bbox_wgs84(x_tile: int, y_tile: int, zoom: int) -> Tuple[float, float, float, float]:
        """Get WGS84 bbox of tile (minx, miny, maxx, maxy)"""
        lat_nw, lon_nw = WebMercatorTileCalculator.num2deg(x_tile, y_tile, zoom)
        lat_se, lon_se = WebMercatorTileCalculator.num2deg(x_tile + 1, y_tile + 1, zoom)
        return lon_nw, lat_se, lon_se, lat_nw  # minx, miny, maxx, maxy
    
    @staticmethod
    def tile_bbox_web_mercator(x_tile: int, y_tile: int, zoom: int) -> Tuple[float, float, float, float]:
        """Get Web Mercator bbox of tile in meters"""
        n = 2.0 ** zoom
        unit = WebMercatorTileCalculator.WM_SIZE / n
        
        minx = -WebMercatorTileCalculator.WM_ORIGIN_SHIFT + x_tile * unit
        maxx = -WebMercatorTileCalculator.WM_ORIGIN_SHIFT + (x_tile + 1) * unit
        miny = WebMercatorTileCalculator.WM_ORIGIN_SHIFT - (y_tile + 1) * unit
        maxy = WebMercatorTileCalculator.WM_ORIGIN_SHIFT - y_tile * unit
        
        return minx, miny, maxx, maxy
    
    @staticmethod
    def enumerate_tiles_for_bounds(minx: float, miny: float, maxx: float, maxy: float, 
                                 zoom: int) -> Generator[Tuple[int, int], None, None]:
        """Enumerate all tiles that intersect given WGS84 bounds"""
        x_min, y_max = WebMercatorTileCalculator.deg2num(miny, minx, zoom)
        x_max, y_min = WebMercatorTileCalculator.deg2num(maxy, maxx, zoom)
        
        # Ensure valid tile ranges
        n = 2 ** zoom
        x_min = max(0, min(x_min, n - 1))
        x_max = max(0, min(x_max, n - 1))
        y_min = max(0, min(y_min, n - 1))
        y_max = max(0, min(y_max, n - 1))
        
        for x in range(x_min, x_max + 1):
            for y in range(y_min, y_max + 1):
                yield x, y


class GeometryRenderer:
    """Renders geometries to PIL Images with antialiasing and styles"""
    
    def __init__(self, tile_size: int = TILE_SIZE):
        self.tile_size = tile_size
    
    def create_empty_tile(self) -> Image.Image:
        """Create transparent empty tile"""
        return Image.new('RGBA', (self.tile_size, self.tile_size), (0, 0, 0, 0))
    
    def web_mercator_to_pixel(self, x_meters: float, y_meters: float, 
                            bbox_meters: Tuple[float, float, float, float]) -> Tuple[float, float]:
        """Convert Web Mercator coordinates to pixel coordinates"""
        minx, miny, maxx, maxy = bbox_meters
        
        # Scale to 0-1
        px_norm = (x_meters - minx) / (maxx - minx)
        py_norm = 1.0 - (y_meters - miny) / (maxy - miny)  # Flip Y axis
        
        # Scale to pixel coordinates
        px = px_norm * self.tile_size
        py = py_norm * self.tile_size
        
        return px, py
    
    def linestring_to_pixel_coords(self, linestring: LineString, 
                                 bbox_meters: Tuple[float, float, float, float]) -> List[Tuple[float, float]]:
        """Convert LineString coordinates to pixel coordinates"""
        coords = []
        for x, y in linestring.coords:
            px, py = self.web_mercator_to_pixel(x, y, bbox_meters)
            coords.append((px, py))
        return coords
    
    def render_linestrings(self, linestrings: List[LineString], 
                          bbox_meters: Tuple[float, float, float, float],
                          style: TileStyle) -> Image.Image:
        """Render LineStrings to PIL Image with given style"""
        # Create image with higher resolution for antialiasing
        aa_factor = 2  # 2x supersampling
        aa_size = self.tile_size * aa_factor
        
        # Create high-res image
        img_aa = Image.new('RGBA', (aa_size, aa_size), (0, 0, 0, 0))
        draw = ImageDraw.Draw(img_aa)
        
        # Scale style parameters for high-res rendering
        line_width_aa = style.line_width * aa_factor
        halo_width_aa = style.halo_width * aa_factor
        
        # Render each LineString
        for linestring in linestrings:
            try:
                pixel_coords = self.linestring_to_pixel_coords(linestring, bbox_meters)
                
                # Scale coordinates for high-res rendering
                pixel_coords_aa = [(x * aa_factor, y * aa_factor) for x, y in pixel_coords]
                
                if len(pixel_coords_aa) < 2:
                    continue
                
                # Draw halo first (if enabled)
                if halo_width_aa > 0:
                    total_width = line_width_aa + 2 * halo_width_aa
                    draw.line(pixel_coords_aa, fill=style.halo_color, width=int(total_width), 
                             joint='round')
                
                # Draw main line
                draw.line(pixel_coords_aa, fill=style.line_color, width=int(line_width_aa), 
                         joint='round')
                
            except Exception as e:
                # Skip problematic geometries
                continue
        
        # Downsample for antialiasing
        img_final = img_aa.resize((self.tile_size, self.tile_size), Image.LANCZOS)
        
        return img_final
    
    def optimize_png(self, img: Image.Image) -> bytes:
        """Optimize PNG using palette compression"""
        # Convert to palette mode if possible (for smaller file size)
        try:
            # Only convert if image has limited colors
            img_palette = img.convert('P', palette=Image.ADAPTIVE, colors=256)
            # Ensure transparency is preserved
            img_palette = img_palette.convert('RGBA')
        except:
            img_palette = img
        
        # Save to bytes
        buffer = io.BytesIO()
        img_palette.save(buffer, format='PNG', optimize=True)
        return buffer.getvalue()


class JA360TileGenerator:
    """
    Main class for generating raster tiles from JA360 coverage data.
    
    Supports both detailed segments and complete roads, with zoom-based layer selection.
    Generates PNG tiles with transparency, antialiasing, and configurable styles.
    """
    
    def __init__(self, config: TileConfig = None):
        self.config = config or TileConfig()
        self.logger = self._setup_logging()
        
        # Data containers
        self.roads_gdf = None
        
        # Spatial indices
        self.roads_tree = None
        
        # CRS transformers
        self.transformer_to_wm = None  # WGS84 → Web Mercator
        self.transformer_to_wgs84 = None  # Web Mercator → WGS84
        
        # Renderer
        self.renderer = GeometryRenderer(self.config.tile_size)
        
        # Statistics
        self.stats = {
            'tiles_generated': 0,
            'tiles_empty': 0,
            'tiles_with_data': 0,
            'total_geometries_processed': 0
        }
        
        self.logger.info("🗺️  JA360 Raster Tile Generator initialized")
        self.logger.info(f"📐 Zoom range: {self.config.min_zoom}-{self.config.max_zoom}")
        self.logger.info(f"🖼️  Tile size: {self.config.tile_size}px")
        self.logger.info(f"🎨 Style layers: {len(self.config.styles)} zoom ranges")
    
    def _setup_logging(self) -> logging.Logger:
        """Setup logging configuration"""
        logger = logging.getLogger('JA360TileGenerator')
        logger.setLevel(logging.INFO)
        
        # Clear existing handlers
        logger.handlers.clear()
        
        # Console handler
        console_handler = logging.StreamHandler()
        console_handler.setLevel(logging.INFO)
        formatter = logging.Formatter('%(message)s')
        console_handler.setFormatter(formatter)
        logger.addHandler(console_handler)
        
        return logger
    
    def _setup_crs_transformers(self):
        """Setup CRS transformers"""
        self.transformer_to_wm = Transformer.from_crs(
            WGS84_CRS, WEB_MERCATOR_CRS, always_xy=True
        )
        self.transformer_to_wgs84 = Transformer.from_crs(
            WEB_MERCATOR_CRS, WGS84_CRS, always_xy=True
        )
        self.logger.info(f"🌐 CRS transformers ready: {WGS84_CRS} ↔ {WEB_MERCATOR_CRS}")
    
    def load_coverage_data(self, roads_file: Union[str, Path]):
        """
        Load coverage roads data
        
        Args:
            roads_file: Path to covered roads GeoJSON
        """
        self.logger.info("📂 Loading coverage data...")
        start_time = time.time()
        
        # Load roads
        roads_path = Path(roads_file)
        if roads_path.exists():
            self.roads_gdf = gpd.read_file(roads_path)
            self.logger.info(f"🛣️  Loaded {len(self.roads_gdf):,} covered roads")
            
            # Ensure Web Mercator CRS
            if self.roads_gdf.crs != WEB_MERCATOR_CRS:
                self.roads_gdf = self.roads_gdf.to_crs(WEB_MERCATOR_CRS)
        else:
            self.logger.error(f"❌ Roads file not found: {roads_path}")
            raise FileNotFoundError(f"Roads file required: {roads_path}")
        
        elapsed = time.time() - start_time
        self.logger.info(f"✅ Coverage data loaded in {elapsed:.1f}s")
    
    def build_spatial_indices(self):
        """Build spatial indices for efficient geometry selection"""
        self.logger.info("🗂️  Building spatial indices...")
        start_time = time.time()
        
        # Build roads index only
        if len(self.roads_gdf) > 0:
            self.roads_tree = STRtree(self.roads_gdf.geometry.values)
            self.logger.info(f"🛣️  Roads spatial index: {len(self.roads_gdf):,} geometries")
        else:
            self.logger.error("❌ No roads data available for indexing")
            raise ValueError("Roads data required for tile generation")
        
        elapsed = time.time() - start_time
        self.logger.info(f"✅ Spatial indices built in {elapsed:.1f}s")
    
    def get_data_bounds_wgs84(self) -> Tuple[float, float, float, float]:
        """Get bounds of roads data in WGS84"""
        # Only use roads bounds
        if len(self.roads_gdf) > 0:
            roads_wgs84 = self.roads_gdf.to_crs(WGS84_CRS)
            bounds = roads_wgs84.total_bounds
            return bounds[0], bounds[1], bounds[2], bounds[3]  # minx, miny, maxx, maxy
        else:
            # Default to Iceland if no data
            return -24.5, 63.4, -13.5, 66.5  # minx, miny, maxx, maxy
    
    def select_geometries_for_tile(self, x_tile: int, y_tile: int, zoom: int) -> List[LineString]:
        """
        Select geometries that intersect with given tile
        
        Args:
            x_tile, y_tile: Tile coordinates
            zoom: Zoom level
            
        Returns:
            List of LineString geometries in Web Mercator coordinates
        """
        # Get tile bbox in WGS84 with buffer
        minx_wgs84, miny_wgs84, maxx_wgs84, maxy_wgs84 = WebMercatorTileCalculator.tile_bbox_wgs84(
            x_tile, y_tile, zoom
        )
        
        # Add buffer to avoid edge cuts
        buffer = self.config.buffer_degrees
        minx_buffered = minx_wgs84 - buffer
        miny_buffered = miny_wgs84 - buffer
        maxx_buffered = maxx_wgs84 + buffer
        maxy_buffered = maxy_wgs84 + buffer
        
        # Create query polygon in Web Mercator
        query_polygon_wgs84 = box(minx_buffered, miny_buffered, maxx_buffered, maxy_buffered)
        
        # Transform to Web Mercator
        def transform_to_wm(x, y, z=None):
            return self.transformer_to_wm.transform(x, y)
        
        query_polygon_wm = transform(transform_to_wm, query_polygon_wgs84)
        
        # Always use roads data
        if self.roads_tree is not None:
            tree = self.roads_tree
            gdf = self.roads_gdf
        else:
            # No data available
            return []
        
        # Query spatial index
        try:
            candidate_indices = tree.query(query_polygon_wm)
            
            # Filter candidates that actually intersect
            intersecting_geometries = []
            for idx in candidate_indices:
                geom = gdf.iloc[idx].geometry
                if geom.intersects(query_polygon_wm):
                    # Clip geometry to query area for cleaner rendering
                    try:
                        clipped = geom.intersection(query_polygon_wm)
                        if not clipped.is_empty:
                            if isinstance(clipped, LineString):
                                intersecting_geometries.append(clipped)
                            elif isinstance(clipped, MultiLineString):
                                for line in clipped.geoms:
                                    if isinstance(line, LineString):
                                        intersecting_geometries.append(line)
                    except:
                        # If clipping fails, use original geometry
                        if isinstance(geom, LineString):
                            intersecting_geometries.append(geom)
                        elif isinstance(geom, MultiLineString):
                            for line in geom.geoms:
                                if isinstance(line, LineString):
                                    intersecting_geometries.append(line)
            
            return intersecting_geometries
        
        except Exception as e:
            self.logger.warning(f"⚠️  Error selecting geometries for tile {zoom}/{x_tile}/{y_tile}: {e}")
            return []
    
    def generate_tile(self, x_tile: int, y_tile: int, zoom: int) -> Optional[bytes]:
        """
        Generate a single PNG tile
        
        Args:
            x_tile, y_tile: Tile coordinates
            zoom: Zoom level
            
        Returns:
            PNG bytes or None if tile is empty and empty tiles are disabled
        """
        # Get style for this zoom level
        style = self.config.get_style_for_zoom(zoom)
        if style is None:
            self.logger.warning(f"⚠️  No style defined for zoom {zoom}")
            return None
        
        # Select geometries for this tile
        geometries = self.select_geometries_for_tile(x_tile, y_tile, zoom)
        
        # Check if tile has data
        has_data = len(geometries) > 0
        
        if not has_data and not self.config.generate_empty_tiles:
            return None  # Skip empty tiles
        
        # Get tile bbox in Web Mercator for rendering
        bbox_meters = WebMercatorTileCalculator.tile_bbox_web_mercator(x_tile, y_tile, zoom)
        
        # Render tile
        if has_data:
            img = self.renderer.render_linestrings(geometries, bbox_meters, style)
            self.stats['tiles_with_data'] += 1
            self.stats['total_geometries_processed'] += len(geometries)
        else:
            img = self.renderer.create_empty_tile()
            self.stats['tiles_empty'] += 1
        
        # Convert to PNG bytes
        if self.config.optimize_png:
            png_bytes = self.renderer.optimize_png(img)
        else:
            buffer = io.BytesIO()
            img.save(buffer, format='PNG')
            png_bytes = buffer.getvalue()
        
        self.stats['tiles_generated'] += 1
        return png_bytes
    
    def enumerate_all_tiles(self) -> Generator[Tuple[int, int, int], None, None]:
        """Enumerate all tiles to be generated"""
        # Get data bounds
        minx, miny, maxx, maxy = self.get_data_bounds_wgs84()
        
        self.logger.info(f"📊 Data bounds: ({miny:.4f}, {minx:.4f}) → ({maxy:.4f}, {maxx:.4f})")
        
        total_tiles = 0
        for zoom in range(self.config.min_zoom, self.config.max_zoom + 1):
            zoom_tiles = 0
            for x_tile, y_tile in WebMercatorTileCalculator.enumerate_tiles_for_bounds(
                minx, miny, maxx, maxy, zoom
            ):
                yield x_tile, y_tile, zoom
                zoom_tiles += 1
                total_tiles += 1
            
            self.logger.info(f"📏 Zoom {zoom}: {zoom_tiles:,} tiles")
        
        self.logger.info(f"📊 Total tiles to generate: {total_tiles:,}")
    
    def generate_all_tiles_static(self, output_dir: Union[str, Path]) -> Dict:
        """
        Generate all tiles as static z/x/y.png files
        
        Args:
            output_dir: Output directory for tiles
            
        Returns:
            Generation statistics
        """
        output_path = Path(output_dir)
        self.logger.info(f"🚀 Generating static tiles to {output_path}")
        
        start_time = time.time()
        
        # Reset statistics
        self.stats = {
            'tiles_generated': 0,
            'tiles_empty': 0,
            'tiles_with_data': 0,
            'total_geometries_processed': 0
        }
        
        # Generate all tiles
        batch_size = 1000  # Progress reporting interval
        tiles_processed = 0
        
        for x_tile, y_tile, zoom in self.enumerate_all_tiles():
            # Create zoom directory
            zoom_dir = output_path / str(zoom) / str(x_tile)
            zoom_dir.mkdir(parents=True, exist_ok=True)
            
            # Generate tile
            png_bytes = self.generate_tile(x_tile, y_tile, zoom)
            
            if png_bytes is not None:
                # Save tile
                tile_path = zoom_dir / f"{y_tile}.png"
                with open(tile_path, 'wb') as f:
                    f.write(png_bytes)
            
            tiles_processed += 1
            
            # Progress report
            if tiles_processed % batch_size == 0:
                elapsed = time.time() - start_time
                rate = tiles_processed / elapsed if elapsed > 0 else 0
                self.logger.info(f"📊 {tiles_processed:,} tiles processed ({rate:.1f} tiles/s)")
        
        elapsed = time.time() - start_time
        self.logger.info(f"✅ Static tile generation completed in {elapsed:.1f}s")
        
        # Final statistics
        self.stats['generation_time_seconds'] = elapsed
        self.stats['tiles_per_second'] = tiles_processed / elapsed if elapsed > 0 else 0
        
        return self.stats
    
    def generate_mbtiles(self, output_file: Union[str, Path]) -> Dict:
        """
        Generate MBTiles file for PMTiles conversion
        
        Args:
            output_file: Output MBTiles file path
            
        Returns:
            Generation statistics
        """
        output_path = Path(output_file)
        self.logger.info(f"🗃️  Generating MBTiles to {output_path}")
        
        start_time = time.time()
        
        # Create MBTiles database
        if output_path.exists():
            output_path.unlink()
        
        conn = sqlite3.connect(str(output_path))
        cursor = conn.cursor()
        
        # Create MBTiles schema
        cursor.execute('''
            CREATE TABLE metadata (name text, value text);
        ''')
        
        cursor.execute('''
            CREATE TABLE tiles (zoom_level integer, tile_column integer, tile_row integer, tile_data blob);
        ''')
        
        cursor.execute('''
            CREATE UNIQUE INDEX tile_index on tiles (zoom_level, tile_column, tile_row);
        ''')
        
        # Add metadata
        bounds = self.get_data_bounds_wgs84()
        metadata = {
            'name': 'JA360 Coverage',
            'type': 'overlay',
            'version': '1.0',
            'description': 'JA360 street coverage data raster tiles',
            'format': 'png',
            'bounds': f"{bounds[0]},{bounds[1]},{bounds[2]},{bounds[3]}",
            'minzoom': str(self.config.min_zoom),
            'maxzoom': str(self.config.max_zoom),
            'center': f"{(bounds[0]+bounds[2])/2},{(bounds[1]+bounds[3])/2},{self.config.max_zoom}"
        }
        
        for name, value in metadata.items():
            cursor.execute('INSERT INTO metadata (name, value) VALUES (?, ?)', (name, value))
        
        # Reset statistics
        self.stats = {
            'tiles_generated': 0,
            'tiles_empty': 0,
            'tiles_with_data': 0,
            'total_geometries_processed': 0
        }
        
        # Generate and insert tiles
        batch_size = 1000
        tiles_processed = 0
        batch_data = []
        
        for x_tile, y_tile, zoom in self.enumerate_all_tiles():
            # Generate tile
            png_bytes = self.generate_tile(x_tile, y_tile, zoom)
            
            if png_bytes is not None:
                # MBTiles uses TMS Y coordinate system (flip Y)
                y_tms = (2 ** zoom - 1) - y_tile
                batch_data.append((zoom, x_tile, y_tms, png_bytes))
            
            tiles_processed += 1
            
            # Insert batch
            if len(batch_data) >= batch_size:
                cursor.executemany(
                    'INSERT INTO tiles (zoom_level, tile_column, tile_row, tile_data) VALUES (?, ?, ?, ?)',
                    batch_data
                )
                conn.commit()
                batch_data = []
                
                elapsed = time.time() - start_time
                rate = tiles_processed / elapsed if elapsed > 0 else 0
                self.logger.info(f"📊 {tiles_processed:,} tiles processed ({rate:.1f} tiles/s)")
        
        # Insert remaining tiles
        if batch_data:
            cursor.executemany(
                'INSERT INTO tiles (zoom_level, tile_column, tile_row, tile_data) VALUES (?, ?, ?, ?)',
                batch_data
            )
            conn.commit()
        
        conn.close()
        
        elapsed = time.time() - start_time
        file_size_mb = output_path.stat().st_size / (1024 * 1024)
        
        self.logger.info(f"✅ MBTiles generation completed in {elapsed:.1f}s")
        self.logger.info(f"📄 File size: {file_size_mb:.1f} MB")
        
        # Final statistics
        self.stats['generation_time_seconds'] = elapsed
        self.stats['tiles_per_second'] = tiles_processed / elapsed if elapsed > 0 else 0
        self.stats['file_size_mb'] = file_size_mb
        
        return self.stats
    
    def process_static(self, segments_file: Union[str, Path], roads_file: Union[str, Path], 
                      output_dir: Union[str, Path]) -> Dict:
        """
        Complete pipeline for static tile generation
        
        Args:
            segments_file: Path to coverage segments GeoJSON
            roads_file: Path to covered roads GeoJSON 
            output_dir: Output directory for tiles
            
        Returns:
            Generation statistics
        """
        total_start = time.time()
        
        try:
            # Setup
            self._setup_crs_transformers()
            
            # Load data
            self.load_coverage_data(segments_file, roads_file)
            
            # Build spatial indices
            self.build_spatial_indices()
            
            # Generate tiles
            stats = self.generate_all_tiles_static(output_dir)
            
            # Summary
            total_elapsed = time.time() - total_start
            self.logger.info(f"\n🎉 Processing completed in {total_elapsed:.1f}s!")
            self.logger.info(f"📊 Final statistics:")
            self.logger.info(f"   • {stats['tiles_generated']:,} tiles generated")
            self.logger.info(f"   • {stats['tiles_with_data']:,} tiles with data")
            self.logger.info(f"   • {stats['tiles_empty']:,} empty tiles")
            self.logger.info(f"   • {stats['total_geometries_processed']:,} geometries processed")
            self.logger.info(f"   • {stats['tiles_per_second']:.1f} tiles/second")
            
            return stats
            
        except Exception as e:
            self.logger.error(f"❌ Processing failed: {e}")
            raise
    
    def process_mbtiles(self, roads_file: Union[str, Path], 
                       output_file: Union[str, Path]) -> Dict:
        """
        Complete pipeline for MBTiles generation
        
        Args:
            roads_file: Path to covered roads GeoJSON
            output_file: Output MBTiles file path
            
        Returns:
            Generation statistics
        """
        total_start = time.time()
        
        try:
            # Setup
            self._setup_crs_transformers()
            
            # Load data
            self.load_coverage_data(roads_file)
            
            # Build spatial indices
            self.build_spatial_indices()
            
            # Generate MBTiles
            stats = self.generate_mbtiles(output_file)
            
            # Summary
            total_elapsed = time.time() - total_start
            self.logger.info(f"\n🎉 Processing completed in {total_elapsed:.1f}s!")
            self.logger.info(f"📊 Final statistics:")
            self.logger.info(f"   • {stats['tiles_generated']:,} tiles generated")
            self.logger.info(f"   • {stats['tiles_with_data']:,} tiles with data")
            self.logger.info(f"   • {stats['tiles_empty']:,} empty tiles")
            self.logger.info(f"   • {stats['total_geometries_processed']:,} geometries processed")
            self.logger.info(f"   • {stats['tiles_per_second']:.1f} tiles/second")
            self.logger.info(f"   • {stats['file_size_mb']:.1f} MB file size")
            
            return stats
            
        except Exception as e:
            self.logger.error(f"❌ Processing failed: {e}")
            raise


def main():
    """Main entry point with default parameters"""
    print("🇮🇸 JA360 Raster Tile Generator")
    print("=" * 50)
    
    # Default paths
    base_dir = Path(__file__).parent.parent.parent / "data" / "ja360"
    usage_dir = base_dir / "usage"
    output_dir = base_dir / "tile"
    
    # Input file
    roads_file = usage_dir / "roads.geojson"
    mbtiles_output = output_dir / "ja360_coverage.mbtiles"
    
    print(f"📂 Input directory: {usage_dir}")
    print(f"📂 Output directory: {output_dir}")
    print(f"🔍 Zoom range: 1-11")
    print(f"📐 Tile size: 512px")
    
    # Check input file
    if not roads_file.exists():
        print(f"\n❌ Roads file not found: {roads_file}")
        print("💡 Run ja_coverage.py first to generate coverage data")
        sys.exit(1)
    
    # Create output directory
    output_dir.mkdir(parents=True, exist_ok=True)
    
    # Default configuration (zoom 1-18)
    config = TileConfig(
        min_zoom=1,
        max_zoom=11,
        tile_size=512,
        generate_empty_tiles=True,
        optimize_png=True
    )
    
    # Create generator
    generator = JA360TileGenerator(config)
    
    try:
        # Generate MBTiles by default
        print(f"\n🗃️  Generating MBTiles...")
        stats = generator.process_mbtiles(roads_file, mbtiles_output)
        
        print(f"\n✅ MBTiles generation completed!")
        print(f"📄 MBTiles file: {mbtiles_output}")
        print(f"📊 File size: {stats['file_size_mb']:.1f} MB")
        print(f"🚀 Performance: {stats['tiles_per_second']:.1f} tiles/second")
        print(f"📊 Statistics:")
        print(f"   • {stats['tiles_generated']:,} tiles generated")
        print(f"   • {stats['tiles_with_data']:,} tiles with data")
        print(f"   • {stats['tiles_empty']:,} empty tiles")
        print(f"   • {stats['total_geometries_processed']:,} geometries processed")
        
        # Convert MBTiles to PMTiles
        print(f"\n🔄 Converting MBTiles to PMTiles...")
        pmtiles_output = output_dir / "ja360_coverage.pmtiles"
        
        try:
            import subprocess
            import shutil
            
            # Check if pmtiles is available
            pmtiles_cmd = shutil.which('pmtiles')
            if not pmtiles_cmd:
                print(f"⚠️  pmtiles command not found. Installing...")
                print(f"💡 Please install pmtiles: npm install -g pmtiles")
                print(f"💡 Then run: pmtiles convert {mbtiles_output} {pmtiles_output}")
                return
            
            # Convert MBTiles to PMTiles
            result = subprocess.run([
                'pmtiles', 'convert', 
                str(mbtiles_output), 
                str(pmtiles_output)
            ], capture_output=True, text=True, check=True)
            
            # Get PMTiles file size
            pmtiles_size_mb = pmtiles_output.stat().st_size / (1024 * 1024)
            compression_ratio = (1 - pmtiles_size_mb / stats['file_size_mb']) * 100
            
            print(f"✅ PMTiles conversion completed!")
            print(f"📄 PMTiles file: {pmtiles_output}")
            print(f"📊 PMTiles size: {pmtiles_size_mb:.1f} MB")
            print(f"📉 Compression: {compression_ratio:.1f}% smaller than MBTiles")
            
            # Clean up MBTiles file (optional)
            print(f"\n🧹 Cleaning up...")
            mbtiles_output.unlink()
            print(f"🗑️  Removed MBTiles file: {mbtiles_output}")
            
        except subprocess.CalledProcessError as e:
            print(f"❌ PMTiles conversion failed: {e}")
            print(f"💡 Please install pmtiles: npm install -g pmtiles")
            print(f"💡 Then run: pmtiles convert {mbtiles_output} {pmtiles_output}")
        except FileNotFoundError:
            print(f"❌ pmtiles command not found")
            print(f"💡 Please install pmtiles: npm install -g pmtiles")
            print(f"💡 Then run: pmtiles convert {mbtiles_output} {pmtiles_output}")
        except Exception as e:
            print(f"❌ Error during PMTiles conversion: {e}")
            print(f"💡 MBTiles file available: {mbtiles_output}")
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

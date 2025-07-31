#!/usr/bin/env python3
"""
Naver Maps PBF to GeoParquet Processor - DuckDB Optimized

This script processes raw PBF tiles from Naver Maps data collection using DuckDB
for maximum performance. It applies Y-axis mirroring correction and consolidates
all tiles into a single GeoParquet file.

Performance optimizations:
- Pure DuckDB operations for data processing
- Spatial extension for geometric operations
- Parallel processing with optimal batching
- Memory-efficient streaming operations
- Zero-copy data transfers where possible

Input:  data/naver/raw/{session}/z/x/y.pbf (tile data)
Output: data/naver/processed/naver_panoramas.parquet (consolidated GeoParquet)

Usage:
    python naver_processor_duckdb.py [--workers N] [--batch-size N] [--base-dir PATH]

Author: Data Collection Team
Date: 2024
"""

import os
import sys
import json
import logging
import argparse
import time
import math
import psutil
import multiprocessing
from pathlib import Path
from typing import List, Dict, Any, Tuple, Optional
from datetime import datetime
from dataclasses import dataclass, asdict
from concurrent.futures import ThreadPoolExecutor, ProcessPoolExecutor, as_completed
import threading
import tempfile

# DuckDB for high-performance data processing
import duckdb

# Minimal external dependencies for PBF decoding only
import mapbox_vector_tile
# Shapely removed - direct coordinate transformation for better performance


@dataclass
class SystemResources:
    """System resources configuration for optimal DuckDB processing."""
    total_memory_gb: float
    available_memory_gb: float
    duckdb_memory_gb: float
    total_cpu_cores: int
    duckdb_threads: int
    
    @classmethod
    def detect(cls, memory_ratio: float = 0.7, thread_ratio: float = 0.8) -> 'SystemResources':
        """
        Detect system resources and optimize for DuckDB.
        
        Args:
            memory_ratio: Ratio of available memory for DuckDB (default: 0.7)
            thread_ratio: Ratio of CPU cores for DuckDB (default: 0.8)
            
        Returns:
            SystemResources optimized for DuckDB
        """
        memory = psutil.virtual_memory()
        total_memory_gb = memory.total / (1024**3)
        available_memory_gb = memory.available / (1024**3)
        duckdb_memory_gb = available_memory_gb * memory_ratio
        
        total_cpu_cores = multiprocessing.cpu_count()
        duckdb_threads = max(1, int(total_cpu_cores * thread_ratio))
        
        return cls(
            total_memory_gb=total_memory_gb,
            available_memory_gb=available_memory_gb,
            duckdb_memory_gb=duckdb_memory_gb,
            total_cpu_cores=total_cpu_cores,
            duckdb_threads=duckdb_threads
        )
    
    def get_duckdb_config(self) -> Dict[str, Any]:
        """Get DuckDB configuration dictionary."""
        return {
            'memory_limit': f'{self.duckdb_memory_gb:.1f}GB',
            'threads': self.duckdb_threads,
            'enable_external_access': True,
            'temp_directory': '/tmp/duckdb_naver'
        }
    
    def configure_duckdb_runtime(self, conn: duckdb.DuckDBPyConnection):
        """Configure DuckDB runtime settings (after connection is established)."""
        try:
            # Memory configuration (can be changed at runtime)
            conn.execute(f"SET memory_limit='{self.duckdb_memory_gb:.1f}GB'")
            conn.execute(f"SET max_memory='{self.duckdb_memory_gb:.1f}GB'")
            
            # Thread configuration
            conn.execute(f"SET threads={self.duckdb_threads}")
            
            # Performance optimizations that can be set at runtime
            conn.execute("SET enable_progress_bar=true")
            conn.execute("SET preserve_insertion_order=false")
            
            # Try to set temp directory (may fail on some systems)
            try:
                conn.execute("SET temp_directory='/tmp/duckdb_naver'")
            except:
                pass  # Ignore if temp directory can't be set
            
        except Exception as e:
            # Log warning but don't fail - use default settings
            pass
        
        return conn


@dataclass
class ProcessingStats:
    """Statistics for DuckDB processing operations."""
    total_files: int = 0
    processed_files: int = 0
    failed_files: int = 0
    total_features: int = 0
    layers_found: set = None
    start_time: float = 0
    end_time: float = 0
    duckdb_operations: int = 0
    
    def __post_init__(self):
        if self.layers_found is None:
            self.layers_found = set()


class DuckDBTileProcessor:
    """
    High-performance PBF processor using DuckDB for all data operations.
    
    This processor leverages DuckDB's columnar engine and spatial extension
    for maximum throughput when processing large numbers of tiles.
    """
    
    def __init__(self, base_dir: str = "../../data/naver", max_workers: Optional[int] = None):
        """
        Initialize the DuckDB-optimized processor.
        
        Args:
            base_dir: Base directory containing Naver data
            max_workers: Maximum worker processes for PBF decoding
        """
        self.base_dir = Path(base_dir)
        self.raw_dir = self.base_dir / "raw"
        self.processed_base_dir = self.base_dir / "processed"
        self.temp_dir = Path(tempfile.mkdtemp(prefix="naver_duckdb_"))
        
        # System optimization
        self.system_resources = SystemResources.detect()
        self.max_workers = max_workers or min(self.system_resources.duckdb_threads, 8)
        
        # Session-specific directories will be set up in discover_pbf_files
        self.processed_dir = None
        self.session_num = None
        
        # Create base directories
        self.processed_base_dir.mkdir(parents=True, exist_ok=True)
        self.temp_dir.mkdir(parents=True, exist_ok=True)
        
        # Setup logging FIRST (temporary location)
        self.logger = self._setup_logging()
        
        # Output files will be set after session discovery
        self.output_file = None
        self.stats_file = None
        
        # Initialize DuckDB with optimal configuration
        self.conn = duckdb.connect(config=self.system_resources.get_duckdb_config())
        self.system_resources.configure_duckdb_runtime(self.conn)
        self._setup_duckdb_extensions()
        
        # Processing state
        self.stats = ProcessingStats()
        self.lock = threading.Lock()
        
        self._log_initialization()
    
    def _setup_duckdb_extensions(self):
        """Setup required DuckDB extensions."""
        try:
            # Install and load spatial extension
            self.conn.execute("INSTALL spatial")
            self.conn.execute("LOAD spatial")
            
            # Try to set spatial optimizations
            try:
                self.conn.execute("SET spatial.sphere=WGS84")
            except:
                pass  # Ignore if not available
            
            # Install and load JSON extension for metadata handling
            self.conn.execute("INSTALL json")
            self.conn.execute("LOAD json")
            
            self.logger.info("✅ DuckDB extensions loaded: spatial, json")
            
        except Exception as e:
            self.logger.warning(f"⚠️  Could not load DuckDB extensions: {e}")
            # Continue without extensions - basic functionality will still work
    
    def _setup_logging(self) -> logging.Logger:
        """Setup high-performance logging."""
        logger = logging.getLogger('naver_duckdb_processor')
        logger.setLevel(logging.DEBUG)
        logger.handlers.clear()
        
        # File handler with buffering - use base processed dir initially
        log_dir = self.processed_dir if self.processed_dir else self.processed_base_dir
        log_file = log_dir / f"duckdb_processing_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log"
        file_handler = logging.FileHandler(log_file, encoding='utf-8')
        file_handler.setLevel(logging.DEBUG)
        file_formatter = logging.Formatter(
            '%(asctime)s - %(levelname)s - %(message)s',
            datefmt='%Y-%m-%d %H:%M:%S'
        )
        file_handler.setFormatter(file_formatter)
        logger.addHandler(file_handler)
        
        # Console handler
        console_handler = logging.StreamHandler()
        console_handler.setLevel(logging.INFO)
        console_formatter = logging.Formatter('%(message)s')
        console_handler.setFormatter(console_formatter)
        logger.addHandler(console_handler)
        
        return logger
    
    def _reconfigure_logging(self):
        """Reconfigure logging to use session-specific directory."""
        if not self.processed_dir or not hasattr(self, 'logger'):
            return
        
        # Remove old file handler if exists
        for handler in self.logger.handlers[:]:
            if isinstance(handler, logging.FileHandler):
                self.logger.removeHandler(handler)
                handler.close()
        
        # Add new file handler with session-specific directory
        log_file = self.processed_dir / f"duckdb_processing_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log"
        file_handler = logging.FileHandler(log_file, encoding='utf-8')
        file_handler.setLevel(logging.DEBUG)
        file_formatter = logging.Formatter(
            '%(asctime)s - %(levelname)s - %(message)s',
            datefmt='%Y-%m-%d %H:%M:%S'
        )
        file_handler.setFormatter(file_formatter)
        self.logger.addHandler(file_handler)
        
        self.logger.info(f"📝 Logging reconfigured for session {self.session_num}: {log_file}")
    
    def _log_initialization(self):
        """Log initialization information."""
        self.logger.info("🚀 DuckDB-Optimized Naver PBF Processor")
        self.logger.info(f"📁 Raw data: {self.raw_dir}")
        processed_info = f"{self.processed_dir}" if self.processed_dir else f"{self.processed_base_dir} (session-specific dir will be created)"
        self.logger.info(f"📁 Processed data: {processed_info}")
        self.logger.info(f"🔧 System Resources:")
        self.logger.info(f"   💾 Total Memory: {self.system_resources.total_memory_gb:.1f} GB")
        self.logger.info(f"   💾 DuckDB Memory: {self.system_resources.duckdb_memory_gb:.1f} GB")
        self.logger.info(f"   🧵 Total Cores: {self.system_resources.total_cpu_cores}")
        self.logger.info(f"   🧵 DuckDB Threads: {self.system_resources.duckdb_threads}")
        self.logger.info(f"   ⚡ Worker Processes: {self.max_workers}")
    
    def discover_pbf_files(self) -> List[Dict]:
        """
        Discover PBF files and create DuckDB table with file metadata.
        
        Returns:
            List of tile information for processing
        """
        self.logger.info("🔍 Discovering PBF files...")
        
        if not self.raw_dir.exists():
            raise FileNotFoundError(f"Raw data directory not found: {self.raw_dir}")
        
        # Find latest session
        session_dirs = [d for d in self.raw_dir.iterdir() if d.is_dir() and d.name.isdigit()]
        if not session_dirs:
            raise FileNotFoundError(f"No session directories found in {self.raw_dir}")
        
        latest_session_dir = max(session_dirs, key=lambda d: int(d.name))
        session_num = int(latest_session_dir.name)
        zoom_dir = latest_session_dir / "17"
        
        if not zoom_dir.exists():
            raise FileNotFoundError(f"Zoom 17 directory not found: {zoom_dir}")
        
        # Setup session-specific processed directory and output files
        self.session_num = session_num
        self.processed_dir = self.processed_base_dir / str(session_num)
        self.processed_dir.mkdir(parents=True, exist_ok=True)
        
        # Set output files for this session
        self.output_file = self.processed_dir / "naver_panoramas.parquet"
        self.stats_file = self.processed_dir / "processing_stats.json"
        
        # Reconfigure logging to use session-specific directory
        self._reconfigure_logging()
        
        self.logger.info(f"📂 Processing session {session_num}, zoom level 17")
        self.logger.info(f"📁 Scanning directory: {zoom_dir}")
        self.logger.info(f"📁 Session output directory: {self.processed_dir}")
        
        # Create DuckDB table for file inventory
        self.conn.execute("""
            CREATE OR REPLACE TABLE tile_files (
                file_path VARCHAR,
                session INTEGER,
                z INTEGER,
                x INTEGER,
                y INTEGER,
                file_size BIGINT,
                west DOUBLE,
                south DOUBLE,
                east DOUBLE,
                north DOUBLE
            )
        """)
        
        # Collect file information with progress logging
        tile_files = []
        total_size = 0
        processed_count = 0
        
        self.logger.info("📊 Scanning for PBF files...")
        
        # First pass: count total files for progress tracking
        all_pbf_files = list(zoom_dir.rglob("*.pbf"))
        total_files = len(all_pbf_files)
        
        self.logger.info(f"📈 Found {total_files:,} PBF files to process")
        
        if total_files == 0:
            raise FileNotFoundError(f"No PBF files found in {zoom_dir}")
        
        scan_start_time = time.time()
        
        self.logger.info(f"📊 Processing {total_files:,} tiles")
        
        for i, pbf_file in enumerate(all_pbf_files):
            try:
                parts = pbf_file.relative_to(zoom_dir).parts
                if len(parts) == 2:  # x/y.pbf
                    x = int(parts[0])
                    y = int(pbf_file.stem)
                    file_size = pbf_file.stat().st_size
                    total_size += file_size
                    
                    # Calculate geographic bounds
                    west, south, east, north = self._tile_to_bbox(x, y, 17)
                    
                    tile_info = {
                        'file_path': str(pbf_file),
                        'session': session_num,
                        'z': 17,
                        'x': x,
                        'y': y,
                        'file_size': file_size,
                        'west': west,
                        'south': south,
                        'east': east,
                        'north': north
                    }
                    tile_files.append(tile_info)
                    processed_count += 1
                    
                    # Progress logging every 100,000 files
                    if (i + 1) % 100000 == 0:
                        elapsed = time.time() - scan_start_time
                        rate = (i + 1) / elapsed if elapsed > 0 else 0
                        progress = (i + 1) / total_files * 100
                        self.logger.info(f"📊 Scan: {i + 1:,}/{total_files:,} ({progress:.1f}%) - {rate:.0f} files/sec")
                    
            except (ValueError, IndexError, OSError) as e:
                self.logger.debug(f"⚠️  Skipping invalid file {pbf_file}: {e}")
                continue
        
        scan_elapsed = time.time() - scan_start_time
        scan_rate = processed_count / scan_elapsed if scan_elapsed > 0 else 0
        
        if not tile_files:
            raise FileNotFoundError(f"No valid PBF files found in session {session_num}")
        
        self.logger.info(f"✅ Scan complete: {processed_count:,} valid files in {scan_elapsed:.1f}s ({scan_rate:.0f} files/sec)")
        self.logger.info(f"💾 Total data size: {total_size / (1024**2):.1f} MB")
        
        # Insert into DuckDB table for efficient querying (with progress)
        self.logger.info("📊 Building DuckDB file index...")
        
        batch_size = 50000  # Increased from 1000 to 50,000 for better performance
        index_start_time = time.time()
        
        for i in range(0, len(tile_files), batch_size):
            batch = tile_files[i:i + batch_size]
            
            self.conn.executemany("""
                INSERT INTO tile_files VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, [
                (tf['file_path'], tf['session'], tf['z'], tf['x'], tf['y'], 
                 tf['file_size'], tf['west'], tf['south'], tf['east'], tf['north'])
                for tf in batch
            ])
            
            # Progress logging every 50,000 indexed files (every batch)
            elapsed = time.time() - index_start_time
            current_indexed = min(i + batch_size, len(tile_files))
            rate = current_indexed / elapsed if elapsed > 0 else 0
            progress = current_indexed / len(tile_files) * 100
            self.logger.info(f"📊 Index: {current_indexed:,}/{len(tile_files):,} ({progress:.1f}%) - {rate:.0f} files/sec")
        
        index_elapsed = time.time() - index_start_time
        index_rate = len(tile_files) / index_elapsed if index_elapsed > 0 else 0
        
        self.logger.info(f"✅ Index complete: {len(tile_files):,} files indexed in {index_elapsed:.1f}s ({index_rate:.0f} files/sec)")
        
        # Get statistics using DuckDB
        self.logger.info("📊 Computing spatial statistics...")
        stats = self.conn.execute("""
            SELECT 
                COUNT(*) as total_files,
                SUM(file_size) as total_size,
                MIN(x) as min_x, MAX(x) as max_x,
                MIN(y) as min_y, MAX(y) as max_y,
                MIN(west) as min_west, MAX(east) as max_east,
                MIN(south) as min_south, MAX(north) as max_north
            FROM tile_files
        """).fetchone()
        
        self.logger.info(f"✅ File discovery complete!")
        self.logger.info(f"📊 Total files: {stats[0]:,} ({stats[1] / (1024**2):.1f} MB)")
        self.logger.info(f"🗺️  Tile range: X[{stats[2]}-{stats[3]}], Y[{stats[4]}-{stats[5]}]")
        self.logger.info(f"🌍 Geographic bounds: [{stats[6]:.4f}, {stats[8]:.4f}, {stats[7]:.4f}, {stats[9]:.4f}]")
        
        return tile_files
    
    def _tile_to_bbox(self, x: int, y: int, z: int) -> Tuple[float, float, float, float]:
        """Calculate geographic bounding box for a tile."""
        n = 2.0 ** z
        west = x / n * 360.0 - 180.0
        east = (x + 1) / n * 360.0 - 180.0
        
        north_rad = math.atan(math.sinh(math.pi * (1 - 2 * y / n)))
        north = math.degrees(north_rad)
        
        south_rad = math.atan(math.sinh(math.pi * (1 - 2 * (y + 1) / n)))
        south = math.degrees(south_rad)
        
        return west, south, east, north
    
    def setup_feature_tables(self):
        """Create DuckDB tables for storing processed features."""
        self.logger.info("🏗️  Setting up DuckDB feature tables...")
        
        # Main features table with spatial geometry
        self.conn.execute("""
            CREATE OR REPLACE TABLE features (
                feature_id BIGINT,
                layer VARCHAR,
                tile_x INTEGER,
                tile_y INTEGER,
                tile_z INTEGER,
                session INTEGER,
                geometry_type VARCHAR,
                geometry GEOMETRY,
                properties JSON,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)
        
        # Temporary table for batch inserts
        self.conn.execute("""
            CREATE OR REPLACE TABLE features_temp (
                feature_id BIGINT,
                layer VARCHAR,
                tile_x INTEGER,
                tile_y INTEGER,
                tile_z INTEGER,
                session INTEGER,
                geometry_type VARCHAR,
                geometry_wkt VARCHAR,
                properties JSON
            )
        """)
        
        self.logger.info("✅ Feature tables created")
    
    def process_pbf_batch(self, tile_batch: List[Dict]) -> int:
        """
        Process a batch of PBF files and insert features into DuckDB.
        
        Args:
            tile_batch: List of tile information dictionaries
            
        Returns:
            Number of features processed
        """
        batch_features = []
        feature_id = int(time.time() * 1000000)  # Microsecond timestamp for unique IDs
        
        # Process each tile in the batch
        for tile_info in tile_batch:
            try:
                features = self._process_single_pbf_file(tile_info, feature_id)
                batch_features.extend(features)
                feature_id += len(features)
                
                with self.lock:
                    self.stats.processed_files += 1
                    
            except Exception as e:
                self.logger.error(f"❌ Error processing {tile_info['file_path']}: {e}")
                with self.lock:
                    self.stats.failed_files += 1
                continue
        
        # Bulk insert into DuckDB using efficient batch operations
        if batch_features:
            self._bulk_insert_features(batch_features)
        
        return len(batch_features)
    
    def _process_single_pbf_file(self, tile_info: Dict, start_feature_id: int) -> List[Dict]:
        """
        Process a single PBF file and extract features.
        
        Args:
            tile_info: Tile information dictionary
            start_feature_id: Starting feature ID for this tile
            
        Returns:
            List of feature dictionaries
        """
        features = []
        feature_id = start_feature_id
        
        try:
            # Read and decode PBF
            with open(tile_info['file_path'], 'rb') as f:
                pbf_data = f.read()
            
            if not pbf_data:
                return features
            
            decoded_tile = mapbox_vector_tile.decode(pbf_data)
            
            # Process each layer
            for layer_name, layer_data in decoded_tile.items():
                with self.lock:
                    self.stats.layers_found.add(layer_name)
                
                layer_features = layer_data.get('features', [])
                
                for feature in layer_features:
                    try:
                        processed_feature = self._process_mvt_feature(
                            feature, layer_name, tile_info, feature_id
                        )
                        if processed_feature:
                            features.append(processed_feature)
                            feature_id += 1
                    except Exception as e:
                        self.logger.debug(f"⚠️  Skipping feature in {tile_info['file_path']}: {e}")
                        continue
            
            return features
            
        except Exception as e:
            self.logger.error(f"❌ Error decoding {tile_info['file_path']}: {e}")
            return features
    
    def _process_mvt_feature(self, feature: Dict, layer_name: str, 
                            tile_info: Dict, feature_id: int) -> Optional[Dict]:
        """
        Process a single MVT feature into DuckDB-compatible format.
        
        Args:
            feature: MVT feature data
            layer_name: Layer name
            tile_info: Tile information
            feature_id: Unique feature ID
            
        Returns:
            Processed feature dictionary or None
        """
        try:
            geometry_data = feature.get('geometry', {})
            properties = feature.get('properties', {})
            
            # Convert MVT geometry directly to geographic WKT (no Shapely needed)
            geographic_wkt = self._mvt_to_geographic_wkt(
                geometry_data, tile_info['x'], tile_info['y'], tile_info['z']
            )
            if geographic_wkt is None:
                return None
            
            # Extract geometry type from WKT
            geometry_type = geographic_wkt.split('(')[0].strip()
            
            return {
                'feature_id': feature_id,
                'layer': layer_name,
                'tile_x': tile_info['x'],
                'tile_y': tile_info['y'],
                'tile_z': tile_info['z'],
                'session': tile_info['session'],
                'geometry_type': geometry_type,
                'geometry_wkt': geographic_wkt,
                'properties': json.dumps(properties) if properties else '{}'
            }
            
        except Exception as e:
            self.logger.debug(f"Error processing feature: {e}")
            return None
    
    def _mvt_to_geographic_wkt(self, geometry_data: Dict, x: int, y: int, z: int, tile_extent: int = 4096) -> Optional[str]:
        """
        Convert MVT geometry directly to geographic WKT for maximum performance.
        No Shapely dependency - direct coordinate transformation.
        
        Args:
            geometry_data: MVT geometry data
            x, y, z: Tile coordinates  
            tile_extent: Tile extent (default: 4096)
            
        Returns:
            WGT string or None if invalid
        """
        geom_type = geometry_data.get('type', '')
        coordinates = geometry_data.get('coordinates', [])
        
        if not coordinates:
            return None
        
        # Calculate transformation parameters once
        west, south, east, north = self._tile_to_bbox(x, y, z)
        x_scale = (east - west) / tile_extent
        y_scale = (north - south) / tile_extent
        
        def transform_coord_pair(coord_pair):
            """Transform a single coordinate pair [x, y] to geographic."""
            tile_x, tile_y = coord_pair
            lon = west + (tile_x * x_scale)
            lat = south + (tile_y * y_scale)
            return f"{lon} {lat}"
        
        def transform_coords(coords):
            """Transform coordinate array."""
            return [transform_coord_pair(coord) for coord in coords]
        
        try:
            if geom_type == 'Point':
                coord_str = transform_coord_pair(coordinates)
                return f"POINT({coord_str})"
                
            elif geom_type == 'LineString':
                coords_str = ', '.join(transform_coords(coordinates))
                return f"LINESTRING({coords_str})"
                
            elif geom_type == 'Polygon':
                # Exterior ring
                exterior_coords = ', '.join(transform_coords(coordinates[0]))
                
                # Interior rings (holes) if any
                if len(coordinates) > 1:
                    holes = []
                    for hole in coordinates[1:]:
                        hole_coords = ', '.join(transform_coords(hole))
                        holes.append(f"({hole_coords})")
                    holes_str = ', ' + ', '.join(holes)
                else:
                    holes_str = ''
                
                return f"POLYGON(({exterior_coords}){holes_str})"
                
            elif geom_type == 'MultiPoint':
                points = []
                for coord in coordinates:
                    coord_str = transform_coord_pair(coord)
                    points.append(f"({coord_str})")
                return f"MULTIPOINT({', '.join(points)})"
                
            elif geom_type == 'MultiLineString':
                lines = []
                for line in coordinates:
                    coords_str = ', '.join(transform_coords(line))
                    lines.append(f"({coords_str})")
                return f"MULTILINESTRING({', '.join(lines)})"
                
            elif geom_type == 'MultiPolygon':
                polygons = []
                for poly_coords in coordinates:
                    # Exterior ring
                    exterior_coords = ', '.join(transform_coords(poly_coords[0]))
                    
                    # Interior rings (holes) if any
                    if len(poly_coords) > 1:
                        holes = []
                        for hole in poly_coords[1:]:
                            hole_coords = ', '.join(transform_coords(hole))
                            holes.append(f"({hole_coords})")
                        holes_str = ', ' + ', '.join(holes)
                    else:
                        holes_str = ''
                    
                    polygons.append(f"(({exterior_coords}){holes_str})")
                
                return f"MULTIPOLYGON({', '.join(polygons)})"
                
            else:
                return None
                
        except Exception as e:
            self.logger.debug(f"Error converting geometry: {e}")
            return None
    
    def _bulk_insert_features(self, features: List[Dict]):
        """Efficiently bulk insert features into DuckDB."""
        if not features:
            return
        
        try:
            # Insert into temporary table first for better performance
            self.conn.executemany("""
                INSERT INTO features_temp VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, [
                (f['feature_id'], f['layer'], f['tile_x'], f['tile_y'], f['tile_z'],
                 f['session'], f['geometry_type'], f['geometry_wkt'], f['properties'])
                for f in features
            ])
            
            # Convert WKT to GEOMETRY and insert into main table using DuckDB spatial functions
            self.conn.execute("""
                INSERT INTO features (feature_id, layer, tile_x, tile_y, tile_z, session, 
                                    geometry_type, geometry, properties)
                SELECT feature_id, layer, tile_x, tile_y, tile_z, session, 
                       geometry_type, ST_GeomFromText(geometry_wkt), properties::JSON
                FROM features_temp
            """)
            
            # Clear temporary table
            self.conn.execute("DELETE FROM features_temp")
            
            with self.lock:
                self.stats.total_features += len(features)
                self.stats.duckdb_operations += 2  # insert + conversion
                
        except Exception as e:
            self.logger.error(f"❌ Error bulk inserting features: {e}")
    
    def process_all_tiles(self, tile_files: List[Dict]):
        """Process all tiles using parallel workers and DuckDB for aggregation."""
        self.logger.info(f"🔄 Processing {len(tile_files):,} tiles with {self.max_workers} workers...")
        
        self.stats.total_files = len(tile_files)
        self.stats.start_time = time.time()
        
        # Setup feature tables
        self.setup_feature_tables()
        
        # Calculate optimal batch size based on memory
        optimal_batch_size = max(10, min(1000, int(self.system_resources.duckdb_memory_gb * 100)))
        
        self.logger.info(f"📦 Using batch size: {optimal_batch_size} tiles per batch")
        
        # Process in batches with parallel workers
        total_batches = math.ceil(len(tile_files) / optimal_batch_size)
        
        for batch_idx in range(total_batches):
            start_idx = batch_idx * optimal_batch_size
            end_idx = min(start_idx + optimal_batch_size, len(tile_files))
            batch_tiles = tile_files[start_idx:end_idx]
            
            self.logger.info(f"🏗️  Processing batch {batch_idx + 1}/{total_batches} ({len(batch_tiles)} tiles)")
            
            # Process batch with workers
            if self.max_workers > 1:
                with ProcessPoolExecutor(max_workers=self.max_workers) as executor:
                    # Split batch among workers
                    worker_batch_size = max(1, len(batch_tiles) // self.max_workers)
                    worker_batches = [
                        batch_tiles[i:i + worker_batch_size] 
                        for i in range(0, len(batch_tiles), worker_batch_size)
                    ]
                    
                    # Submit worker tasks using standalone function
                    futures = [
                        executor.submit(process_worker_batch_standalone, worker_batch)
                        for worker_batch in worker_batches
                    ]
                    
                    # Collect results and insert into DuckDB
                    for future in as_completed(futures):
                        try:
                            features = future.result()
                            if features:
                                self._bulk_insert_features(features)
                                with self.lock:
                                    self.stats.processed_files += len([f for f in batch_tiles if any(feat['tile_x'] == f['x'] and feat['tile_y'] == f['y'] for feat in features)])
                            self.logger.debug(f"Worker batch completed: {len(features)} features")
                        except Exception as e:
                            self.logger.error(f"Worker batch failed: {e}")
            else:
                # Single-threaded processing
                self.process_pbf_batch(batch_tiles)
            
            # Progress reporting
            elapsed = time.time() - self.stats.start_time
            rate = self.stats.processed_files / elapsed if elapsed > 0 else 0
            
            # Get current feature count from DuckDB
            current_features = self.conn.execute("SELECT COUNT(*) FROM features").fetchone()[0]
            
            self.logger.info(
                f"📊 Progress: {self.stats.processed_files:,}/{len(tile_files):,} tiles "
                f"({self.stats.processed_files/len(tile_files)*100:.1f}%) - "
                f"{rate:.1f} tiles/sec - {current_features:,} features"
            )
        
        self.stats.end_time = time.time()
        
        # Final feature count
        final_count = self.conn.execute("SELECT COUNT(*) FROM features").fetchone()[0]
        self.stats.total_features = final_count
        
        self.logger.info(f"✅ Processing complete: {final_count:,} features extracted")
    
    def optimize_and_export(self):
        """Optimize data in DuckDB and export to GeoParquet."""
        self.logger.info("🚀 Optimizing data and exporting to GeoParquet...")
        
        # Create spatial index for performance
        self.logger.info("📍 Creating spatial index...")
        try:
            self.conn.execute("CREATE INDEX idx_features_geom ON features USING GIST (geometry)")
            self.logger.info("✅ Spatial index created")
        except Exception as e:
            self.logger.warning(f"⚠️  Could not create spatial index: {e}")
        
        # Create layer index
        self.conn.execute("CREATE INDEX idx_features_layer ON features (layer)")
        
        # Get layer statistics using DuckDB aggregations
        layer_stats = self.conn.execute("""
            SELECT 
                layer, 
                COUNT(*) as feature_count,
                COUNT(DISTINCT geometry_type) as geometry_types,
                MIN(tile_x) as min_x, MAX(tile_x) as max_x,
                MIN(tile_y) as min_y, MAX(tile_y) as max_y
            FROM features 
            GROUP BY layer 
            ORDER BY feature_count DESC
        """).fetchall()
        
        self.logger.info("📊 Layer statistics:")
        for layer, count, geom_types, min_x, max_x, min_y, max_y in layer_stats:
            self.logger.info(f"   • {layer}: {count:,} features, {geom_types} geometry types, "
                           f"tiles X[{min_x}-{max_x}] Y[{min_y}-{max_y}]")
        
        # Export to GeoParquet using DuckDB's spatial extension
        self.logger.info(f"💾 Exporting to GeoParquet: {self.output_file}")
        
        export_start = time.time()
        
        # Export with spatial metadata and compression
        self.conn.execute(f"""
            COPY (
                SELECT 
                    feature_id,
                    layer,
                    tile_x, tile_y, tile_z,
                    session,
                    geometry_type,
                    geometry,
                    properties,
                    created_at
                FROM features
                ORDER BY layer, tile_x, tile_y, feature_id
            ) TO '{self.output_file}' 
            (FORMAT PARQUET, COMPRESSION 'snappy')
        """)
        
        export_elapsed = time.time() - export_start
        file_size = self.output_file.stat().st_size / (1024 * 1024)
        
        self.logger.info(f"✅ Export completed in {export_elapsed:.1f}s")
        self.logger.info(f"📦 File size: {file_size:.1f} MB")
        self.logger.info(f"📊 Compression ratio: {self.stats.total_features * 200 / (file_size * 1024 * 1024):.1f}x")
        
        # Validate output using DuckDB
        self._validate_output()
    
    def _validate_output(self):
        """Validate the exported GeoParquet file using DuckDB."""
        self.logger.info("🔍 Validating exported GeoParquet...")
        
        try:
            # Read back the exported file
            validation_query = f"""
                SELECT 
                    COUNT(*) as total_features,
                    COUNT(DISTINCT layer) as unique_layers,
                    COUNT(DISTINCT geometry_type) as unique_geometry_types,
                    MIN(tile_x) as min_x, MAX(tile_x) as max_x,
                    MIN(tile_y) as min_y, MAX(tile_y) as max_y,
                    ST_XMin(ST_Envelope(ST_Union_Agg(geometry))) as bbox_west,
                    ST_YMin(ST_Envelope(ST_Union_Agg(geometry))) as bbox_south,
                    ST_XMax(ST_Envelope(ST_Union_Agg(geometry))) as bbox_east,
                    ST_YMax(ST_Envelope(ST_Union_Agg(geometry))) as bbox_north
                FROM read_parquet('{self.output_file}')
            """
            
            validation_result = self.conn.execute(validation_query).fetchone()
            
            self.logger.info("✅ Validation results:")
            self.logger.info(f"   📊 Total features: {validation_result[0]:,}")
            self.logger.info(f"   📚 Unique layers: {validation_result[1]}")
            self.logger.info(f"   🔷 Geometry types: {validation_result[2]}")
            self.logger.info(f"   🗺️  Tile range: X[{validation_result[3]}-{validation_result[4]}], Y[{validation_result[5]}-{validation_result[6]}]")
            
            if all(x is not None for x in validation_result[7:11]):
                self.logger.info(f"   🌍 Geographic bounds: [{validation_result[7]:.6f}, {validation_result[8]:.6f}, {validation_result[9]:.6f}, {validation_result[10]:.6f}]")
            
        except Exception as e:
            self.logger.error(f"❌ Validation failed: {e}")
    
    def save_processing_stats(self):
        """Save comprehensive processing statistics."""
        # Get final statistics from DuckDB
        db_stats = self.conn.execute("""
            SELECT 
                COUNT(*) as total_features,
                COUNT(DISTINCT layer) as unique_layers,
                COUNT(DISTINCT CONCAT(tile_x, '_', tile_y)) as unique_tiles,
                MIN(created_at) as first_feature_time,
                MAX(created_at) as last_feature_time
            FROM features
        """).fetchone()
        
        layer_breakdown = self.conn.execute("""
            SELECT layer, COUNT(*) as count
            FROM features
            GROUP BY layer
            ORDER BY count DESC
        """).fetchall()
        
        geometry_breakdown = self.conn.execute("""
            SELECT geometry_type, COUNT(*) as count
            FROM features
            GROUP BY geometry_type
            ORDER BY count DESC
        """).fetchall()
        
        # Compile comprehensive statistics
        stats_dict = {
            'processing_time': self.stats.end_time - self.stats.start_time,
            'processing_date': datetime.now().isoformat(),
            'system_resources': {
                'total_memory_gb': self.system_resources.total_memory_gb,
                'duckdb_memory_gb': self.system_resources.duckdb_memory_gb,
                'total_cpu_cores': self.system_resources.total_cpu_cores,
                'duckdb_threads': self.system_resources.duckdb_threads,
                'worker_processes': self.max_workers
            },
            'input_statistics': {
                'total_files': self.stats.total_files,
                'processed_files': self.stats.processed_files,
                'failed_files': self.stats.failed_files,
                'success_rate': (self.stats.processed_files / self.stats.total_files * 100) if self.stats.total_files > 0 else 0
            },
            'output_statistics': {
                'total_features': db_stats[0] if db_stats else self.stats.total_features,
                'unique_layers': db_stats[1] if db_stats else len(self.stats.layers_found),
                'unique_tiles': db_stats[2] if db_stats else 0,
                'layers_found': sorted(list(self.stats.layers_found)),
                'layer_breakdown': dict(layer_breakdown),
                'geometry_breakdown': dict(geometry_breakdown)
            },
            'performance_metrics': {
                'files_per_second': self.stats.processed_files / (self.stats.end_time - self.stats.start_time) if self.stats.end_time > self.stats.start_time else 0,
                'features_per_second': self.stats.total_features / (self.stats.end_time - self.stats.start_time) if self.stats.end_time > self.stats.start_time else 0,
                'duckdb_operations': self.stats.duckdb_operations
            },
            'output_file': {
                'path': str(self.output_file),
                'size_mb': self.output_file.stat().st_size / (1024 * 1024) if self.output_file.exists() else 0,
                'format': 'GeoParquet',
                'compression': 'snappy'
            }
        }
        
        # Save to JSON
        with open(self.stats_file, 'w') as f:
            json.dump(stats_dict, f, indent=2, default=str)
        
        self.logger.info(f"📊 Statistics saved to {self.stats_file}")
        return stats_dict
    
    def cleanup(self):
        """Clean up temporary resources."""
        try:
            # Close DuckDB connection
            self.conn.close()
            
            # Remove temporary directory
            import shutil
            if self.temp_dir.exists():
                shutil.rmtree(self.temp_dir)
            
            self.logger.info("🧹 Cleanup completed")
            
        except Exception as e:
            self.logger.warning(f"⚠️  Cleanup warning: {e}")
    
    def print_final_summary(self, stats_dict: Dict):
        """Print comprehensive final summary."""
        processing_time = stats_dict['processing_time']
        minutes = int(processing_time // 60)
        seconds = int(processing_time % 60)
        
        self.logger.info("\n" + "="*70)
        self.logger.info("🎉 DUCKDB PROCESSING COMPLETE")
        self.logger.info("="*70)
        
        # Input summary
        input_stats = stats_dict['input_statistics']
        self.logger.info(f"📥 INPUT:")
        self.logger.info(f"   • Files processed: {input_stats['processed_files']:,} / {input_stats['total_files']:,}")
        self.logger.info(f"   • Success rate: {input_stats['success_rate']:.1f}%")
        self.logger.info(f"   • Failed files: {input_stats['failed_files']}")
        
        # Output summary
        output_stats = stats_dict['output_statistics']
        self.logger.info(f"📤 OUTPUT:")
        self.logger.info(f"   • Total features: {output_stats['total_features']:,}")
        self.logger.info(f"   • Unique layers: {output_stats['unique_layers']}")
        self.logger.info(f"   • Unique tiles: {output_stats['unique_tiles']:,}")
        self.logger.info(f"   • File: {self.output_file}")
        self.logger.info(f"   • Size: {stats_dict['output_file']['size_mb']:.1f} MB")
        
        # Performance summary
        perf_stats = stats_dict['performance_metrics']
        self.logger.info(f"⚡ PERFORMANCE:")
        self.logger.info(f"   • Processing time: {minutes}m {seconds}s")
        self.logger.info(f"   • Files/second: {perf_stats['files_per_second']:.1f}")
        self.logger.info(f"   • Features/second: {perf_stats['features_per_second']:.1f}")
        self.logger.info(f"   • DuckDB operations: {perf_stats['duckdb_operations']:,}")
        
        # Layer breakdown
        if output_stats['layer_breakdown']:
            self.logger.info(f"📚 LAYERS:")
            for layer, count in list(output_stats['layer_breakdown'].items())[:5]:  # Top 5 layers
                self.logger.info(f"   • {layer}: {count:,} features")
            if len(output_stats['layer_breakdown']) > 5:
                self.logger.info(f"   • ... and {len(output_stats['layer_breakdown']) - 5} more layers")
        
        # Geometry breakdown
        if output_stats['geometry_breakdown']:
            self.logger.info(f"🔷 GEOMETRIES:")
            for geom_type, count in output_stats['geometry_breakdown'].items():
                self.logger.info(f"   • {geom_type}: {count:,} features")
        
        # System resources
        sys_stats = stats_dict['system_resources']
        self.logger.info(f"🖥️  SYSTEM:")
        self.logger.info(f"   • DuckDB memory: {sys_stats['duckdb_memory_gb']:.1f} GB")
        self.logger.info(f"   • DuckDB threads: {sys_stats['duckdb_threads']}")
        self.logger.info(f"   • Worker processes: {sys_stats['worker_processes']}")
        
        self.logger.info("="*70)
    
    def process(self):
        """
        Main processing pipeline using DuckDB for maximum performance.
        """
        try:
            # Step 1: Discover PBF files and create file inventory
            tile_files = self.discover_pbf_files()
            
            # Step 2: Process all tiles using DuckDB for data processing
            self.process_all_tiles(tile_files)
            
            # Step 3: Optimize data and export to GeoParquet
            self.optimize_and_export()
            
            # Step 4: Save comprehensive statistics
            stats_dict = self.save_processing_stats()
            
            # Step 5: Print final summary
            self.print_final_summary(stats_dict)
            
            return stats_dict
            
        except Exception as e:
            self.logger.error(f"❌ Processing failed: {e}")
            raise
        finally:
            self.cleanup()


def process_worker_batch_standalone(worker_batch: List[Dict]) -> List[Dict]:
    """
    Standalone function for processing a batch of PBF files in a worker process.
    Optimized version without Shapely dependency for maximum performance.
    
    Args:
        worker_batch: List of tile information dictionaries
        
    Returns:
        List of processed features
    """
    import mapbox_vector_tile
    import json
    import time
    import math
    from typing import Dict, Tuple, Optional
    
    def tile_to_bbox(x: int, y: int, z: int) -> Tuple[float, float, float, float]:
        """Calculate geographic bounding box for a tile."""
        n = 2.0 ** z
        west = x / n * 360.0 - 180.0
        east = (x + 1) / n * 360.0 - 180.0
        
        north_rad = math.atan(math.sinh(math.pi * (1 - 2 * y / n)))
        north = math.degrees(north_rad)
        
        south_rad = math.atan(math.sinh(math.pi * (1 - 2 * (y + 1) / n)))
        south = math.degrees(south_rad)
        
        return west, south, east, north
    
    def mvt_to_geographic_wkt(geometry_data: Dict, x: int, y: int, z: int, tile_extent: int = 4096) -> Optional[str]:
        """
        Convert MVT geometry directly to geographic WKT for maximum performance.
        No Shapely dependency - direct coordinate transformation.
        """
        geom_type = geometry_data.get('type', '')
        coordinates = geometry_data.get('coordinates', [])
        
        if not coordinates:
            return None
        
        # Calculate transformation parameters once
        west, south, east, north = tile_to_bbox(x, y, z)
        x_scale = (east - west) / tile_extent
        y_scale = (north - south) / tile_extent
        
        def transform_coord_pair(coord_pair):
            """Transform a single coordinate pair [x, y] to geographic."""
            tile_x, tile_y = coord_pair
            lon = west + (tile_x * x_scale)
            lat = south + (tile_y * y_scale)
            return f"{lon} {lat}"
        
        def transform_coords(coords):
            """Transform coordinate array."""
            return [transform_coord_pair(coord) for coord in coords]
        
        try:
            if geom_type == 'Point':
                coord_str = transform_coord_pair(coordinates)
                return f"POINT({coord_str})"
                
            elif geom_type == 'LineString':
                coords_str = ', '.join(transform_coords(coordinates))
                return f"LINESTRING({coords_str})"
                
            elif geom_type == 'Polygon':
                # Exterior ring
                exterior_coords = ', '.join(transform_coords(coordinates[0]))
                
                # Interior rings (holes) if any
                if len(coordinates) > 1:
                    holes = []
                    for hole in coordinates[1:]:
                        hole_coords = ', '.join(transform_coords(hole))
                        holes.append(f"({hole_coords})")
                    holes_str = ', ' + ', '.join(holes)
                else:
                    holes_str = ''
                
                return f"POLYGON(({exterior_coords}){holes_str})"
                
            elif geom_type == 'MultiPoint':
                points = []
                for coord in coordinates:
                    coord_str = transform_coord_pair(coord)
                    points.append(f"({coord_str})")
                return f"MULTIPOINT({', '.join(points)})"
                
            elif geom_type == 'MultiLineString':
                lines = []
                for line in coordinates:
                    coords_str = ', '.join(transform_coords(line))
                    lines.append(f"({coords_str})")
                return f"MULTILINESTRING({', '.join(lines)})"
                
            elif geom_type == 'MultiPolygon':
                polygons = []
                for poly_coords in coordinates:
                    # Exterior ring
                    exterior_coords = ', '.join(transform_coords(poly_coords[0]))
                    
                    # Interior rings (holes) if any
                    if len(poly_coords) > 1:
                        holes = []
                        for hole in poly_coords[1:]:
                            hole_coords = ', '.join(transform_coords(hole))
                            holes.append(f"({hole_coords})")
                        holes_str = ', ' + ', '.join(holes)
                    else:
                        holes_str = ''
                    
                    polygons.append(f"(({exterior_coords}){holes_str})")
                
                return f"MULTIPOLYGON({', '.join(polygons)})"
                
            else:
                return None
                
        except Exception:
            return None
    
    # Process the batch
    all_features = []
    feature_id = int(time.time() * 1000000)
    
    for tile_info in worker_batch:
        try:
            # Read and decode PBF
            with open(tile_info['file_path'], 'rb') as f:
                pbf_data = f.read()
            
            if not pbf_data:
                continue
            
            decoded_tile = mapbox_vector_tile.decode(pbf_data)
            
            # Process each layer
            for layer_name, layer_data in decoded_tile.items():
                layer_features = layer_data.get('features', [])
                
                for feature in layer_features:
                    try:
                        geometry_data = feature.get('geometry', {})
                        properties = feature.get('properties', {})
                        
                        # Convert MVT geometry directly to geographic WKT (no Shapely)
                        geographic_wkt = mvt_to_geographic_wkt(
                            geometry_data, tile_info['x'], tile_info['y'], tile_info['z']
                        )
                        if geographic_wkt is None:
                            continue
                        
                        # Extract geometry type from WKT
                        geometry_type = geographic_wkt.split('(')[0].strip()
                        
                        processed_feature = {
                            'feature_id': feature_id,
                            'layer': layer_name,
                            'tile_x': tile_info['x'],
                            'tile_y': tile_info['y'],
                            'tile_z': tile_info['z'],
                            'session': tile_info['session'],
                            'geometry_type': geometry_type,
                            'geometry_wkt': geographic_wkt,
                            'properties': json.dumps(properties) if properties else '{}'
                        }
                        
                        all_features.append(processed_feature)
                        feature_id += 1
                        
                    except Exception:
                        continue
                        
        except Exception:
            continue
    
    return all_features


def main():
    """Main entry point for the DuckDB-optimized processor."""
    parser = argparse.ArgumentParser(
        description='Naver PBF to GeoParquet Processor - DuckDB Optimized',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
DuckDB Optimizations:
- Spatial extension for geometric operations
- Columnar processing for maximum throughput  
- Optimal memory and thread configuration
- Parallel processing with efficient aggregation
- Direct GeoParquet export with compression

Examples:
  # Auto-optimize for system (recommended)
  python naver_processor_duckdb.py
  
  # Custom worker count
  python naver_processor_duckdb.py --workers 16
  
  # Custom data directory
  python naver_processor_duckdb.py --base-dir /path/to/naver/data
        """
    )
    
    parser.add_argument(
        '--workers', 
        type=int, 
        default=None, 
        help='Number of worker processes for PBF decoding (default: auto-detect)'
    )
    
    parser.add_argument(
        '--base-dir', 
        default='../../data/naver', 
        help='Base data directory (default: ../../data/naver)'
    )
    
    parser.add_argument(
        '--memory-ratio',
        type=float,
        default=0.7,
        help='Ratio of available memory for DuckDB (default: 0.7)'
    )
    
    parser.add_argument(
        '--thread-ratio',
        type=float,
        default=0.8,
        help='Ratio of CPU cores for DuckDB (default: 0.8)'
    )
    
    args = parser.parse_args()
    
    print("🚀 Naver Maps PBF to GeoParquet Processor")
    print("⚡ DuckDB-Optimized for Maximum Performance")
    print("📊 Processing latest session at zoom level 17")
    print("=" * 60)
    
    try:
        # Initialize processor with DuckDB optimizations
        processor = DuckDBTileProcessor(
            base_dir=args.base_dir,
            max_workers=args.workers
        )
        
        # Override system resource ratios if specified
        if args.memory_ratio != 0.7 or args.thread_ratio != 0.8:
            processor.system_resources = SystemResources.detect(
                memory_ratio=args.memory_ratio,
                thread_ratio=args.thread_ratio
            )
            # Recreate connection with new config
            processor.conn.close()
            processor.conn = duckdb.connect(config=processor.system_resources.get_duckdb_config())
            processor.system_resources.configure_duckdb_runtime(processor.conn)
            processor._setup_duckdb_extensions()
            processor.logger.info(f"🔧 Custom ratios: memory={args.memory_ratio}, threads={args.thread_ratio}")
        
        # Run the processing pipeline
        stats = processor.process()
        
        print(f"\n🎉 SUCCESS!")
        print(f"📊 {stats['output_statistics']['total_features']:,} features processed")
        print(f"📁 Output: {stats['output_file']['path']}")
        print(f"📦 Size: {stats['output_file']['size_mb']:.1f} MB")
        print(f"⏱️  Time: {int(stats['processing_time']//60)}m {int(stats['processing_time']%60)}s")
        
    except KeyboardInterrupt:
        print("\n⚠️  Processing interrupted by user")
        sys.exit(1)
    except Exception as e:
        print(f"\n❌ Error: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)


if __name__ == "__main__":
    main()
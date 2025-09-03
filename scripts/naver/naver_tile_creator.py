#!/usr/bin/env python3
"""
Naver GeoParquet to PMTiles Converter with Coverage Statistics

Converts processed Naver Maps GeoParquet data to PMTiles format and generates
comprehensive coverage statistics for the Korean road network.

Features:
- Converts GeoParquet to PMTiles for web visualization
- Generates detailed coverage statistics (km of roads, segments, etc.)
- Fixed output name: naver.pmtiles  
- Fixed zoom range: 3-17
- Automatic dependency installation

Outputs:
- PMTiles file: data/naver/usage/tiles/naver.pmtiles
- Coverage stats: data/naver/usage/stats/naver_coverage_stats.json

Usage:
    python naver_tile_creator.py

Requirements:
- tippecanoe must be installed
- pmtiles will be auto-installed if not found
- Processed GeoParquet file must exist

Author: Data Collection Team
Date: 2024
"""

import os
import sys
import subprocess
import tempfile
import time
import json
from pathlib import Path
from datetime import datetime

import duckdb


def find_latest_session(base_dir: Path) -> Path:
    """Find the latest processed session directory."""
    processed_dir = base_dir / "processed"
    
    if not processed_dir.exists():
        raise FileNotFoundError(f"Processed directory not found: {processed_dir}")
    
    session_dirs = [d for d in processed_dir.iterdir() if d.is_dir() and d.name.isdigit()]
    
    if not session_dirs:
        raise FileNotFoundError(f"No session directories found in {processed_dir}")
    
    return max(session_dirs, key=lambda d: int(d.name))


def install_pmtiles() -> bool:
    """Download and install pmtiles binary for Linux x86_64."""
    print("🔄 Installing pmtiles...")
    
    import urllib.request
    import tarfile
    import shutil
    
    try:
        # Create local bin directory
        local_bin = Path.home() / ".local" / "bin"
        local_bin.mkdir(parents=True, exist_ok=True)
        
        # Download URL
        url = "https://github.com/protomaps/go-pmtiles/releases/download/v1.28.0/go-pmtiles_1.28.0_Linux_x86_64.tar.gz"
        
        # Download to temporary file
        with tempfile.NamedTemporaryFile(suffix='.tar.gz') as tmp_file:
            print("📥 Downloading pmtiles...")
            urllib.request.urlretrieve(url, tmp_file.name)
            
            # Extract tar.gz
            with tarfile.open(tmp_file.name, 'r:gz') as tar:
                # Extract pmtiles binary
                pmtiles_member = tar.getmember('pmtiles')
                pmtiles_member.name = 'pmtiles'  # Remove any path prefix
                tar.extract(pmtiles_member, path=local_bin)
            
            # Make executable
            pmtiles_path = local_bin / "pmtiles"
            pmtiles_path.chmod(0o755)
            
            print(f"✅ pmtiles installed to {pmtiles_path}")
            print(f"💡 Add {local_bin} to your PATH: export PATH=\"$PATH:{local_bin}\"")
            
            return True
            
    except Exception as e:
        print(f"❌ Failed to install pmtiles: {e}")
        return False


def check_dependencies() -> bool:
    """Check if tippecanoe and pmtiles are available."""
    # Check tippecanoe
    try:
        subprocess.run(['tippecanoe', '--version'], capture_output=True)
        print("✅ tippecanoe found")
    except FileNotFoundError:
        print("❌ tippecanoe not found in PATH")
        print("💡 Install with: brew install tippecanoe (macOS) or apt install tippecanoe (Ubuntu)")
        return False
    
    # Check pmtiles
    try:
        subprocess.run(['pmtiles', '--version'], capture_output=True)
        print("✅ pmtiles found")
        return True
    except FileNotFoundError:
        print("❌ pmtiles not found in PATH")
        
        # Try to install automatically
        if install_pmtiles():
            # Check again after installation
            local_bin = Path.home() / ".local" / "bin"
            pmtiles_path = local_bin / "pmtiles"
            
            if pmtiles_path.exists():
                print("✅ pmtiles installed successfully")
                # Update PATH for current session
                os.environ['PATH'] = f"{local_bin}:{os.environ['PATH']}"
                return True
        
        print("💡 Manual install: Download from https://github.com/protomaps/go-pmtiles/releases")
        return False


def generate_coverage_statistics(parquet_file: Path, stats_file: Path):
    """Generate coverage statistics from the GeoParquet file."""
    print(f"📊 Generating coverage statistics...")
    
    conn = duckdb.connect()
    
    try:
        conn.execute("INSTALL spatial")
        conn.execute("LOAD spatial")
    except:
        pass
    
    start_time = time.time()
    
    # DEBUG: Check if distance_km column exists
    print("🔍 DEBUG: Checking available columns in Parquet file...")
    columns_info = conn.execute(f"""
        SELECT column_name, data_type 
        FROM information_schema.columns 
        WHERE table_name = 'read_parquet(''{parquet_file}'')'
    """).fetchall()
    print(f"🔍 DEBUG: Available columns: {[col[0] for col in columns_info]}")
    
    # Alternative method to check columns
    try:
        sample_data = conn.execute(f"""
            SELECT * FROM read_parquet('{parquet_file}') LIMIT 1
        """).fetchone()
        if sample_data:
            print(f"🔍 DEBUG: Sample row columns: {list(sample_data.keys()) if hasattr(sample_data, 'keys') else 'No column names'}")
    except Exception as e:
        print(f"🔍 DEBUG: Error getting sample data: {e}")
    
    # DEBUG: Check if distance_km column exists and has data
    try:
        distance_check = conn.execute(f"""
            SELECT 
                COUNT(*) as total_rows,
                COUNT(distance_km) as rows_with_distance,
                MIN(distance_km) as min_distance,
                MAX(distance_km) as max_distance,
                AVG(distance_km) as avg_distance
            FROM read_parquet('{parquet_file}')
        """).fetchone()
        print(f"🔍 DEBUG: Distance column check: {distance_check}")
        print(f"🔍 DEBUG: Note: distance_km column contains NaN values, using geometric calculation instead")
    except Exception as e:
        print(f"🔍 DEBUG: Error checking distance_km column: {e}")
    
    # Get overall statistics
    overall_stats = conn.execute(f"""
        SELECT 
            COUNT(*) as total_features,
            COUNT(DISTINCT layer) as unique_layers,
            COUNT(DISTINCT geometry_type) as unique_geometry_types,
            COUNT(DISTINCT CONCAT(tile_x, '_', tile_y)) as unique_tiles
        FROM read_parquet('{parquet_file}')
    """).fetchone()
    
    print(f"🔍 DEBUG: Overall stats: {overall_stats}")
    
    # Get layer breakdown
    layer_stats = conn.execute(f"""
        SELECT 
            layer,
            COUNT(*) as feature_count,
            COUNT(DISTINCT geometry_type) as geometry_types
        FROM read_parquet('{parquet_file}')
        GROUP BY layer
        ORDER BY feature_count DESC
    """).fetchall()
    
    print(f"🔍 DEBUG: Layer breakdown: {layer_stats}")
    
    # Get geometry type breakdown
    geometry_stats = conn.execute(f"""
        SELECT 
            geometry_type,
            COUNT(*) as feature_count
        FROM read_parquet('{parquet_file}')
        GROUP BY geometry_type
        ORDER BY feature_count DESC
    """).fetchall()
    
    print(f"🔍 DEBUG: Geometry breakdown: {geometry_stats}")
    
    # DEBUG: Check a few sample geometries before distance calculation
    print("🔍 DEBUG: Checking sample geometries...")
    sample_geoms = conn.execute(f"""
        SELECT 
            geometry_type,
            ST_AsText(geometry) as geom_text,
            ST_IsValid(geometry) as is_valid
        FROM read_parquet('{parquet_file}')
        WHERE geometry_type = 'LINESTRING'
        LIMIT 3
    """).fetchall()
    print(f"🔍 DEBUG: Sample geometries: {sample_geoms}")
    
    # Get distance statistics for LineStrings
    print("🔍 DEBUG: Calculating distance statistics...")
    
    # First, let's deduplicate identical geometries to get accurate statistics
    print("🔍 DEBUG: Deduplicating identical geometries...")
    
    # Count total unique geometries vs duplicates
    dedup_stats = conn.execute(f"""
        SELECT 
            COUNT(*) as total_geometries,
            COUNT(DISTINCT ST_AsText(geometry)) as unique_geometries,
            COUNT(*) - COUNT(DISTINCT ST_AsText(geometry)) as duplicate_geometries
        FROM read_parquet('{parquet_file}')
        WHERE geometry_type = 'LINESTRING'
    """).fetchone()
    
    print(f"🔍 DEBUG: Deduplication stats: {dedup_stats}")
    
    # Use the working method: calculate length in degrees then convert to km
    # At the latitude of Korea (~35.5°N):
    # - 1 degree latitude ≈ 111 km (correct)
    # - 1 degree longitude ≈ 90.5 km (cos(35.5°) * 111)
    # Using 90 km/deg as average for mixed road orientations (closer to longitude factor)
    
    # First, create a deduplicated view of the data
    print("🔍 DEBUG: Creating deduplicated dataset for accurate calculations...")
    
    # Calculate statistics on the deduplicated dataset
    distance_stats = conn.execute(f"""
        WITH deduplicated_geometries AS (
            SELECT DISTINCT ST_AsText(geometry) as geom_text, geometry
            FROM read_parquet('{parquet_file}')
            WHERE geometry_type = 'LINESTRING'
        )
        SELECT 
            COUNT(*) as unique_linestring_count,
            ROUND(SUM(ST_Length(geometry) * 90.0), 2) as total_distance_km,
            ROUND(AVG(ST_Length(geometry) * 90.0), 3) as avg_distance_km,
            ROUND(MIN(ST_Length(geometry) * 90.0), 3) as min_distance_km,
            ROUND(MAX(ST_Length(geometry) * 90.0), 3) as max_distance_km
        FROM deduplicated_geometries
    """).fetchone()
    
    print(f"🔍 DEBUG: Using WGS84 calculation with 90.0 km/degree conversion (Korea-specific)")
    print(f"🔍 DEBUG: Note: Using CTE to deduplicate geometries before calculation")
    
    print(f"🔍 DEBUG: Raw distance stats (deduplicated): {distance_stats}")
    
    # DEBUG: Show the impact of deduplication on total length
    if dedup_stats and distance_stats:
        print(f"🔍 DEBUG: === DEDUPLICATION IMPACT ===")
        print(f"🔍 DEBUG: Before dedup: {dedup_stats[0]:,} geometries")
        print(f"🔍 DEBUG: After dedup: {dedup_stats[1]:,} unique geometries")
        print(f"🔍 DEBUG: Duplicates removed: {dedup_stats[2]:,} ({dedup_stats[2]/dedup_stats[0]*100:.1f}%)")
        print(f"🔍 DEBUG: Total length after dedup: {distance_stats[1]:,.2f} km")
        print(f"🔍 DEBUG: ================================")
    
    # DEBUG: Show the final result immediately after calculation
    if distance_stats and distance_stats[1]:
        print(f"🎯 FINAL RESULT: Network coverage after deduplication: {distance_stats[1]:,.2f} km")
        print(f"🎯 FINAL RESULT: Unique segments: {distance_stats[0]:,}")
    else:
        print(f"⚠️  WARNING: Distance calculation failed or returned NaN")
    
    print(f"🔍 DEBUG: ================================")
    
    # DEBUG: Analyze segment length distribution to identify duplicates/short segments
    print("🔍 DEBUG: Analyzing segment length distribution...")
    try:
        length_distribution = conn.execute(f"""
            SELECT 
                CASE 
                    WHEN ST_Length(geometry) * 90.0 < 0.001 THEN 'Very short (<1m)'
                    WHEN ST_Length(geometry) * 90.0 < 0.01 THEN 'Short (1-10m)'
                    WHEN ST_Length(geometry) * 90.0 < 0.1 THEN 'Medium (10-100m)'
                    WHEN ST_Length(geometry) * 90.0 < 1.0 THEN 'Long (100m-1km)'
                    ELSE 'Very long (>1km)'
                END as length_category,
                COUNT(*) as segment_count,
                ROUND(SUM(ST_Length(geometry) * 90.0), 2) as total_length_km
            FROM read_parquet('{parquet_file}')
            WHERE geometry_type = 'LINESTRING'
            GROUP BY length_category
            ORDER BY total_length_km DESC
        """).fetchall()
        print(f"🔍 DEBUG: Length distribution: {length_distribution}")
        
        # Check for very short segments that might be duplicates
        short_segments = conn.execute(f"""
            SELECT COUNT(*) as count
            FROM read_parquet('{parquet_file}')
            WHERE geometry_type = 'LINESTRING' 
            AND ST_Length(geometry) * 90.0 < 0.01
        """).fetchone()
        print(f"🔍 DEBUG: Segments < 10m: {short_segments[0] if short_segments else 0}")
        
        # Check for potential duplicate coordinates
        duplicate_check = conn.execute(f"""
            SELECT COUNT(*) as count
            FROM (
                SELECT ST_AsText(geometry) as geom_text
                FROM read_parquet('{parquet_file}')
                WHERE geometry_type = 'LINESTRING'
                GROUP BY ST_AsText(geometry)
                HAVING COUNT(*) > 1
            ) duplicates
        """).fetchone()
        print(f"🔍 DEBUG: Geometries with exact duplicate coordinates: {duplicate_check[0] if duplicate_check else 0}")
        
        # Check for segments that might be duplicates due to very close coordinates
        close_duplicates = conn.execute(f"""
            SELECT COUNT(*) as count
            FROM read_parquet('{parquet_file}')
            WHERE geometry_type = 'LINESTRING'
            AND ST_Length(geometry) * 90.0 < 0.001  -- Less than 1 meter
        """).fetchone()
        print(f"🔍 DEBUG: Segments < 1m (potential duplicates): {close_duplicates[0] if close_duplicates else 0}")
        
        # Sample of very short segments to understand the issue
        sample_short = conn.execute(f"""
            SELECT 
                ST_AsText(geometry) as geom_text,
                ROUND(ST_Length(geometry) * 90.0, 6) as length_m
            FROM read_parquet('{parquet_file}')
            WHERE geometry_type = 'LINESTRING'
            AND ST_Length(geometry) * 90.0 < 0.01
            LIMIT 3
        """).fetchall()
        print(f"🔍 DEBUG: Sample of short segments: {sample_short}")
        
    except Exception as e:
        print(f"🔍 DEBUG: Error in length distribution analysis: {e}")
    
    # DEBUG: Verify our distance calculation worked
    print("🔍 DEBUG: Verifying distance calculation results...")
    if distance_stats and distance_stats[1] is not None:
        print(f"🔍 DEBUG: ✅ Distance calculation successful: {distance_stats[1]:,.2f} km total")
    else:
        print(f"🔍 DEBUG: ❌ Distance calculation failed or returned None")
    
    # Get geographic bounds
    bounds_stats = conn.execute(f"""
        SELECT 
            MIN(tile_x) as min_x, MAX(tile_x) as max_x,
            MIN(tile_y) as min_y, MAX(tile_y) as max_y
        FROM read_parquet('{parquet_file}')
    """).fetchone()
    
    print(f"🔍 DEBUG: Bounds stats: {bounds_stats}")
    
    # Calculate geographic bounds from tile coordinates (zoom level 17)
    def tile_to_bbox(x, y, z):
        """Convert tile coordinates to geographic bbox."""
        import math
        n = 2.0 ** z
        lon_deg = x / n * 360.0 - 180.0
        lat_rad = math.atan(math.sinh(math.pi * (1 - 2 * y / n)))
        lat_deg = math.degrees(lat_rad)
        return lon_deg, lat_deg
    
    if bounds_stats:
        min_x, max_x, min_y, max_y = bounds_stats
        west, north = tile_to_bbox(min_x, min_y, 17)
        east, south = tile_to_bbox(max_x, max_y, 17)
        geographic_bounds = [west, south, east, north]
    else:
        geographic_bounds = None
    
    print(f"🔍 DEBUG: Geographic bounds: {geographic_bounds}")
    
    # Compile statistics
    stats = {
        'generation_info': {
            'timestamp': datetime.now().isoformat(),
            'source_file': str(parquet_file),
            'generation_time_seconds': round(time.time() - start_time, 2)
        },
        'overall_statistics': {
            'total_features': overall_stats[0] if overall_stats else 0,
            'unique_layers': overall_stats[1] if overall_stats else 0,
            'unique_geometry_types': overall_stats[2] if overall_stats else 0,
            'unique_tiles': overall_stats[3] if overall_stats else 0
        },
        'coverage_statistics': {
            'total_linestring_segments': distance_stats[0] if distance_stats else 0,
            'total_network_length_km': distance_stats[1] if distance_stats and distance_stats[1] else 0,
            'average_segment_length_km': distance_stats[2] if distance_stats and distance_stats[2] else 0,
            'min_segment_length_km': distance_stats[3] if distance_stats and distance_stats[3] else 0,
            'max_segment_length_km': distance_stats[4] if distance_stats and distance_stats[4] else 0
        },
        'deduplication_info': {
            'total_geometries_before_dedup': dedup_stats[0] if dedup_stats else 0,
            'unique_geometries_after_dedup': dedup_stats[1] if dedup_stats else 0,
            'duplicate_geometries_removed': dedup_stats[2] if dedup_stats else 0,
            'duplication_rate_percent': round((dedup_stats[2] / dedup_stats[0] * 100), 2) if dedup_stats and dedup_stats[0] > 0 else 0
        },
        'layer_breakdown': {
            layer: count for layer, count, _ in layer_stats
        },
        'geometry_breakdown': {
            geom_type: count for geom_type, count in geometry_stats
        },
        'geographic_bounds': {
            'bbox': geographic_bounds,
            'description': 'Bounding box [west, south, east, north] in WGS84 degrees'
        },
        'tile_coverage': {
            'tile_range_x': [min_x, max_x] if bounds_stats else None,
            'tile_range_y': [min_y, max_y] if bounds_stats else None,
            'zoom_level': 17
        }
    }
    
    print(f"🔍 DEBUG: Final stats object: {stats}")
    
    # Save statistics to JSON file
    with open(stats_file, 'w', encoding='utf-8') as f:
        json.dump(stats, f, indent=2, ensure_ascii=False)
    
    conn.close()
    
    elapsed = time.time() - start_time
    file_size_kb = stats_file.stat().st_size / 1024
    
    print(f"✅ Coverage statistics generated in {elapsed:.1f}s ({file_size_kb:.1f} KB)")
    
    # Print summary
    if distance_stats and distance_stats[1]:
        print(f"📏 Network coverage: {distance_stats[1]:,} km total length")
        print(f"🛣️  Unique segments: {distance_stats[0]:,} LineString segments (after deduplication)")
        print(f"📊 Features: {overall_stats[0]:,} total features in {overall_stats[1]} layers")
        
        # Show deduplication results
        if dedup_stats:
            print(f"🧹 Deduplication: Removed {dedup_stats[2]:,} duplicate geometries ({dedup_stats[2]/dedup_stats[0]*100:.1f}%)")
    else:
        print(f"⚠️  WARNING: Distance calculation failed or returned NaN")
        print(f"🔍 DEBUG: Check the debug logs above for details")
    
    return stats


def create_geojson(parquet_file: Path, geojson_file: Path):
    """Convert GeoParquet to GeoJSON using DuckDB."""
    print(f"🔄 Converting to GeoJSON...")
    
    conn = duckdb.connect()
    
    try:
        conn.execute("INSTALL spatial")
        conn.execute("LOAD spatial")
    except:
        pass
    
    start_time = time.time()
    
    # Simple conversion to GeoJSON
    conn.execute(f"""
        COPY (
            SELECT json_object(
                'type', 'Feature',
                'geometry', ST_AsGeoJSON(geometry)::JSON,
                'properties', json_object(
                    'layer', layer,
                    'type', geometry_type
                )
            ) as feature
            FROM read_parquet('{parquet_file}')
            WHERE geometry IS NOT NULL
        ) TO '{geojson_file}' 
        (FORMAT JSON, ARRAY true)
    """)
    
    elapsed = time.time() - start_time
    file_size_mb = geojson_file.stat().st_size / (1024 * 1024)
    
    print(f"✅ GeoJSON created in {elapsed:.1f}s ({file_size_mb:.1f} MB)")
    
    conn.close()


def run_tippecanoe(geojson_file: Path, output_file: Path) -> bool:
    """Run tippecanoe to create MBTiles, then convert to PMTiles."""
    print(f"🔄 Creating MBTiles...")
    
    # Step 1: Create MBTiles first
    mbtiles_file = output_file.with_suffix('.mbtiles')
    
    cmd = [
        'tippecanoe',
        '--output', str(mbtiles_file),
        "--read-parallel",
        '--minimum-zoom=3',
        '--maximum-zoom=17',
        '--simplification=20',
        "--no-feature-limit",
        "--maximum-tile-bytes=500000",
        "--force",
        str(geojson_file)
    ]
    
    start_time = time.time()
    
    try:
        result = subprocess.run(cmd, capture_output=True, text=True)
        mbtiles_elapsed = time.time() - start_time
        
        if result.returncode != 0:
            print(f"❌ tippecanoe failed: {result.stderr}")
            return False
        
        mbtiles_size_mb = mbtiles_file.stat().st_size / (1024 * 1024)
        print(f"✅ MBTiles created in {mbtiles_elapsed:.1f}s ({mbtiles_size_mb:.1f} MB)")
        
        # Step 2: Convert MBTiles to PMTiles
        print(f"🔄 Converting to PMTiles...")
        
        convert_start = time.time()
        
        # Try different pmtiles command locations
        pmtiles_cmd = 'pmtiles'
        local_pmtiles = Path.home() / ".local" / "bin" / "pmtiles"
        if local_pmtiles.exists():
            pmtiles_cmd = str(local_pmtiles)
        
        convert_cmd = [pmtiles_cmd, 'convert', str(mbtiles_file), str(output_file)]
        
        convert_result = subprocess.run(convert_cmd, capture_output=True, text=True)
        convert_elapsed = time.time() - convert_start
        
        if convert_result.returncode == 0:
            pmtiles_size_mb = output_file.stat().st_size / (1024 * 1024)
            print(f"✅ PMTiles created in {convert_elapsed:.1f}s ({pmtiles_size_mb:.1f} MB)")
            
            # Clean up temporary MBTiles file
            mbtiles_file.unlink()
            print(f"🧹 Cleaned up temporary MBTiles file")
            
            total_elapsed = time.time() - start_time
            print(f"🎉 Total conversion time: {total_elapsed:.1f}s")
            return True
        else:
            print(f"❌ PMTiles conversion failed: {convert_result.stderr}")
            print("💡 Make sure pmtiles is installed: npm install -g pmtiles")
            return False
            
    except Exception as e:
        print(f"❌ Error in conversion process: {e}")
        return False


def main():
    """Main conversion function."""
    print("🗺️  Naver GeoParquet to PMTiles Converter")
    print("="*50)
    
    # Setup paths
    base_dir = Path("../../data/naver")
    usage_dir = base_dir / "usage" / "tiles"
    stats_dir = base_dir / "usage" / "stats"
    usage_dir.mkdir(parents=True, exist_ok=True)
    stats_dir.mkdir(parents=True, exist_ok=True)
    
    output_file = usage_dir / "naver.pmtiles"
    stats_file = stats_dir / "naver_coverage_stats.json"
    
    try:
        # Check dependencies
        if not check_dependencies():
            sys.exit(1)
        
        # Find input file
        session_dir = find_latest_session(base_dir)
        input_file = session_dir / "naver_panoramas.parquet"
        
        if not input_file.exists():
            raise FileNotFoundError(f"Input file not found: {input_file}")
        
        print(f"📁 Input: {input_file}")
        print(f"📁 PMTiles output: {output_file}")
        print(f"📁 Statistics output: {stats_file}")
        
        # Step 1: Generate coverage statistics
        generate_coverage_statistics(input_file, stats_file)
        
        # Step 2: Convert using temporary GeoJSON
        with tempfile.TemporaryDirectory() as temp_dir:
            geojson_file = Path(temp_dir) / "naver.geojson"
            
            # Step 2a: Create GeoJSON
            create_geojson(input_file, geojson_file)
            
            # Step 2b: Create PMTiles
            success = run_tippecanoe(geojson_file, output_file)
            
            if not success:
                sys.exit(1)
        
        print(f"\n🎉 Success!")
        print(f"📄 PMTiles: {output_file}")
        print(f"📊 Statistics: {stats_file}")
        
    except Exception as e:
        print(f"❌ Error: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()
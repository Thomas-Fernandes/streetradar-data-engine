#!/usr/bin/env python3
"""
Simple Naver GeoParquet to PMTiles Converter

Converts processed Naver Maps GeoParquet data to PMTiles format.
- Fixed output name: naver.pmtiles  
- Fixed zoom range: 0-17
- No configuration options - just works

Usage:
    python naver_pmtiles.py

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
from pathlib import Path

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
        '--simplification=5',
        "--no-feature-limit",
        "--maximum-tile-bytes=2000000",
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
    usage_dir.mkdir(parents=True, exist_ok=True)
    
    output_file = usage_dir / "naver.pmtiles"
    
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
        print(f"📁 Output: {output_file}")
        
        # Convert using temporary GeoJSON
        with tempfile.TemporaryDirectory() as temp_dir:
            geojson_file = Path(temp_dir) / "naver.geojson"
            
            # Step 1: Create GeoJSON
            create_geojson(input_file, geojson_file)
            
            # Step 2: Create PMTiles
            success = run_tippecanoe(geojson_file, output_file)
            
            if not success:
                sys.exit(1)
        
        print(f"\n🎉 Success! PMTiles created: {output_file}")
        
    except Exception as e:
        print(f"❌ Error: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()
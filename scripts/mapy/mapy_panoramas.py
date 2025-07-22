#!/usr/bin/env python3
"""
Mapy.cz Panoramas Data Processing Script

This script consolidates, deduplicates, and processes panorama data from raw parquet files
collected by the Mapy.cz data collection system. It takes multiple batch files and produces
a single, clean dataset of unique panoramas with spatial indexing.

Input:  data/mapy/raw/*.parquet (batch files from collection)
Output: data/mapy/processed/panoramas.parquet (consolidated, deduplicated panoramas)

The script handles:
- Consolidation of multiple batch files
- Deduplication based on panorama ID (keeping most recent)
- Data validation and cleaning
- Spatial indexing for efficient queries
- Statistics reporting

This script is designed to run in automated pipelines (Kestra, Airflow) without user interaction.

Usage:
    python mapy_panoramas.py
"""

import os
import sys
import time
import json
from pathlib import Path
from typing import Dict, List, Optional, Tuple
from datetime import datetime

import duckdb
import pyarrow as pa
import pyarrow.parquet as pq


class PanoramaProcessor:
    """
    Processes and consolidates Mapy.cz panorama data from raw batch files.
    
    This class handles the consolidation of multiple parquet files into a single
    deduplicated dataset, with data validation and cleaning.
    """
    
    def __init__(self, raw_dir: str = "data/mapy/raw/test", processed_dir: str = "data/mapy/processed/test"):
        """
        Initialize the panorama processor.
        
        Args:
            raw_dir: Directory containing raw parquet batch files
            processed_dir: Directory for processed output files
        """
        self.raw_dir = Path(raw_dir)
        self.processed_dir = Path(processed_dir)
        
        # Create directories with parents if they don't exist
        self.processed_dir.mkdir(parents=True, exist_ok=True)
        
        # Create panoramas subdirectory
        self.panoramas_dir = self.processed_dir / "panoramas"
        self.panoramas_dir.mkdir(parents=True, exist_ok=True)
        
        # Output paths
        self.output_file = self.panoramas_dir / "panoramas.parquet"
        self.stats_file = self.panoramas_dir / "statistics.json"
        
        # DuckDB connection with memory optimization
        self.conn = duckdb.connect()
        self._configure_duckdb_for_large_datasets()
        self._setup_spatial_extension()
    
    def _configure_duckdb_for_large_datasets(self):
        """Configure DuckDB for handling large datasets efficiently."""
        try:
            # Memory management - more conservative settings
            self.conn.execute("SET memory_limit='3GB';")  # Very conservative
            self.conn.execute("SET temp_directory='/tmp/duckdb_temp';")  # Use temp disk space
            
            # Performance tuning
            self.conn.execute("SET threads=2;")  # Reduce threads further
            self.conn.execute("SET max_memory='3GB';")
            self.conn.execute("SET enable_progress_bar=true;")
            self.conn.execute("SET preserve_insertion_order=false;")  # Disable for memory saving
            
            # Disk-based operations for large datasets
            self.conn.execute("SET enable_external_access=true;")
            
            print("⚙️  DuckDB configured for large datasets (conservative memory)")
            
        except Exception as e:
            print(f"⚠️  Warning: Could not optimize DuckDB configuration: {e}")
    
    def _setup_spatial_extension(self):
        """Set up DuckDB spatial extension for geographic operations."""
        try:
            self.conn.execute("INSTALL spatial;")
            self.conn.execute("LOAD spatial;")
            print("✅ DuckDB spatial extension loaded")
        except Exception as e:
            print(f"⚠️  Warning: Could not load spatial extension: {e}")
    
    def discover_input_files(self) -> List[Path]:
        """
        Discover all parquet files in the raw directory.
        
        Returns:
            List of paths to parquet files
            
        Raises:
            FileNotFoundError: If raw directory doesn't exist or contains no parquet files
        """
        if not self.raw_dir.exists():
            raise FileNotFoundError(f"Raw directory not found: {self.raw_dir}")
        
        parquet_files = list(self.raw_dir.glob("*.parquet"))
        
        if not parquet_files:
            raise FileNotFoundError(f"No parquet files found in {self.raw_dir}")
        
        print(f"📁 Found {len(parquet_files)} parquet files")
        return sorted(parquet_files)
    
    def validate_input_files(self, files: List[Path]) -> List[Path]:
        """
        Validate that input files are readable and contain expected columns.
        
        Args:
            files: List of parquet file paths
            
        Returns:
            List of valid parquet file paths
        """
        valid_files = []
        required_columns = {'id', 'lat', 'lon', 'date', 'provider'}
        
        for file_path in files:
            try:
                # Read just the schema to check columns
                table = pq.read_table(file_path, columns=list(required_columns))
                
                # Check if file is not empty
                if table.num_rows == 0:
                    print(f"⚠️  Skipping empty file: {file_path.name}")
                    continue
                
                valid_files.append(file_path)
                print(f"✅ Validated: {file_path.name} ({table.num_rows:,} rows)")
                
            except Exception as e:
                print(f"❌ Invalid file {file_path.name}: {e}")
                continue
        
        if not valid_files:
            raise ValueError("No valid parquet files found")
        
        return valid_files
    
    def get_file_statistics(self, files: List[Path]) -> Dict:
        """
        Get basic statistics about input files.
        
        Args:
            files: List of parquet file paths
            
        Returns:
            Dictionary containing file statistics
        """
        total_rows = 0
        total_size = 0
        file_count = len(files)
        
        for file_path in files:
            try:
                table = pq.read_table(file_path)
                total_rows += table.num_rows
                total_size += file_path.stat().st_size
            except Exception as e:
                print(f"⚠️  Could not read {file_path.name}: {e}")
        
        return {
            'file_count': file_count,
            'total_rows': total_rows,
            'total_size_mb': total_size / (1024 * 1024),
            'avg_rows_per_file': total_rows / file_count if file_count > 0 else 0
        }
    
    def consolidate_and_deduplicate(self, files: List[Path]) -> Tuple[int, int]:
        """
        Consolidate multiple parquet files and deduplicate based on panorama ID.
        Uses chunked processing to handle large datasets without OOM.
        
        Args:
            files: List of parquet file paths
            
        Returns:
            Tuple of (total_rows_before, total_rows_after)
        """
        print(f"\n🔄 Consolidating {len(files)} files...")
        
        # Configure DuckDB for better memory management
        self.conn.execute("SET memory_limit='4GB';")  # Adjust based on available RAM
        self.conn.execute("SET threads=4;")  # Limit threads to reduce memory pressure
        
        # Process files in chunks to avoid OOM
        chunk_size = 50  # Process 50 files at a time
        file_chunks = [files[i:i + chunk_size] for i in range(0, len(files), chunk_size)]
        
        total_before = 0
        
        print(f"📦 Processing in {len(file_chunks)} chunks of {chunk_size} files each...")
        
        # First, get total row count
        for chunk_idx, chunk in enumerate(file_chunks):
            chunk_pattern = "', '".join([str(f) for f in chunk])
            chunk_pattern = f"'{chunk_pattern}'"
            
            chunk_count = self.conn.execute(
                f"SELECT COUNT(*) FROM read_parquet([{chunk_pattern}])"
            ).fetchone()[0]
            total_before += chunk_count
            
            if chunk_idx % 10 == 0:
                print(f"   Counted chunk {chunk_idx + 1}/{len(file_chunks)}: {chunk_count:,} rows")
        
        print(f"📊 Total input rows: {total_before:,}")
        
        # Create consolidated table with chunked processing
        start_time = time.time()
        
        for chunk_idx, chunk in enumerate(file_chunks):
            chunk_pattern = "', '".join([str(f) for f in chunk])
            chunk_pattern = f"'{chunk_pattern}'"
            
            if chunk_idx == 0:
                # Create table with first chunk
                query = f"""
                CREATE OR REPLACE TABLE consolidated_panoramas AS
                SELECT DISTINCT ON (id) 
                    id,
                    lat,
                    lon,
                    date,
                    provider,
                    elevation,
                    permalink,
                    "order" as exploration_order,
                    links,
                    link_ids,
                    historical_count,
                    historical,
                    time as collection_time
                FROM read_parquet([{chunk_pattern}])
                WHERE id IS NOT NULL 
                  AND lat IS NOT NULL 
                  AND lon IS NOT NULL
                  AND lat BETWEEN -90 AND 90
                  AND lon BETWEEN -180 AND 180
                ORDER BY id, time DESC;
                """
            else:
                # Insert and deduplicate with subsequent chunks
                query = f"""
                INSERT INTO consolidated_panoramas
                SELECT DISTINCT ON (id) 
                    id,
                    lat,
                    lon,
                    date,
                    provider,
                    elevation,
                    permalink,
                    "order" as exploration_order,
                    links,
                    link_ids,
                    historical_count,
                    historical,
                    time as collection_time
                FROM read_parquet([{chunk_pattern}])
                WHERE id IS NOT NULL 
                  AND lat IS NOT NULL 
                  AND lon IS NOT NULL
                  AND lat BETWEEN -90 AND 90
                  AND lon BETWEEN -180 AND 180
                  AND id NOT IN (SELECT id FROM consolidated_panoramas)
                ORDER BY id, time DESC;
                """
            
            self.conn.execute(query)
            
            # Progress report every 10 chunks
            if chunk_idx % 10 == 0 or chunk_idx == len(file_chunks) - 1:
                current_count = self.conn.execute(
                    "SELECT COUNT(*) FROM consolidated_panoramas"
                ).fetchone()[0]
                print(f"   ✅ Processed chunk {chunk_idx + 1}/{len(file_chunks)}: {current_count:,} total panoramas")
        
        # Final deduplication (in case of overlaps between chunks)
        print("🔧 Final deduplication by chunks...")
        
        # Create temporary table for chunked deduplication
        self.conn.execute("CREATE OR REPLACE TABLE consolidated_temp AS SELECT * FROM consolidated_panoramas LIMIT 0;")
        
        # Get ID ranges for chunked processing
        id_stats = self.conn.execute("""
            SELECT MIN(id) as min_id, MAX(id) as max_id, COUNT(*) as total_count
            FROM consolidated_panoramas
        """).fetchone()
        
        min_id, max_id, total_count = id_stats
        
        # Calculate chunk size based on IDs (aim for ~100k records per chunk)
        target_chunk_size = 100000
        num_chunks = max(1, total_count // target_chunk_size)
        id_range = (max_id - min_id) // num_chunks
        
        print(f"   🔢 Deduplicating ID range {min_id:,} → {max_id:,} in {num_chunks} chunks")
        
        for i in range(num_chunks + 1):
            chunk_start = min_id + (i * id_range)
            chunk_end = min_id + ((i + 1) * id_range) if i < num_chunks else max_id + 1
            
            # Deduplicate this ID range
            self.conn.execute(f"""
                INSERT INTO consolidated_temp
                SELECT DISTINCT ON (id) *
                FROM consolidated_panoramas
                WHERE id >= {chunk_start} AND id < {chunk_end}
                ORDER BY id, collection_time DESC;
            """)
            
            if i % 5 == 0 or i == num_chunks:
                current_count = self.conn.execute("SELECT COUNT(*) FROM consolidated_temp").fetchone()[0]
                print(f"   ✅ Deduplicated chunk {i + 1}/{num_chunks + 1}: {current_count:,} unique panoramas")
        
        # Replace original table with deduplicated version
        self.conn.execute("DROP TABLE consolidated_panoramas;")
        self.conn.execute("ALTER TABLE consolidated_temp RENAME TO consolidated_panoramas;")
        
        total_after = self.conn.execute(
            "SELECT COUNT(*) FROM consolidated_panoramas"
        ).fetchone()[0]
        
        elapsed = time.time() - start_time
        print(f"✅ Consolidation complete in {elapsed:.1f}s")
        print(f"   📊 {total_before:,} → {total_after:,} panoramas ({total_before-total_after:,} duplicates removed)")
        
        return total_before, total_after
    
    def create_spatial_index(self):
        """Create spatial index for efficient geographic queries."""
        print(f"\n📍 Creating spatial index...")
        
        try:
            # Add geometry column for spatial operations
            self.conn.execute("""
                ALTER TABLE consolidated_panoramas 
                ADD COLUMN IF NOT EXISTS geometry GEOMETRY;
            """)
            
            # Update geometry column
            self.conn.execute("""
                UPDATE consolidated_panoramas 
                SET geometry = ST_Point(lon, lat);
            """)
            
            print("✅ Spatial index created")
            
        except Exception as e:
            print(f"⚠️  Warning: Could not create spatial index: {e}")
    
    def validate_output_data(self) -> Dict:
        """
        Validate the consolidated data and generate quality metrics.
        
        Returns:
            Dictionary containing validation results
        """
        print(f"\n🔍 Validating output data...")
        
        # Basic statistics
        stats = self.conn.execute("""
            SELECT 
                COUNT(*) as total_panoramas,
                COUNT(DISTINCT id) as unique_ids,
                COUNT(DISTINCT provider) as unique_providers,
                MIN(lat) as min_lat,
                MAX(lat) as max_lat,
                MIN(lon) as min_lon,
                MAX(lon) as max_lon,
                MIN(date) as earliest_date,
                MAX(date) as latest_date,
                AVG(elevation) as avg_elevation,
                COUNT(CASE WHEN link_ids IS NOT NULL THEN 1 END) as panoramas_with_links,
                COUNT(CASE WHEN historical_count > 0 THEN 1 END) as panoramas_with_history
            FROM consolidated_panoramas
        """).fetchone()
        
        # Provider distribution
        providers = self.conn.execute("""
            SELECT provider, COUNT(*) as count
            FROM consolidated_panoramas
            GROUP BY provider
            ORDER BY count DESC
        """).fetchall()
        
        # Date range analysis
        date_ranges = self.conn.execute("""
            SELECT 
                EXTRACT(YEAR FROM TRY_CAST(date AS TIMESTAMP)) as year,
                COUNT(*) as count
            FROM consolidated_panoramas
            WHERE TRY_CAST(date AS TIMESTAMP) IS NOT NULL
            GROUP BY EXTRACT(YEAR FROM TRY_CAST(date AS TIMESTAMP))
            ORDER BY year
        """).fetchall()
        
        validation_results = {
            'total_panoramas': stats[0],
            'unique_ids': stats[1],
            'unique_providers': stats[2],
            'bbox': {
                'min_lat': float(stats[3]),
                'max_lat': float(stats[4]),
                'min_lon': float(stats[5]),
                'max_lon': float(stats[6])
            },
            'date_range': {
                'earliest': str(stats[7]),
                'latest': str(stats[8])
            },
            'avg_elevation': float(stats[9]) if stats[9] else None,
            'panoramas_with_links': stats[10],
            'panoramas_with_history': stats[11],
            'providers': dict(providers),
            'yearly_distribution': dict(date_ranges)
        }
        
        print(f"✅ Validation complete:")
        print(f"   📊 {validation_results['total_panoramas']:,} unique panoramas")
        print(f"   🏢 {validation_results['unique_providers']} providers")
        print(f"   📅 {validation_results['date_range']['earliest']} → {validation_results['date_range']['latest']}")
        print(f"   🔗 {validation_results['panoramas_with_links']:,} panoramas with links")
        
        return validation_results
    
    def export_to_parquet(self):
        """Export consolidated data to parquet format."""
        print(f"\n💾 Exporting to parquet...")
        
        start_time = time.time()
        
        # Export to parquet with good compression
        self.conn.execute(f"""
            COPY consolidated_panoramas 
            TO '{self.output_file}' 
            (FORMAT PARQUET, COMPRESSION 'snappy')
        """)
        
        elapsed = time.time() - start_time
        file_size = self.output_file.stat().st_size / (1024 * 1024)
        
        print(f"✅ Export complete in {elapsed:.1f}s")
        print(f"   📁 {self.output_file}")
        print(f"   📦 {file_size:.1f} MB")
    
    def save_statistics(self, statistics: Dict):
        """
        Save processing statistics to JSON file.
        
        Args:
            statistics: Dictionary containing processing statistics
        """
        with open(self.stats_file, 'w') as f:
            json.dump(statistics, f, indent=2, default=str)
        
        print(f"✅ Statistics saved to {self.stats_file}")
    
    def process(self):
        """
        Main processing pipeline.
        
        Executes the complete panorama processing workflow:
        1. Discover and validate input files
        2. Consolidate and deduplicate data
        3. Create spatial index
        4. Validate output data
        5. Export to parquet
        6. Save statistics
        """
        print("🚀 Starting panorama processing pipeline...")
        start_time = time.time()
        
        try:
            # Step 1: Discover input files
            input_files = self.discover_input_files()
            
            # Step 2: Validate input files
            valid_files = self.validate_input_files(input_files)
            
            # Step 3: Get input statistics
            input_stats = self.get_file_statistics(valid_files)
            print(f"📊 Input: {input_stats['total_rows']:,} rows, {input_stats['total_size_mb']:.1f} MB")
            
            # Step 4: Consolidate and deduplicate
            rows_before, rows_after = self.consolidate_and_deduplicate(valid_files)
            
            # Step 5: Create spatial index
            self.create_spatial_index()
            
            # Step 6: Validate output
            validation_results = self.validate_output_data()
            
            # Step 7: Export to parquet
            self.export_to_parquet()
            
            # Step 8: Compile final statistics
            total_time = time.time() - start_time
            final_stats = {
                'processing_time': total_time,
                'processing_date': datetime.now().isoformat(),
                'input_files': len(valid_files),
                'input_rows': rows_before,
                'output_rows': rows_after,
                'duplicates_removed': rows_before - rows_after,
                'deduplication_rate': (rows_before - rows_after) / rows_before * 100 if rows_before > 0 else 0,
                'output_file': str(self.output_file),
                'output_size_mb': self.output_file.stat().st_size / (1024 * 1024),
                'validation': validation_results
            }
            
            # Step 9: Save statistics
            self.save_statistics(final_stats)
            
            print(f"\n🎉 Processing complete in {total_time:.1f}s!")
            print(f"   📊 {rows_after:,} panoramas processed")
            print(f"   📁 Output: {self.output_file}")
            
            return final_stats
            
        except Exception as e:
            print(f"❌ Processing failed: {e}")
            raise
        finally:
            # Clean up DuckDB connection
            self.conn.close()


def main():
    """Main entry point for the script."""
    print("🌐 Mapy.cz Panoramas Data Processing")
    print("=" * 50)
    
    try:
        processor = PanoramaProcessor()
        stats = processor.process()
        
        print("\n📈 Final Summary:")
        print(f"   • {stats['output_rows']:,} unique panoramas")
        print(f"   • {stats['duplicates_removed']:,} duplicates removed ({stats['deduplication_rate']:.1f}%)")
        print(f"   • {stats['validation']['unique_providers']} providers")
        print(f"   • {stats['output_size_mb']:.1f} MB output file")
        
    except KeyboardInterrupt:
        print("\n⚠️  Processing interrupted by user")
        sys.exit(1)
    except Exception as e:
        print(f"\n❌ Error: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()
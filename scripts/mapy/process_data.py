#!/usr/bin/env python3
"""
Mapy.cz Data Processor - Combine all raw parquet files into a single geoparquet
"""

import gc
import logging
import os
import sys
import time
from datetime import datetime
from pathlib import Path

# Add streetlevel to path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "../../"))

import geopandas as gpd
import pandas as pd
from shapely.geometry import Point

# Configuration
BASE_DIR = Path(__file__).parent.parent.parent / "data" / "mapy"
RAW_DIR = BASE_DIR / "raw"
PROCESSED_DIR = BASE_DIR / "processed"
OUTPUT_FILE = PROCESSED_DIR / "mapy_complete.parquet"
CHUNK_SIZE = 100000


class MapyDataProcessor:
    def __init__(self):
        PROCESSED_DIR.mkdir(parents=True, exist_ok=True)
        self.setup_logging()
        self.stats = {
            "start_time": time.time(),
            "files_processed": 0,
            "total_records": 0,
            "unique_records": 0,
            "duplicates_removed": 0,
            "invalid_coords": 0,
        }

    def setup_logging(self):
        self.logger = logging.getLogger("MapyProcessor")
        self.logger.setLevel(logging.INFO)
        self.logger.handlers.clear()

        log_file = PROCESSED_DIR / f"processing_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log"
        file_handler = logging.FileHandler(log_file, encoding="utf-8")
        file_handler.setLevel(logging.DEBUG)
        file_formatter = logging.Formatter("%(asctime)s - %(levelname)s - %(message)s")
        file_handler.setFormatter(file_formatter)
        self.logger.addHandler(file_handler)

        console_handler = logging.StreamHandler()
        console_handler.setLevel(logging.INFO)
        console_formatter = logging.Formatter("%(message)s")
        console_handler.setFormatter(console_formatter)
        self.logger.addHandler(console_handler)

        self.logger.info("🚀 Mapy.cz Data Processor initialized")
        self.logger.info(f"📁 Output file: {OUTPUT_FILE}")

    def get_parquet_files(self) -> list[Path]:
        if not RAW_DIR.exists():
            raise FileNotFoundError(f"Raw directory not found: {RAW_DIR}")
        parquet_files = list(RAW_DIR.glob("*.parquet"))
        parquet_files.sort()
        if not parquet_files:
            raise FileNotFoundError(f"No parquet files found in {RAW_DIR}")
        self.logger.info(f"📋 Found {len(parquet_files)} parquet files")
        return parquet_files

    def create_geometry_column(self, df: pd.DataFrame) -> gpd.GeoDataFrame:
        geometry = [Point(lon, lat) for lon, lat in zip(df["lon"], df["lat"], strict=False)]
        gdf = gpd.GeoDataFrame(df, geometry=geometry, crs="EPSG:4326")
        return gdf

    def process_all_files(self):
        parquet_files = self.get_parquet_files()
        seen_ids = set()
        all_chunks = []

        self.logger.info(f"🔄 Processing {len(parquet_files)} files...")

        for i, file_path in enumerate(parquet_files):
            try:
                df = pd.read_parquet(file_path)
                self.stats["files_processed"] += 1
                self.stats["total_records"] += len(df)

                # Remove duplicates
                before_dedup = len(df)
                df = df[~df["id"].isin(seen_ids)]
                seen_ids.update(df["id"])
                global_duplicates = before_dedup - len(df)
                if global_duplicates > 0:
                    self.stats["duplicates_removed"] += global_duplicates

                if not df.empty:
                    # Validate coordinates
                    valid_coords = (
                        df["lat"].notna()
                        & df["lon"].notna()
                        & (df["lat"] >= -90)
                        & (df["lat"] <= 90)
                        & (df["lon"] >= -180)
                        & (df["lon"] <= 180)
                    )
                    df = df[valid_coords]

                    if not df.empty:
                        gdf = self.create_geometry_column(df)
                        all_chunks.append(gdf)

                if (i + 1) % 50 == 0:
                    self.logger.info(f"📊 Progress: {i + 1}/{len(parquet_files)} files")

                del df
                if i % 20 == 0:
                    gc.collect()

            except Exception as e:
                self.logger.error(f"❌ Error processing {file_path}: {e}")
                continue

        if not all_chunks:
            raise ValueError("No valid data found")

        self.logger.info(f"🔗 Combining {len(all_chunks)} chunks...")
        final_gdf = gpd.pd.concat(all_chunks, ignore_index=True)
        self.stats["unique_records"] = len(final_gdf)
        return final_gdf

    def save_geoparquet(self, gdf: gpd.GeoDataFrame):
        self.logger.info("💾 Saving geoparquet...")
        gdf.to_parquet(OUTPUT_FILE, compression="snappy", engine="pyarrow")
        file_size = OUTPUT_FILE.stat().st_size / (1024 * 1024)
        self.logger.info(f"✅ Saved! Size: {file_size:.1f} MB")

    def run(self):
        final_gdf = self.process_all_files()
        self.save_geoparquet(final_gdf)

        elapsed = time.time() - self.stats["start_time"]
        self.logger.info(f"🎉 Completed in {elapsed / 60:.1f} minutes")
        self.logger.info(f"📊 Records: {self.stats['unique_records']:,}")


def main():
    processor = MapyDataProcessor()
    processor.run()


if __name__ == "__main__":
    main()

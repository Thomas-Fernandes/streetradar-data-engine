#!/usr/bin/env python3
"""
JA360 Data Processor
Converts all collected JSON batches into a single GeoJSON file
with all metadata preserved.
"""

import glob
import json
import sys
import time
from collections.abc import Generator
from datetime import datetime
from pathlib import Path
from typing import Any

# Configuration
INPUT_DIR = Path("../../data/ja360/raw")
OUTPUT_DIR = Path("../../data/ja360/processed")
OUTPUT_FILENAME = "ja360_iceland_panoramas.geojson"


class JA360Processor:
    """Processor to convert JA360 data to GeoJSON"""

    def __init__(self):
        self.input_dir = INPUT_DIR
        self.output_dir = OUTPUT_DIR
        self.output_dir.mkdir(parents=True, exist_ok=True)

        # Processing metrics
        self.total_panoramas = 0
        self.processed_batches = 0
        self.start_time = time.time()

        print("🔄 JA360 Data Processor")
        print(f"📁 Source: {self.input_dir}")
        print(f"📁 Destination: {self.output_dir}")
        print(f"📄 Output file: {OUTPUT_FILENAME}")

    def find_batch_files(self) -> list[Path]:
        """Find all JSON batch files"""
        pattern = str(self.input_dir / "ja360_batch_*.json")
        batch_files = [Path(f) for f in glob.glob(pattern)]
        batch_files.sort()  # Sort by name for consistent order

        print(f"📊 {len(batch_files)} batch files found")
        return batch_files

    def load_batch_file(self, filepath: Path) -> dict[str, Any]:
        """Load a JSON batch file"""
        try:
            with open(filepath, encoding="utf-8") as f:
                data = json.load(f)
            return data
        except Exception as e:
            print(f"❌ Error loading {filepath}: {e}")
            return {}

    def panorama_to_geojson_feature(
        self, pano_id: str, pano_data: dict[str, Any]
    ) -> dict[str, Any]:
        """Convert a panorama to a GeoJSON feature"""

        # Point geometry
        geometry = {
            "type": "Point",
            "coordinates": [pano_data["lon"], pano_data["lat"]],  # [longitude, latitude]
        }

        # Properties (all metadata)
        properties = {
            "id": pano_data["id"],
            "heading": pano_data.get("heading"),
            "pano_url": pano_data.get("pano_url"),
            "blur_key": pano_data.get("blur_key"),
            "collection_time": pano_data.get("collection_time"),
        }

        # Add date if available
        if "date" in pano_data and pano_data["date"]:
            properties["capture_year"] = pano_data["date"].get("year")
            properties["capture_month"] = pano_data["date"].get("month")

        # Add address if available
        if "address" in pano_data and pano_data["address"]:
            address = pano_data["address"]
            properties["address_street"] = address.get("street_name_and_house_number")
            properties["address_postal_code"] = address.get("postal_code")
            properties["address_place"] = address.get("place")

        # Add street names if available
        if "street_names" in pano_data and pano_data["street_names"]:
            street_names = []
            for street in pano_data["street_names"]:
                street_info = {
                    "name": street.get("name"),
                    "angles": street.get("angles", []),
                    "distance": street.get("distance"),
                }
                street_names.append(street_info)
            properties["street_names"] = street_names

        # Count neighbors
        if "neighbor_ids" in pano_data:
            properties["neighbor_count"] = len(pano_data["neighbor_ids"])
            properties["neighbor_ids"] = pano_data["neighbor_ids"]

        return {"type": "Feature", "geometry": geometry, "properties": properties}

    def process_all_batches(self) -> Generator[dict[str, Any], None, None]:
        """Generator that processes all batches and yields GeoJSON features"""
        batch_files = self.find_batch_files()

        for batch_file in batch_files:
            print(f"🔄 Processing {batch_file.name}...")

            batch_data = self.load_batch_file(batch_file)
            if not batch_data:
                continue

            batch_count = 0
            for pano_id, pano_data in batch_data.items():
                try:
                    feature = self.panorama_to_geojson_feature(pano_id, pano_data)
                    yield feature
                    batch_count += 1
                    self.total_panoramas += 1
                except Exception as e:
                    print(f"❌ Error with panorama {pano_id}: {e}")
                    continue

            self.processed_batches += 1
            print(f"✅ {batch_count:,} panoramas processed in {batch_file.name}")

    def create_geojson(self) -> None:
        """Create the consolidated GeoJSON file"""
        output_path = self.output_dir / OUTPUT_FILENAME

        print("\n🚀 Starting GeoJSON conversion...")

        # GeoJSON structure
        geojson = {
            "type": "FeatureCollection",
            "metadata": {
                "generated": datetime.now().isoformat(),
                "source": "JA360 Iceland Panoramas",
                "processor": "ja_processor.py",
                "total_features": 0,  # Will be updated at the end
                "bounds": None,  # Will be calculated during processing
            },
            "features": [],
        }

        # Variables to calculate bounds
        min_lon = min_lat = float("inf")
        max_lon = max_lat = float("-inf")

        # Process all batches
        for feature in self.process_all_batches():
            geojson["features"].append(feature)

            # Update bounds
            lon, lat = feature["geometry"]["coordinates"]
            min_lon = min(min_lon, lon)
            max_lon = max(max_lon, lon)
            min_lat = min(min_lat, lat)
            max_lat = max(max_lat, lat)

            # Progress display
            if len(geojson["features"]) % 10000 == 0:
                print(f"📊 {len(geojson['features']):,} features added...")

        # Finalize metadata
        geojson["metadata"]["total_features"] = len(geojson["features"])
        geojson["metadata"]["bounds"] = {
            "min_longitude": min_lon,
            "max_longitude": max_lon,
            "min_latitude": min_lat,
            "max_latitude": max_lat,
        }

        # Save the GeoJSON
        print("\n💾 Saving GeoJSON...")
        try:
            with open(output_path, "w", encoding="utf-8") as f:
                json.dump(geojson, f, ensure_ascii=False, separators=(",", ":"))

            print(f"✅ GeoJSON saved: {output_path}")

        except Exception as e:
            print(f"❌ Error during saving: {e}")
            return

        # Final statistics
        elapsed = time.time() - self.start_time
        file_size_mb = output_path.stat().st_size / (1024 * 1024)

        print("\n🎉 Processing completed!")
        print(f"📊 {self.total_panoramas:,} panoramas processed")
        print(f"📁 {self.processed_batches} batch files processed")
        print(f"⏱️ Total time: {elapsed:.1f}s")
        print(f"📄 File size: {file_size_mb:.1f} MB")
        print(f"🗺️ Bounds: ({min_lat:.4f}, {min_lon:.4f}) → ({max_lat:.4f}, {max_lon:.4f})")


def main():
    """Main function"""
    try:
        processor = JA360Processor()
        processor.create_geojson()

    except KeyboardInterrupt:
        print("\n⚠️ User interruption")
        sys.exit(1)
    except Exception as e:
        print(f"\n❌ Error: {e}")
        import traceback

        traceback.print_exc()
        sys.exit(1)


if __name__ == "__main__":
    main()

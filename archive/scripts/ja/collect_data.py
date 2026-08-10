#!/usr/bin/env python3
"""
JA360 Panorama Scraper - BFS Navigation
Collects all panoramas of Iceland by navigating from neighbor to neighbor
Now supports 3 zones: Reykjavik, Vestmannaeyjar and Grimsey
"""

import argparse
import asyncio
import json
import logging
import os
import sys
import time
from collections import deque
from datetime import datetime
from pathlib import Path

import aiohttp

# Add streetlevel to path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "../../"))

from streetlevel.ja import find_panorama_async, find_panorama_by_id_async

from streetradar.http import HttpClient, HttpPolicy

# Zone configuration
ZONES = {
    1: {
        "name": "Reykjavik",
        "lat": 64.1355,
        "lon": -21.8954,
        "description": "Capital and main island of Iceland",
        "search_radius": 100,
    },
    2: {
        "name": "Vestmannaeyjar",
        "lat": 63.4377,
        "lon": -20.2670,
        "description": "Vestmann Islands - Heimaey (south of Iceland)",
        "search_radius": 500,  # Larger radius for isolated islands
    },
    3: {
        "name": "Grimsey",
        "lat": 66.5442,  # Coordinates converted from ISN93 x=543713, y=671978
        "lon": -18.0133,
        "description": "Grimsey Island - Point with confirmed panoramas (Arctic Circle)",
        "search_radius": 500,  # Larger radius for isolated islands
    },
}

# Configuration
OUTPUT_DIR = Path("../../data/ja360/raw")
SEARCH_RADIUS = 100  # meters
BATCH_SIZE = 2500  # Save every 2500 panoramas (optimized for I/O)
MAX_CONCURRENT = 75  # Concurrent requests (reduced to avoid 504)
NETWORK_TIMEOUT = 30.0  # Timeout for network requests (increased for diagnostics)
PROGRESS_INTERVAL = 100  # Display detailed stats every 1000 collections
MONITORING_INTERVAL = 10.0  # Real-time monitoring log interval (seconds)
BATCH_DELAY = 0.1  # Delay between batches to avoid overloading the server (seconds)


class JA360Scraper:
    def __init__(self, zone_id: int):
        self.zone_id = zone_id
        self.zone = ZONES[zone_id]
        self.zone_name = self.zone["name"]

        # Use the raw folder directly without subfolders
        self.output_dir = OUTPUT_DIR
        self.output_dir.mkdir(parents=True, exist_ok=True)

        # Logging configuration
        self.setup_logging()

        # Scraping state
        self.visited: set[int] = set()  # IDs of already processed panoramas
        self.to_visit: deque = deque()  # BFS queue of panoramas to process
        self.panoramas: dict[int, dict] = {}  # Collected data

        # Metrics
        self.start_time = time.time()
        self.total_requests = 0
        self.errors = 0
        self.timeouts = 0
        self.last_stats_time = time.time()
        self.last_collected_count = 0
        self.last_periodic_log_time = time.time()  # For periodic logs

        # Semaphore to limit concurrency
        # Concurrency now lives in the HttpClient built in the run method.
        self.http: HttpClient | None = None

        self.log_and_print(f"🇮🇸 JA360 Panorama Scraper - Zone: {self.zone_name}")
        self.log_and_print(f"📍 {self.zone['description']}")
        self.log_and_print(f"📁 Output: {self.output_dir}")
        self.log_and_print(
            f"🎯 Starting point: {self.zone_name} ({self.zone['lat']}, {self.zone['lon']})"
        )
        self.log_and_print(f"⚡ Concurrency: {MAX_CONCURRENT} simultaneous requests")
        self.log_and_print(f"⏱️ Network timeout: {NETWORK_TIMEOUT}s")
        self.log_and_print(f"💾 Batch size: {BATCH_SIZE} panoramas")
        self.log_and_print(f"⏸️ Delay between batches: {BATCH_DELAY}s")
        self.log_and_print(f"📊 Detailed statistics every {PROGRESS_INTERVAL} collections")
        self.log_and_print(f"⏰ Real-time monitoring every {MONITORING_INTERVAL}s")

    def setup_logging(self):
        """Configure the logging system"""
        # Create log filename with timestamp
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        log_filename = f"ja360_scraper_{self.zone_name.lower()}_{timestamp}.log"
        log_path = self.output_dir / log_filename

        # Logger configuration
        self.logger = logging.getLogger("JA360Scraper")
        self.logger.setLevel(logging.INFO)

        # Create formatter
        formatter = logging.Formatter(
            "%(asctime)s - %(levelname)s - %(message)s", datefmt="%Y-%m-%d %H:%M:%S"
        )

        # File handler
        file_handler = logging.FileHandler(log_path, encoding="utf-8")
        file_handler.setLevel(logging.INFO)
        file_handler.setFormatter(formatter)

        # Add handler to logger
        self.logger.addHandler(file_handler)

        self.log_path = log_path
        print(f"📝 Log file: {log_path}")

    def log_and_print(self, message: str, level: str = "info"):
        """Display message in terminal AND record it in the log"""
        print(message)

        if level.lower() == "error":
            self.logger.error(message)
        elif level.lower() == "warning":
            self.logger.warning(message)
        else:
            self.logger.info(message)

    def panorama_to_dict(self, pano) -> dict:
        """Convert a JA panorama to a serializable dictionary"""
        data = {
            "id": pano.id,
            "lat": pano.lat,
            "lon": pano.lon,
            "heading": pano.heading,
            "pano_url": pano.pano_url,
            "blur_key": pano.blur_key,
            "collection_time": time.time(),
        }

        # Capture date
        if pano.date:
            data["date"] = {"year": pano.date.year, "month": pano.date.month}

        # Address
        if pano.address:
            data["address"] = {
                "street_name_and_house_number": pano.address.street_name_and_house_number,
                "postal_code": pano.address.postal_code,
                "place": pano.address.place,
            }

        # Street names
        if pano.street_names:
            data["street_names"] = []
            for street in pano.street_names:
                street_data = {
                    "name": street.name,
                    "angles": street.angles,
                    "distance": street.distance,
                }
                data["street_names"].append(street_data)

        # Neighbors (IDs only to avoid recursion)
        if pano.neighbors:
            data["neighbor_ids"] = [neighbor.id for neighbor in pano.neighbors]
        else:
            data["neighbor_ids"] = []

        return data

    async def fetch_panorama_details(
        self, session: aiohttp.ClientSession, panoid: int
    ) -> dict | None:
        """Retrieve complete details of a panorama"""
        if self.http is None:
            raise RuntimeError("fetch_panorama_details called outside the run() HTTP client")

        # guard() replaces the bare semaphore: same concurrency ceiling, plus
        # the requests-per-hour budget from SCRAPE_JA_REQUESTS_PER_HOUR.
        async with self.http.guard():
            try:
                self.total_requests += 1

                # Add timeout to avoid blocking
                pano = await asyncio.wait_for(
                    find_panorama_by_id_async(panoid, session), timeout=NETWORK_TIMEOUT
                )

                if pano:
                    return self.panorama_to_dict(pano)
                else:
                    return None

            except TimeoutError:
                self.errors += 1
                self.timeouts += 1
                self.log_and_print(f"⏱️ Timeout panorama {panoid}", "warning")
                return None
            except Exception as e:
                self.errors += 1
                self.log_and_print(f"❌ Error panorama {panoid}: {e}", "error")
                return None

    async def find_starting_panorama(self, session: aiohttp.ClientSession) -> int | None:
        """Find the starting panorama in the selected zone"""
        search_radius = self.zone.get("search_radius", SEARCH_RADIUS)
        self.log_and_print(f"🔍 Searching for starting panorama at {self.zone_name}...")
        self.log_and_print(f"📐 Initial search radius: {search_radius}m")

        try:
            # First attempt with normal radius
            pano = await find_panorama_async(
                self.zone["lat"], self.zone["lon"], session, search_radius
            )
            if pano:
                self.log_and_print(
                    f"✅ Starting panorama found: {pano.id} at ({pano.lat:.4f}, {pano.lon:.4f})"
                )
                return pano.id

            # For isolated islands, try with a larger radius
            if self.zone_id in [2, 3]:  # Vestmannaeyjar and Grimsey
                extended_radius = 1000  # 1km
                self.log_and_print(f"🔍 Attempting with extended radius: {extended_radius}m...")
                pano = await find_panorama_async(
                    self.zone["lat"], self.zone["lon"], session, extended_radius
                )
                if pano:
                    self.log_and_print(
                        f"✅ Starting panorama found (extended radius): {pano.id} at ({pano.lat:.4f}, {pano.lon:.4f})"
                    )
                    return pano.id

            self.log_and_print(
                f"❌ No panorama found at {self.zone_name} (tested radii: {search_radius}m", "error"
            )
            if self.zone_id in [2, 3]:
                self.log_and_print(f"   and {extended_radius}m)", "error")
            else:
                self.log_and_print(")", "error")
            return None

        except Exception as e:
            self.log_and_print(f"❌ Initial search error: {e}", "error")
            return None

    def save_batch(self, force=False):
        """Save a batch of panoramas"""
        if len(self.panoramas) >= BATCH_SIZE or force:
            if not self.panoramas:
                return

            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            filename = f"ja360_batch_{timestamp}_{len(self.panoramas)}.json"
            filepath = self.output_dir / filename

            try:
                with open(filepath, "w", encoding="utf-8") as f:
                    json.dump(self.panoramas, f, indent=2, ensure_ascii=False)

                self.log_and_print(f"💾 Batch saved: {len(self.panoramas)} panoramas → {filename}")
                self.panoramas.clear()  # Clear for next batch

            except Exception as e:
                self.log_and_print(f"❌ Save error: {e}", "error")

    def print_stats(self):
        """Display progress statistics with detailed metrics"""
        current_time = time.time()
        elapsed = current_time - self.start_time

        # Overall speed
        overall_speed = len(self.visited) / elapsed if elapsed > 0 else 0

        # Instantaneous speed (since last stats)
        time_since_last = current_time - self.last_stats_time
        collected_since_last = len(self.visited) - self.last_collected_count
        instant_speed = collected_since_last / time_since_last if time_since_last > 0 else 0

        # Success rate
        success_rate = (
            (self.total_requests - self.errors) / self.total_requests * 100
            if self.total_requests > 0
            else 0
        )

        # Remaining time estimation (based on current queue)
        if instant_speed > 0 and len(self.to_visit) > 0:
            eta_seconds = len(self.to_visit) / instant_speed
            eta_minutes = eta_seconds / 60
            eta_str = f" | ETA: {eta_minutes:.1f}min"
        else:
            eta_str = ""

        self.log_and_print(
            f"📊 {len(self.visited):,} panoramas | "
            f"Queue: {len(self.to_visit):,} | "
            f"Speed: {overall_speed:.1f}/s (inst: {instant_speed:.1f}/s) | "
            f"Success: {success_rate:.1f}% | "
            f"Timeouts: {self.timeouts} | Errors: {self.errors}{eta_str} | "
            f"{elapsed:.0f}s"
        )

        # Update metrics for next calculation
        self.last_stats_time = current_time
        self.last_collected_count = len(self.visited)

    def print_periodic_status(self):
        """Display periodic status according to MONITORING_INTERVAL"""
        current_time = time.time()
        elapsed = current_time - self.start_time

        # Overall speed
        overall_speed = len(self.visited) / elapsed if elapsed > 0 else 0

        # Success rate
        success_rate = (
            (self.total_requests - self.errors) / self.total_requests * 100
            if self.total_requests > 0
            else 0
        )

        self.log_and_print(
            f"⏰ [{elapsed:.0f}s] {len(self.visited):,} panoramas collected | "
            f"Speed: {overall_speed:.2f}/s | "
            f"Queue: {len(self.to_visit):,} | "
            f"Success: {success_rate:.1f}% | "
            f"Timeouts: {self.timeouts} | "
            f"Requests: {self.total_requests}"
        )

        # Update timestamp of last log
        self.last_periodic_log_time = current_time

    async def scrape(self):
        """Main scraping with optimized BFS"""
        # Optimized connector configuration
        # Partial adoption of the shared layer, and worth being precise about
        # what that buys. streetlevel's find_panorama_async makes the request
        # itself, so it cannot go through HttpClient.get_bytes the way Naver
        # does. What it does get is our session — User-Agent, headers, timeout —
        # plus the request budget and concurrency ceiling via `guard()`.
        #
        # It does NOT get retries, backoff or proxy rotation: those live in the
        # request path streetlevel bypasses. For a proxy here, set
        # SCRAPE_JA_TRUST_ENV=1 and HTTPS_PROXY, which gives one static proxy
        # rather than a rotating pool.
        self.http_policy = HttpPolicy.from_env(
            "SCRAPE_JA",
            max_concurrency=MAX_CONCURRENT,
            timeout=float(NETWORK_TIMEOUT + 5),
            trust_env=os.getenv("SCRAPE_JA_TRUST_ENV", "") not in ("", "0"),
        )

        async with HttpClient(self.http_policy) as http:
            self.http = http
            session = http.session

            # 1. Find the starting panorama
            start_id = await self.find_starting_panorama(session)
            if not start_id:
                return

            # 2. Initialize BFS queue
            self.to_visit.append(start_id)

            self.log_and_print("\n🚀 Starting BFS scraping...")

            # 3. Main BFS
            while self.to_visit:
                # Prepare batch of panoramas to process
                current_batch = []
                batch_size = min(MAX_CONCURRENT, len(self.to_visit))

                for _ in range(batch_size):
                    if self.to_visit:
                        panoid = self.to_visit.popleft()
                        if panoid not in self.visited:
                            current_batch.append(panoid)
                            self.visited.add(panoid)

                if not current_batch:
                    break

                # Parallel batch processing
                tasks = [self.fetch_panorama_details(session, panoid) for panoid in current_batch]

                results = await asyncio.gather(*tasks, return_exceptions=True)

                # Process results
                new_neighbors = 0
                successful_in_batch = 0

                for panoid, result in zip(current_batch, results, strict=False):
                    if isinstance(result, Exception):
                        continue

                    if result:
                        successful_in_batch += 1
                        # Store the panorama
                        self.panoramas[panoid] = result

                        # Add neighbors to queue
                        for neighbor_id in result.get("neighbor_ids", []):
                            if neighbor_id not in self.visited and neighbor_id not in self.to_visit:
                                self.to_visit.append(neighbor_id)
                                new_neighbors += 1

                # Periodic save
                self.save_batch()

                # Time-based periodic logs
                current_time = time.time()
                if current_time - self.last_periodic_log_time >= MONITORING_INTERVAL:
                    self.print_periodic_status()

                # Detailed periodic statistics (more frequent)
                if len(self.visited) % PROGRESS_INTERVAL == 0 and len(self.visited) > 0:
                    self.print_stats()
                    self.log_and_print(f"   🆕 {new_neighbors} new neighbors discovered")

                # Small delay between batches to avoid overloading the server
                if BATCH_DELAY > 0:
                    await asyncio.sleep(BATCH_DELAY)

                # Safety limit (optional - disabled for complete collection)
                # if len(self.visited) > 50000:  # Adjust according to your needs
                #     print(f"⚠️ Safety limit reached: {len(self.visited)} panoramas")
                #     break

        # Final save
        self.save_batch(force=True)

        # Final statistics
        total_time = time.time() - self.start_time
        avg_speed = len(self.visited) / total_time if total_time > 0 else 0

        self.log_and_print("\n🎉 Scraping completed!")
        self.log_and_print(f"📊 {len(self.visited):,} panoramas collected")
        self.log_and_print(f"⏱️ Total time: {total_time:.1f}s")
        self.log_and_print(f"⚡ Average speed: {avg_speed:.1f} panoramas/second")
        self.log_and_print(f"🌐 Total requests: {self.total_requests}")
        self.log_and_print(f"❌ Errors: {self.errors}")
        self.log_and_print(f"📁 Data saved in: {self.output_dir}")

        # Save final statistics with detailed metrics
        stats = {
            "zone_id": self.zone_id,
            "zone_name": self.zone_name,
            "zone_description": self.zone["description"],
            "total_panoramas": len(self.visited),
            "total_time": total_time,
            "avg_speed": avg_speed,
            "total_requests": self.total_requests,
            "errors": self.errors,
            "timeouts": self.timeouts,
            "success_rate": (self.total_requests - self.errors) / self.total_requests * 100
            if self.total_requests > 0
            else 0,
            "start_coordinates": [self.zone["lat"], self.zone["lon"]],
            "completion_time": datetime.now().isoformat(),
            "config": {
                "max_concurrent": MAX_CONCURRENT,
                "network_timeout": NETWORK_TIMEOUT,
                "batch_size": BATCH_SIZE,
                "progress_interval": PROGRESS_INTERVAL,
            },
        }

        stats_file = self.output_dir / "scraping_stats.json"
        with open(stats_file, "w") as f:
            json.dump(stats, f, indent=2)

        self.log_and_print(f"📊 Statistics saved: {stats_file}")
        self.log_and_print(f"📝 Complete log available: {self.log_path}")


def parse_arguments():
    """Parse command line arguments"""
    parser = argparse.ArgumentParser(
        description="JA360 Panorama Scraper for Iceland - Automatically scrape all zones",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
The script automatically scrapes the 3 zones of Iceland:
  1 - Reykjavik (Capital and main island of Iceland)
  2 - Vestmannaeyjar (Vestmann Islands, south of Iceland)
  3 - Grimsey (Grimsey Island, north of Iceland, Arctic Circle)

Usage:
  python collector.py    # Automatically scrape all zones
        """,
    )

    return parser.parse_args()


async def main():
    """Main function - Automatically scrape all zones"""
    try:
        # Parse arguments (just for help)
        parse_arguments()

        print("\n🇮🇸 JA360 Panorama Scraper - Pipeline Mode")
        print(f"📁 Data saved in: {OUTPUT_DIR}")
        print("🎯 Automatic scraping of 3 Iceland zones")

        # Scrape all zones sequentially
        total_panoramas = 0
        start_time = time.time()

        for zone_id in [1, 2, 3]:  # Reykjavik, Vestmannaeyjar, Grimsey
            zone = ZONES[zone_id]
            print(f"\n{'=' * 50}")
            print(f"🚀 Starting scraping: {zone['name']}")
            print(f"📍 {zone['description']}")
            print(f"📍 Coordinates: {zone['lat']}, {zone['lon']}")
            print(f"{'=' * 50}")

            try:
                scraper = JA360Scraper(zone_id)
                await scraper.scrape()
                total_panoramas += len(scraper.visited)
                print(f"✅ {zone['name']} completed: {len(scraper.visited):,} panoramas collected")

            except Exception as e:
                print(f"❌ Error during scraping of {zone['name']}: {e}")
                import traceback

                traceback.print_exc()
                # Continue with other zones even in case of error
                continue

        # Global final statistics
        total_time = time.time() - start_time
        print(f"\n{'=' * 60}")
        print("🎉 COMPLETE ICELAND SCRAPING FINISHED!")
        print(f"📊 Total panoramas collected: {total_panoramas:,}")
        print(f"⏱️ Total time: {total_time:.1f}s ({total_time / 60:.1f} minutes)")
        print(f"⚡ Global average speed: {total_panoramas / total_time:.1f} panoramas/second")
        print(f"📁 All data is in: {OUTPUT_DIR}")
        print(f"{'=' * 60}")

    except KeyboardInterrupt:
        print("\n⚠️ User interruption")
        sys.exit(1)
    except Exception as e:
        print(f"\n❌ Global error: {e}")
        import traceback

        traceback.print_exc()
        sys.exit(1)


if __name__ == "__main__":
    asyncio.run(main())

#!/usr/bin/env python3
"""
Mapy.cz Panorama Collector - Streamlined BFS Collection

Efficient collection of all Mapy.cz panoramas starting from Prague using BFS traversal.
Designed for long-running collection (multiple days) with automatic checkpoint recovery.

Features:
- Automatic checkpoint and resume functionality
- Memory-efficient batch processing
- Robust error handling and logging
- Parquet output for data consistency
- No rate limiting needed (API handles it)

Output: data/mapy/raw/batch_TIMESTAMP_COUNT.parquet files
"""

import asyncio
import gc
import json
import logging
import os
import signal
import sys
import time
import traceback
from collections import deque
from datetime import datetime
from pathlib import Path
from typing import Any

import aiohttp

# Add streetlevel to path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "../../"))

# Import for Parquet writing
import pyarrow as pa
import pyarrow.parquet as pq
from streetlevel.mapy import find_panorama_async, find_panorama_by_id_async

# Configuration
START_LATITUDE = 50.0755  # Prague center
START_LONGITUDE = 14.4378
SEARCH_RADIUS = 100.0

BATCH_SIZE = 50000
CHECKPOINT_INTERVAL = 2000
MAX_CONCURRENT = 1000
NETWORK_TIMEOUT = 15.0

# Directories
BASE_DIR = Path("../../data/mapy")
RAW_DIR = BASE_DIR / "raw"
CHECKPOINT_DIR = BASE_DIR / "checkpoints"

# Checkpoint files
VISITED_FILE = CHECKPOINT_DIR / "visited_ids.json"
QUEUE_FILE = CHECKPOINT_DIR / "bfs_queue.json"
STATS_FILE = CHECKPOINT_DIR / "collection_stats.json"
CURRENT_BATCH_FILE = CHECKPOINT_DIR / "current_batch.json"


class MapyCollector:
    """Efficient Mapy.cz panorama collector with checkpointing"""

    def __init__(self):
        # Create directories
        RAW_DIR.mkdir(parents=True, exist_ok=True)
        CHECKPOINT_DIR.mkdir(parents=True, exist_ok=True)

        # Collection state
        self.visited_ids: set[int] = set()
        self.bfs_queue: deque = deque()
        self.current_batch: list[dict] = []

        # Statistics
        self.stats = {
            "start_time": time.time(),
            "total_collected": 0,
            "total_requests": 0,
            "errors": 0,
            "timeouts": 0,
            "batches_saved": 0,
            "last_checkpoint": None,
            "current_session_collected": 0,
        }

        # Concurrency control
        self.semaphore = asyncio.Semaphore(MAX_CONCURRENT)
        self.running = True

        # Setup logging
        self.setup_logging()

        # Setup signal handlers for graceful shutdown
        self.setup_signal_handlers()

        # Load checkpoint if exists
        self.load_checkpoint()

        self.logger.info("🚀 Mapy.cz Collector initialized")
        self.logger.info(f"📁 Output directory: {RAW_DIR}")
        self.logger.info(f"💾 Checkpoint directory: {CHECKPOINT_DIR}")
        self.logger.info(f"⚡ Max concurrent requests: {MAX_CONCURRENT}")
        self.logger.info(f"📦 Batch size: {BATCH_SIZE}")

        if self.visited_ids:
            self.logger.info(
                f"🔄 Resuming from checkpoint: {len(self.visited_ids):,} already visited"
            )
            self.logger.info(f"📋 Queue size: {len(self.bfs_queue):,} panoramas")

    def setup_logging(self):
        """Setup comprehensive logging"""
        self.logger = logging.getLogger("MapyCollector")
        self.logger.setLevel(logging.INFO)

        # Clear existing handlers
        self.logger.handlers.clear()

        # File handler
        log_file = CHECKPOINT_DIR / f"collection_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log"
        file_handler = logging.FileHandler(log_file, encoding="utf-8")
        file_handler.setLevel(logging.DEBUG)
        file_formatter = logging.Formatter(
            "%(asctime)s - %(levelname)s - %(message)s", datefmt="%Y-%m-%d %H:%M:%S"
        )
        file_handler.setFormatter(file_formatter)
        self.logger.addHandler(file_handler)

        # Console handler
        console_handler = logging.StreamHandler()
        console_handler.setLevel(logging.INFO)
        console_formatter = logging.Formatter("%(message)s")
        console_handler.setFormatter(console_formatter)
        self.logger.addHandler(console_handler)

        self.logger.info(f"📝 Logging to: {log_file}")

    def setup_signal_handlers(self):
        """Setup signal handlers for graceful shutdown"""

        def signal_handler(signum, frame):
            self.logger.info(f"\n⚠️ Received signal {signum} - Initiating graceful shutdown...")
            self.running = False

        signal.signal(signal.SIGINT, signal_handler)
        signal.signal(signal.SIGTERM, signal_handler)

    def load_checkpoint(self):
        """Load collection state from checkpoint files"""
        try:
            # Load visited IDs
            if VISITED_FILE.exists():
                with open(VISITED_FILE) as f:
                    visited_data = json.load(f)
                    self.visited_ids = set(visited_data)
                self.logger.info(f"✅ Loaded {len(self.visited_ids):,} visited IDs")

            # Load BFS queue
            if QUEUE_FILE.exists():
                with open(QUEUE_FILE) as f:
                    queue_data = json.load(f)
                    self.bfs_queue = deque(queue_data)
                self.logger.info(f"✅ Loaded {len(self.bfs_queue):,} queued IDs")

            # Load statistics
            if STATS_FILE.exists():
                with open(STATS_FILE) as f:
                    saved_stats = json.load(f)
                    # Preserve important stats, reset session stats
                    self.stats.update(
                        {
                            "total_collected": saved_stats.get("total_collected", 0),
                            "batches_saved": saved_stats.get("batches_saved", 0),
                            "last_checkpoint": saved_stats.get("last_checkpoint"),
                        }
                    )
                self.logger.info(
                    f"✅ Loaded stats: {self.stats['total_collected']:,} total collected"
                )

            # Load current batch if exists
            if CURRENT_BATCH_FILE.exists():
                with open(CURRENT_BATCH_FILE) as f:
                    self.current_batch = json.load(f)
                if self.current_batch:
                    self.logger.info(
                        f"✅ Restored {len(self.current_batch)} items in current batch"
                    )

        except Exception as e:
            self.logger.warning(f"⚠️ Error loading checkpoint: {e}")
            # Continue with fresh state if checkpoint is corrupted

    def save_checkpoint(self):
        """Save current collection state to checkpoint files"""
        try:
            # Save visited IDs (convert set to list for JSON)
            with open(VISITED_FILE, "w") as f:
                json.dump(list(self.visited_ids), f)

            # Save BFS queue
            with open(QUEUE_FILE, "w") as f:
                json.dump(list(self.bfs_queue), f)

            # Update and save statistics
            self.stats["last_checkpoint"] = datetime.now().isoformat()
            with open(STATS_FILE, "w") as f:
                json.dump(self.stats, f, indent=2, default=str)

            # Save current batch
            with open(CURRENT_BATCH_FILE, "w") as f:
                json.dump(self.current_batch, f)

            self.logger.debug("💾 Checkpoint saved")

        except Exception as e:
            self.logger.error(f"❌ Error saving checkpoint: {e}")

    def panorama_to_dict(self, pano, collection_order: int) -> dict[str, Any]:
        """Convert panorama object to dictionary for storage"""
        try:
            data = {
                "id": pano.id,
                "lat": pano.lat,
                "lon": pano.lon,
                "date": pano.date.isoformat()
                if hasattr(pano.date, "isoformat")
                else str(pano.date),
                "provider": pano.provider,
                "elevation": pano.elevation,
                "collection_order": collection_order,
                "collection_time": time.time(),
            }

            # Add permalink if available
            if hasattr(pano, "permalink"):
                try:
                    data["permalink"] = pano.permalink()
                except Exception:
                    data["permalink"] = None

            # Add navigation links
            if pano.links:
                data["links_count"] = len(pano.links)
                data["link_ids"] = [link.pano.id for link in pano.links if hasattr(link, "pano")]
            else:
                data["links_count"] = 0
                data["link_ids"] = []

            # We explicitly don't collect historical data for performance
            data["historical_count"] = 0
            data["historical"] = []

            return data

        except Exception as e:
            self.logger.error(f"❌ Error converting panorama {pano.id}: {e}")
            return None

    async def fetch_panorama_by_id(self, panoid: int) -> object | None:
        """Fetch a single panorama by ID with error handling"""
        async with self.semaphore:
            try:
                self.stats["total_requests"] += 1

                # Fetch panorama without historical data for performance
                pano = await asyncio.wait_for(
                    find_panorama_by_id_async(
                        panoid,
                        links=True,  # We need links for BFS traversal
                        historical=False,  # Skip historical data for performance
                    ),
                    timeout=NETWORK_TIMEOUT,
                )

                return pano

            except TimeoutError:
                self.stats["timeouts"] += 1
                self.logger.debug(f"⏱️ Timeout for panorama {panoid}")
                return None
            except Exception as e:
                self.stats["errors"] += 1
                self.logger.debug(f"❌ Error fetching panorama {panoid}: {e}")
                return None

    async def find_starting_panorama(self) -> int | None:
        """Find starting panorama in Prague if no checkpoint exists"""
        if self.bfs_queue:
            return None  # We already have a queue from checkpoint

        self.logger.info("🔍 Searching for starting panorama in Prague...")

        try:
            async with aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=NETWORK_TIMEOUT)):
                pano = await find_panorama_async(
                    START_LATITUDE,
                    START_LONGITUDE,
                    radius=SEARCH_RADIUS,
                    links=True,
                    historical=False,
                )

                if pano:
                    self.logger.info(
                        f"✅ Starting panorama found: {pano.id} at ({pano.lat:.4f}, {pano.lon:.4f})"
                    )
                    return pano.id
                else:
                    self.logger.error("❌ No panorama found at Prague coordinates")
                    return None

        except Exception as e:
            self.logger.error(f"❌ Error finding starting panorama: {e}")
            return None

    async def save_batch_to_parquet(self, force=False):
        """Save current batch to Parquet file"""
        if len(self.current_batch) >= BATCH_SIZE or (force and self.current_batch):
            if not self.current_batch:
                return

            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            batch_count = len(self.current_batch)
            filename = f"mapy_batch_{timestamp}_{batch_count:06d}.parquet"
            filepath = RAW_DIR / filename

            try:
                # Convert to PyArrow table and save
                table = pa.Table.from_pylist(self.current_batch)
                pq.write_table(
                    table,
                    filepath,
                    compression="snappy",
                    use_dictionary=True,
                    write_statistics=True,
                )

                self.stats["batches_saved"] += 1
                self.logger.info(f"💾 Batch saved: {batch_count:,} panoramas → {filename}")

                # Clear current batch and force garbage collection
                self.current_batch.clear()
                gc.collect()

            except Exception as e:
                self.logger.error(f"❌ Error saving batch: {e}")

    def print_progress(self):
        """Print current collection progress"""
        elapsed = time.time() - self.stats["start_time"]
        total_collected = self.stats["total_collected"]
        session_collected = self.stats["current_session_collected"]

        # Calculate speeds
        if elapsed > 0:
            session_speed = session_collected / elapsed
        else:
            session_speed = 0

        # Network statistics
        if self.stats["total_requests"] > 0:
            success_rate = (
                (self.stats["total_requests"] - self.stats["errors"] - self.stats["timeouts"])
                / self.stats["total_requests"]
                * 100
            )
        else:
            success_rate = 0

        self.logger.info(
            f"📊 Progress: {total_collected:,} total | {session_collected:,} this session | "
            f"Queue: {len(self.bfs_queue):,} | Speed: {session_speed:.1f}/s | "
            f"Success: {success_rate:.1f}% | Batches: {self.stats['batches_saved']} | "
            f"Time: {elapsed / 3600:.1f}h"
        )

    async def collect_panoramas(self):
        """Main collection loop using BFS traversal"""
        self.logger.info("\n🌐 Starting panorama collection...")

        # Initialize starting point if needed
        if not self.bfs_queue:
            start_id = await self.find_starting_panorama()
            if not start_id:
                self.logger.error("❌ Could not find starting panorama")
                return
            self.bfs_queue.append(start_id)

        # Main BFS collection loop
        processed_count = 0

        async with aiohttp.ClientSession(
            timeout=aiohttp.ClientTimeout(total=NETWORK_TIMEOUT),
            connector=aiohttp.TCPConnector(limit=MAX_CONCURRENT),
        ):
            while self.bfs_queue and self.running:
                # Process batch of panoramas
                batch_tasks = []
                batch_ids = []

                # Create batch of concurrent requests
                batch_size = min(MAX_CONCURRENT, len(self.bfs_queue))
                for _ in range(batch_size):
                    if self.bfs_queue:
                        panoid = self.bfs_queue.popleft()
                        if panoid not in self.visited_ids:
                            batch_ids.append(panoid)
                            batch_tasks.append(self.fetch_panorama_by_id(panoid))
                            self.visited_ids.add(panoid)

                if not batch_tasks:
                    break

                # Execute batch requests
                results = await asyncio.gather(*batch_tasks, return_exceptions=True)

                # Process results
                for _panoid, result in zip(batch_ids, results, strict=False):
                    processed_count += 1

                    if isinstance(result, Exception) or not result:
                        continue

                    # Convert panorama to dictionary
                    pano_dict = self.panorama_to_dict(result, self.stats["total_collected"] + 1)
                    if not pano_dict:
                        continue

                    # Add to current batch
                    self.current_batch.append(pano_dict)
                    self.stats["total_collected"] += 1
                    self.stats["current_session_collected"] += 1

                    # Add linked panoramas to BFS queue
                    if result.links:
                        for link in result.links:
                            if hasattr(link, "pano") and link.pano.id not in self.visited_ids:
                                self.bfs_queue.append(link.pano.id)

                    # Save batch if it's full
                    await self.save_batch_to_parquet()

                    # Periodic checkpoint and progress display
                    if processed_count % CHECKPOINT_INTERVAL == 0:
                        self.save_checkpoint()
                        self.print_progress()

        # Final operations
        await self.save_batch_to_parquet(force=True)
        self.save_checkpoint()

        # Final statistics
        total_time = time.time() - self.stats["start_time"]
        final_speed = self.stats["current_session_collected"] / total_time if total_time > 0 else 0

        self.logger.info("\n🎉 Collection completed!")
        self.logger.info("📊 Final statistics:")
        self.logger.info(f"   • Total collected: {self.stats['total_collected']:,} panoramas")
        self.logger.info(
            f"   • This session: {self.stats['current_session_collected']:,} panoramas"
        )
        self.logger.info(f"   • Total time: {total_time / 3600:.1f} hours")
        self.logger.info(f"   • Average speed: {final_speed:.1f} panoramas/second")
        self.logger.info(f"   • Batches saved: {self.stats['batches_saved']}")
        self.logger.info(f"   • Network requests: {self.stats['total_requests']:,}")
        self.logger.info(
            f"   • Success rate: {((self.stats['total_requests'] - self.stats['errors'] - self.stats['timeouts']) / self.stats['total_requests'] * 100):.1f}%"
        )
        self.logger.info(f"   • Files saved to: {RAW_DIR}")


async def main():
    """Main entry point"""
    try:
        collector = MapyCollector()
        await collector.collect_panoramas()

    except KeyboardInterrupt:
        print("\n⚠️ Collection interrupted by user")
        sys.exit(1)
    except Exception as e:
        print(f"\n❌ Error: {e}")
        traceback.print_exc()
        sys.exit(1)


if __name__ == "__main__":
    # Set event loop policy for better performance
    if sys.platform.startswith("linux"):
        try:
            import uvloop

            uvloop.install()
            print("🚀 Using uvloop for better performance")
        except ImportError:
            pass

    # Run the collector
    asyncio.run(main())

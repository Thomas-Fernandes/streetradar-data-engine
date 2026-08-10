#!/usr/bin/env python3
"""
Naver Maps Tile Scraper

This script scrapes panorama tiles from Naver Maps API for a given zoom level,
filtering tiles to only include those within the Korean peninsula boundaries.

The script:
- Reads boundary polygon from boundings.geojson
- Calculates which tiles intersect with the boundary
- Downloads tiles in PBF format from Naver Maps API
- Saves tiles in z/x/y.pbf structure
- Manages sessions automatically (incremental session numbers)
- Provides comprehensive logging and progress tracking

Input:  boundings.geojson (polygon defining collection area)
Output: data/naver/raw/{session}/z/x/y.pbf (tile data)
        data/naver/raw/{session}/naver_scraping.log (detailed logs)

Usage:
    python naver_scraper.py [--zoom ZOOM_LEVEL]
"""

import argparse
import asyncio
import json
import logging
import math
import os
import sys
import time
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path

import aiofiles

from streetradar.http import HttpClient, HttpError, HttpPolicy

# Add the current directory to the path to import constants
sys.path.insert(0, os.path.dirname(__file__))

from constants import (
    BASE_DATA_DIR,
    BOUNDS_GEOJSON_FILE,
    CONSOLE_LOG_LEVEL,
    DEFAULT_HEADERS,
    DEFAULT_ZOOM_LEVEL,
    EMOJI_DOWNLOAD,
    EMOJI_ERROR,
    EMOJI_INFO,
    EMOJI_MAP,
    EMOJI_PROGRESS,
    EMOJI_SAVE,
    EMOJI_START,
    EMOJI_SUCCESS,
    EMOJI_TILE,
    EMOJI_WARNING,
    FILE_LOG_LEVEL,
    LOG_DATE_FORMAT,
    LOG_FORMAT,
    MAX_CONCURRENT_REQUESTS,
    MAX_RETRIES,
    NAVER_ENDPOINT_TEMPLATE,
    PROGRESS_REPORT_INTERVAL,
    RAW_DATA_DIR,
    REQUEST_TIMEOUT,
    SESSION_TRACKER_FILE,
    get_log_file_path,
    get_session_dir,
    get_tile_file_path,
)


@dataclass(frozen=True)
class TileCoordinate:
    """Represents a tile coordinate with x, y, and zoom level."""

    x: int
    y: int
    z: int

    def __str__(self) -> str:
        return f"{self.z}/{self.x}/{self.y}"

    def __hash__(self) -> int:
        return hash((self.x, self.y, self.z))


class SessionManager:
    """
    Manages scraping sessions with automatic session numbering.

    Each session gets a unique incremental number (1, 2, 3, ...) and
    its own directory structure for organizing collected data.
    """

    def __init__(self, base_dir: str = BASE_DATA_DIR):
        """
        Initialize the session manager.

        Args:
            base_dir: Base directory for all data storage
        """
        self.base_dir = Path(base_dir)
        self.raw_dir = self.base_dir / RAW_DATA_DIR
        self.session_file = self.base_dir / SESSION_TRACKER_FILE

        # Create base directories
        self.base_dir.mkdir(parents=True, exist_ok=True)
        self.raw_dir.mkdir(parents=True, exist_ok=True)

    def get_next_session_number(self) -> int:
        """
        Determine the next available session number.

        Returns:
            Next session number (1-based)
        """
        # Check existing session directories
        existing_sessions = []
        if self.raw_dir.exists():
            for item in self.raw_dir.iterdir():
                if item.is_dir() and item.name.isdigit():
                    existing_sessions.append(int(item.name))

        # Return next number
        return max(existing_sessions, default=0) + 1

    def create_session(self, session_number: int, zoom_level: int) -> Path:
        """
        Create a new session directory and metadata.

        Args:
            session_number: The session number
            zoom_level: Zoom level for this session

        Returns:
            Path to the session directory
        """
        session_dir = get_session_dir(session_number)
        session_dir.mkdir(parents=True, exist_ok=True)

        # Create zoom subdirectory
        zoom_dir = session_dir / str(zoom_level)
        zoom_dir.mkdir(parents=True, exist_ok=True)

        # Save session metadata
        session_info = {
            "session_number": session_number,
            "zoom_level": zoom_level,
            "created_at": datetime.now().isoformat(),
            "status": "active",
        }

        with open(self.session_file, "w") as f:
            json.dump(session_info, f, indent=2)

        return session_dir


class BoundaryProcessor:
    """
    Processes GeoJSON boundary files and calculates tile intersections.

    This class handles reading the Korean boundary polygon and determining
    which tiles at a given zoom level intersect with the boundary.
    """

    def __init__(self, geojson_file: str):
        """
        Initialize the boundary processor.

        Args:
            geojson_file: Path to the GeoJSON boundary file
        """
        self.geojson_file = Path(geojson_file)
        self.boundary_coords = None

    def load_boundary(self) -> list[tuple[float, float]]:
        """
        Load boundary coordinates from GeoJSON file.

        Returns:
            List of (longitude, latitude) coordinate pairs

        Raises:
            FileNotFoundError: If GeoJSON file doesn't exist
            ValueError: If GeoJSON format is invalid
        """
        if not self.geojson_file.exists():
            raise FileNotFoundError(f"Boundary file not found: {self.geojson_file}")

        try:
            with open(self.geojson_file, encoding="utf-8") as f:
                geojson_data = json.load(f)

            # Extract coordinates from the first feature's polygon
            features = geojson_data.get("features", [])
            if not features:
                raise ValueError("No features found in GeoJSON")

            geometry = features[0].get("geometry", {})
            if geometry.get("type") != "Polygon":
                raise ValueError("First feature is not a Polygon")

            # Get the exterior ring coordinates
            coordinates = geometry.get("coordinates", [[]])[0]
            self.boundary_coords = [(lon, lat) for lon, lat in coordinates]

            return self.boundary_coords

        except (json.JSONDecodeError, KeyError, IndexError) as e:
            raise ValueError(f"Invalid GeoJSON format: {e}") from e

    @staticmethod
    def deg2num(lat_deg: float, lon_deg: float, zoom: int) -> tuple[int, int]:
        """
        Convert latitude/longitude to tile coordinates.

        Args:
            lat_deg: Latitude in degrees
            lon_deg: Longitude in degrees
            zoom: Zoom level

        Returns:
            Tuple of (x, y) tile coordinates
        """
        lat_rad = math.radians(lat_deg)
        n = 2.0**zoom
        x = int((lon_deg + 180.0) / 360.0 * n)
        y = int((1.0 - math.asinh(math.tan(lat_rad)) / math.pi) / 2.0 * n)
        return x, y

    @staticmethod
    def num2deg(x: int, y: int, zoom: int) -> tuple[float, float]:
        """
        Convert tile coordinates to latitude/longitude.

        Args:
            x: Tile X coordinate
            y: Tile Y coordinate
            zoom: Zoom level

        Returns:
            Tuple of (latitude, longitude) in degrees
        """
        n = 2.0**zoom
        lon_deg = x / n * 360.0 - 180.0
        lat_rad = math.atan(math.sinh(math.pi * (1 - 2 * y / n)))
        lat_deg = math.degrees(lat_rad)
        return lat_deg, lon_deg

    def point_in_polygon(
        self, point: tuple[float, float], polygon: list[tuple[float, float]]
    ) -> bool:
        """
        Test if a point is inside a polygon using ray casting algorithm.

        Args:
            point: (longitude, latitude) point to test
            polygon: List of (longitude, latitude) polygon vertices

        Returns:
            True if point is inside polygon, False otherwise
        """
        x, y = point
        n = len(polygon)
        inside = False

        p1x, p1y = polygon[0]
        for i in range(1, n + 1):
            p2x, p2y = polygon[i % n]
            if y > min(p1y, p2y):
                if y <= max(p1y, p2y):
                    if x <= max(p1x, p2x):
                        if p1y != p2y:
                            xinters = (y - p1y) * (p2x - p1x) / (p2y - p1y) + p1x
                        if p1x == p2x or x <= xinters:
                            inside = not inside
            p1x, p1y = p2x, p2y

        return inside

    def get_tiles_in_boundary(self, zoom: int) -> set[TileCoordinate]:
        """
        Get all tiles that intersect with the boundary polygon.

        Args:
            zoom: Zoom level to calculate tiles for

        Returns:
            Set of TileCoordinate objects within the boundary
        """
        if not self.boundary_coords:
            self.load_boundary()

        # Calculate bounding box of the polygon
        min_lon = min(coord[0] for coord in self.boundary_coords)
        max_lon = max(coord[0] for coord in self.boundary_coords)
        min_lat = min(coord[1] for coord in self.boundary_coords)
        max_lat = max(coord[1] for coord in self.boundary_coords)

        # Convert bounding box to tile coordinates
        min_x, max_y = self.deg2num(min_lat, min_lon, zoom)
        max_x, min_y = self.deg2num(max_lat, max_lon, zoom)

        tiles_in_boundary = set()

        # Check each tile in the bounding box
        for x in range(min_x, max_x + 1):
            for y in range(min_y, max_y + 1):
                # Get tile center coordinates
                lat, lon = self.num2deg(x, y, zoom)

                # Test if tile center is within polygon
                if self.point_in_polygon((lon, lat), self.boundary_coords):
                    tiles_in_boundary.add(TileCoordinate(x, y, zoom))

        return tiles_in_boundary


class NaverTileScraper:
    """
    Main scraper class for downloading Naver Maps tiles.

    This class handles the downloading of panorama tiles from Naver Maps API,
    with support for concurrent requests, error handling, and progress tracking.
    """

    def __init__(self, session_dir: Path, zoom_level: int):
        """
        Initialize the tile scraper.

        Args:
            session_dir: Directory for this scraping session
            zoom_level: Zoom level to scrape
        """
        self.session_dir = session_dir
        self.zoom_level = zoom_level
        self.logger = self._setup_logging()

        # Counters and timestamps used to share one dict, which forced every
        # entry to `int | None` and made each `+= 1` a type error. Splitting
        # them keeps the counters plainly integers.
        self.stats: dict[str, int] = {
            "total_tiles": 0,
            "downloaded_tiles": 0,
            "failed_tiles": 0,
            "skipped_tiles": 0,
        }
        self.started_at: float = 0.0
        self.ended_at: float = 0.0

        self.failed_requests: list[dict[str, str]] = []

    def _setup_logging(self) -> logging.Logger:
        """
        Set up logging for the scraper.

        Returns:
            Configured logger instance
        """
        logger = logging.getLogger("naver_scraper")
        logger.setLevel(logging.DEBUG)

        # Clear any existing handlers
        logger.handlers.clear()

        # File handler
        log_file = get_log_file_path(
            int(self.session_dir.name),
        )
        file_handler = logging.FileHandler(log_file, encoding="utf-8")
        file_handler.setLevel(getattr(logging, FILE_LOG_LEVEL))
        file_formatter = logging.Formatter(LOG_FORMAT, LOG_DATE_FORMAT)
        file_handler.setFormatter(file_formatter)
        logger.addHandler(file_handler)

        # Console handler
        console_handler = logging.StreamHandler()
        console_handler.setLevel(getattr(logging, CONSOLE_LOG_LEVEL))
        console_formatter = logging.Formatter("%(levelname)s - %(message)s")
        console_handler.setFormatter(console_formatter)
        logger.addHandler(console_handler)

        return logger

    async def download_tile(self, http: HttpClient, tile: TileCoordinate) -> bool:
        """
        Download a single tile from Naver Maps API.

        Retries, backoff, proxy rotation and the request budget all live in the
        shared HTTP layer, so what is left here is the tile-specific part: where
        the file goes and what an absent tile means.

        Args:
            http: shared client carrying this provider's rate-limit policy
            tile: Tile coordinate to download

        Returns:
            True if the tile was stored or is legitimately absent, False on error
        """
        url = NAVER_ENDPOINT_TEMPLATE.format(x=tile.x, y=tile.y, z=tile.z)
        file_path = get_tile_file_path(int(self.session_dir.name), tile.z, tile.x, tile.y)

        # Skip if file already exists
        if file_path.exists():
            self.stats["skipped_tiles"] += 1
            return True

        try:
            # Most coordinates in the bounding box hold no imagery, so a 404 is
            # the common answer rather than a failure.
            payload = await http.get_bytes(url, missing_ok=True)
        except HttpError as exc:
            self.logger.warning(f"Giving up on tile {tile}: {exc}")
            self.stats["failed_tiles"] += 1
            self.failed_requests.append(
                {
                    "tile": str(tile),
                    "url": url,
                    "error": str(exc),
                    "timestamp": datetime.now().isoformat(),
                }
            )
            return False

        if payload is None:
            self.logger.debug(f"Tile {tile} not found (404)")
            self.stats["skipped_tiles"] += 1
            return True

        file_path.parent.mkdir(parents=True, exist_ok=True)
        async with aiofiles.open(file_path, "wb") as f:
            await f.write(payload)

        self.stats["downloaded_tiles"] += 1
        return True

    async def download_tiles(self, tiles: set[TileCoordinate]) -> None:
        """
        Download all tiles concurrently.

        Args:
            tiles: Set of tile coordinates to download
        """
        self.stats["total_tiles"] = len(tiles)
        self.started_at = time.time()

        self.logger.info(
            f"{EMOJI_START} Starting download of {len(tiles):,} tiles at zoom {self.zoom_level}"
        )

        # Concurrency, timeout, retries and the request budget are all policy
        # now. Defaults come from the constants this script has always used;
        # SCRAPE_NAVER_* overrides them without a code change, which is how you
        # back off in production without redeploying.
        #
        # A word on the default: MAX_CONCURRENT_REQUESTS is 1000 against an
        # undocumented endpoint with no request budget. That is a lot of load
        # from one IP. Set SCRAPE_NAVER_MAX_CONCURRENCY and
        # SCRAPE_NAVER_REQUESTS_PER_HOUR before running this anywhere it matters.
        session_headers = {k: v for k, v in DEFAULT_HEADERS.items() if k != "User-Agent"}
        policy = HttpPolicy.from_env(
            "SCRAPE_NAVER",
            max_concurrency=MAX_CONCURRENT_REQUESTS,
            max_retries=MAX_RETRIES,
            timeout=float(REQUEST_TIMEOUT),
            user_agent=DEFAULT_HEADERS["User-Agent"],
            headers=session_headers,
        )

        async with HttpClient(policy) as http:
            self.logger.info(
                f"HTTP policy: concurrency={policy.max_concurrency}, "
                f"budget={policy.requests_per_hour or 'unlimited'}/h, "
                f"proxies={len(policy.proxies)}"
            )

            # Process tiles in batches for progress reporting
            tiles_list = list(tiles)
            completed = 0

            for i in range(0, len(tiles_list), PROGRESS_REPORT_INTERVAL):
                batch = tiles_list[i : i + PROGRESS_REPORT_INTERVAL]

                tasks = [self.download_tile(http, tile) for tile in batch]
                results = await asyncio.gather(*tasks, return_exceptions=True)

                # These used to be collected and dropped, so a batch could fail
                # outright while the run reported progress and exited 0.
                crashed = [r for r in results if isinstance(r, BaseException)]
                if crashed:
                    self.stats["failed_tiles"] += len(crashed)
                    self.logger.error(
                        f"{len(crashed)}/{len(batch)} tiles raised in this batch; "
                        f"first: {crashed[0]!r}"
                    )

                completed += len(batch)

                # Log progress
                progress_percent = (completed / len(tiles_list)) * 100
                elapsed = time.time() - self.started_at
                rate = completed / elapsed if elapsed > 0 else 0

                self.logger.info(
                    f"{EMOJI_PROGRESS} Progress: {completed:,}/{len(tiles_list):,} "
                    f"({progress_percent:.1f}%) - {rate:.1f} tiles/sec - "
                    f"Downloaded: {self.stats['downloaded_tiles']:,}, "
                    f"Skipped: {self.stats['skipped_tiles']:,}, "
                    f"Failed: {self.stats['failed_tiles']:,}"
                )

        self.ended_at = time.time()

        # Save failed requests log
        if self.failed_requests:
            failed_file = self.session_dir / "failed_requests.json"
            with open(failed_file, "w") as f:
                json.dump(self.failed_requests, f, indent=2)

    def print_final_stats(self) -> None:
        """Print final download statistics."""
        elapsed = self.ended_at - self.started_at
        rate = self.stats["total_tiles"] / elapsed if elapsed > 0 else 0

        self.logger.info(f"\n{EMOJI_SUCCESS} Download completed!")
        self.logger.info(f"{EMOJI_MAP} Total tiles processed: {self.stats['total_tiles']:,}")
        self.logger.info(f"{EMOJI_DOWNLOAD} Downloaded: {self.stats['downloaded_tiles']:,}")
        self.logger.info(f"{EMOJI_INFO} Skipped (existing/404): {self.stats['skipped_tiles']:,}")
        self.logger.info(f"{EMOJI_ERROR} Failed: {self.stats['failed_tiles']:,}")
        self.logger.info(f"{EMOJI_TILE} Average rate: {rate:.1f} tiles/second")
        self.logger.info(f"⏱️ Total time: {elapsed:.1f} seconds")

        if self.stats["failed_tiles"] > 0:
            self.logger.warning(
                f"{EMOJI_WARNING} {self.stats['failed_tiles']} tiles failed to download"
            )
            self.logger.info("Check failed_requests.json for details")


async def main():
    """
    Main function to orchestrate the entire scraping process.

    This function:
    1. Parses command line arguments
    2. Sets up session management
    3. Loads boundary polygon
    4. Calculates tiles to download
    5. Performs the download
    6. Reports final statistics
    """
    # Parse command line arguments
    parser = argparse.ArgumentParser(description="Naver Maps Tile Scraper")
    parser.add_argument(
        "--zoom",
        type=int,
        default=DEFAULT_ZOOM_LEVEL,
        help=f"Zoom level to scrape (default: {DEFAULT_ZOOM_LEVEL})",
    )
    args = parser.parse_args()

    zoom_level = args.zoom

    print(f"{EMOJI_START} Naver Maps Tile Scraper")
    print(f"{EMOJI_MAP} Zoom Level: {zoom_level}")
    print("=" * 50)

    try:
        # Setup session management
        session_manager = SessionManager()
        session_number = session_manager.get_next_session_number()
        session_dir = session_manager.create_session(session_number, zoom_level)

        print(f"{EMOJI_SAVE} Session #{session_number} created: {session_dir}")

        # Load boundary polygon
        boundary_processor = BoundaryProcessor(BOUNDS_GEOJSON_FILE)
        print(f"{EMOJI_MAP} Loading boundary from {BOUNDS_GEOJSON_FILE}")
        boundary_coords = boundary_processor.load_boundary()
        print(f"{EMOJI_SUCCESS} Loaded polygon with {len(boundary_coords)} vertices")

        # Calculate tiles within boundary
        print(f"{EMOJI_TILE} Calculating tiles within boundary at zoom {zoom_level}...")
        tiles_to_download = boundary_processor.get_tiles_in_boundary(zoom_level)
        print(f"{EMOJI_SUCCESS} Found {len(tiles_to_download):,} tiles to process")

        # Initialize scraper
        scraper = NaverTileScraper(session_dir, zoom_level)

        # Download tiles
        await scraper.download_tiles(tiles_to_download)

        # Print final statistics
        scraper.print_final_stats()

        print(f"\n{EMOJI_SUCCESS} Scraping completed successfully!")
        print(f"{EMOJI_SAVE} Data saved to: {session_dir}")
        print(f"{EMOJI_INFO} Log file: {get_log_file_path(session_number)}")

    except KeyboardInterrupt:
        print(f"\n{EMOJI_WARNING} Scraping interrupted by user")
        return 1

    except Exception as e:
        print(f"\n{EMOJI_ERROR} Error during scraping: {e}")
        return 1

    return 0


if __name__ == "__main__":
    # Run the async main function
    sys.exit(asyncio.run(main()))

"""
Configuration constants for Naver Maps data collection.
Contains all configurable parameters for the Naver scraping system.
"""

import os
from pathlib import Path

# =============================================================================
# NAVER API CONFIGURATION
# =============================================================================

# Naver Maps API endpoint configuration
NAVER_BASE_URL = "https://map.pstatic.net/nvbpc/wmts/panorama"
NAVER_API_KEY = os.getenv("NAVER_API_KEY", "c9d936c2-23bf-4a91-82fc-5d03e9b6ad01") #default api key
NAVER_ENDPOINT_TEMPLATE = f"{NAVER_BASE_URL}/{NAVER_API_KEY}/getTile/{{x}}/{{y}}/{{z}}/pbf"

# Default zoom level for data collection
DEFAULT_ZOOM_LEVEL = 17

# =============================================================================
# GEOGRAPHICAL BOUNDS
# =============================================================================

# Bounding geometry file (GeoJSON polygon defining Korea boundaries)
BOUNDS_GEOJSON_FILE = "boundings.geojson"

# Tile coordinate bounds (will be calculated from GeoJSON)
# These are approximate bounds for Korea at zoom level 17
TILE_BOUNDS = {
    'min_x': 50000,  # Will be calculated from GeoJSON
    'max_x': 60000,  # Will be calculated from GeoJSON
    'min_y': 30000,  # Will be calculated from GeoJSON
    'max_y': 40000   # Will be calculated from GeoJSON
}

# =============================================================================
# OUTPUT CONFIGURATION
# =============================================================================

# Base directory for all Naver data
BASE_DATA_DIR = "../../data/naver"

# Raw data directory structure
RAW_DATA_DIR = "raw"

# Session-based output (incremental: 1, 2, 3, ...)
# The script will automatically detect the next session number

# File structure: data/naver/raw/{session_number}/z/x/y.pbf
OUTPUT_FILE_PATTERN = "{z}/{x}/{y}.pbf"

# =============================================================================
# LOGGING CONFIGURATION
# =============================================================================

# Log file configuration
LOG_FILENAME = "naver_scraping.log"
LOG_FORMAT = "%(asctime)s - %(levelname)s - %(message)s"
LOG_DATE_FORMAT = "%Y-%m-%d %H:%M:%S"

# Console output configuration
CONSOLE_LOG_LEVEL = "INFO"
FILE_LOG_LEVEL = "DEBUG"

# =============================================================================
# NETWORK CONFIGURATION
# =============================================================================

# HTTP request configuration
REQUEST_TIMEOUT = 3  # seconds
REQUEST_RETRIES = 3
REQUEST_DELAY = 0.000001  # seconds between requests

# User agent for requests
USER_AGENT = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"

# HTTP headers
DEFAULT_HEADERS = {
    'User-Agent': USER_AGENT,
    'Accept': 'application/x-protobuf,*/*',
    'Accept-Language': 'en-US,en;q=0.9',
    'Accept-Encoding': 'gzip, deflate',  # Removed 'br' (brotli) to avoid compression issues
    'Connection': 'keep-alive',
    'Sec-Fetch-Dest': 'empty',
    'Sec-Fetch-Mode': 'cors',
    'Sec-Fetch-Site': 'cross-site',  # Changed from 'same-origin' since we're calling external API
}

# =============================================================================
# PERFORMANCE CONFIGURATION
# =============================================================================

# Concurrent requests
MAX_CONCURRENT_REQUESTS = 1000

# Batch processing
BATCH_SIZE = 10000  # Number of tiles to process in each batch

# Progress reporting
PROGRESS_REPORT_INTERVAL = 10000  # Report progress every N tiles

# =============================================================================
# SESSION MANAGEMENT
# =============================================================================

# Session tracking file
SESSION_TRACKER_FILE = "session_info.json"

# =============================================================================
# ERROR HANDLING
# =============================================================================

# Retry configuration
MAX_RETRIES = 3
RETRY_DELAY = 0.01  # seconds
EXPONENTIAL_BACKOFF = True

# Error logging
LOG_FAILED_REQUESTS = True
FAILED_REQUESTS_FILE = "failed_requests.json"

# =============================================================================
# DISPLAY CONFIGURATION
# =============================================================================

# Progress display emojis
EMOJI_START = "🚀"
EMOJI_PROGRESS = "📊"
EMOJI_SUCCESS = "✅"
EMOJI_ERROR = "❌"
EMOJI_WARNING = "⚠️"
EMOJI_INFO = "ℹ️"
EMOJI_DOWNLOAD = "⬇️"
EMOJI_SAVE = "💾"
EMOJI_MAP = "🗺️"
EMOJI_TILE = "🔲"

# =============================================================================
# UTILITY FUNCTIONS
# =============================================================================

def get_session_dir(session_number: int) -> Path:
    """
    Get the session directory path for a given session number.
    
    Args:
        session_number: The session number (1, 2, 3, ...)
        
    Returns:
        Path object pointing to the session directory
    """
    return Path(BASE_DATA_DIR) / RAW_DATA_DIR / str(session_number)

def get_log_file_path(session_number: int) -> Path:
    """
    Get the log file path for a given session.
    
    Args:
        session_number: The session number
        
    Returns:
        Path object pointing to the log file
    """
    return get_session_dir(session_number) / LOG_FILENAME

def get_tile_file_path(session_number: int, z: int, x: int, y: int) -> Path:
    """
    Get the file path for a specific tile.
    
    Args:
        session_number: The session number
        z: Zoom level
        x: Tile X coordinate
        y: Tile Y coordinate
        
    Returns:
        Path object pointing to the tile file
    """
    session_dir = get_session_dir(session_number)
    return session_dir / OUTPUT_FILE_PATTERN.format(z=z, x=x, y=y)
"""
Configuration settings for Mapy.cz data collection script.
All modifiable constants and parameters are centralized here.
"""

# =============================================================================
# GEOGRAPHICAL CONSTANTS
# =============================================================================
# Default starting location (Prague)
DEFAULT_START_LATITUDE = 50.0755
DEFAULT_START_LONGITUDE = 14.4378

# Search parameters
DEFAULT_SEARCH_RADIUS = 100.0  # meters

# =============================================================================
# COLLECTION PARAMETERS
# =============================================================================
# Target number of panoramas to collect
DEFAULT_TARGET_COUNT = 10000

# Output directory
DEFAULT_OUTPUT_DIR = "raw"

# =============================================================================
# CONCURRENCY AND PERFORMANCE
# =============================================================================
# Maximum concurrent connections per proxy
DEFAULT_MAX_CONCURRENT_PER_PROXY = 120
ULTRA_MAX_CONCURRENT_PER_PROXY = 300

# Batch size multiplier for proxy saturation
BATCH_SIZE_MULTIPLIER = 2

# Thread pool workers for file writing
FILE_WRITER_MAX_WORKERS = 1

# =============================================================================
# NETWORK TIMEOUTS AND RETRIES
# =============================================================================
# HTTP timeouts (seconds)
HTTP_TOTAL_TIMEOUT = 5
HTTP_CONNECT_TIMEOUT = 3
PANORAMA_FETCH_TIMEOUT = 4.0

# TCP connection timeouts (seconds)
TCP_TTL_DNS_CACHE = 3600
TCP_KEEPALIVE_TIMEOUT = 120

# API timeouts (seconds)
API_TOTAL_TIMEOUT = 15
API_CONNECT_TIMEOUT = 5

# =============================================================================
# HTTP HEADERS AND USER AGENTS
# =============================================================================
# Standard browser user agent (non-traceable)
USER_AGENT_BROWSER = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/138.0.0.0 Safari/537.36"

# Common HTTP headers
HTTP_HEADERS = {
    'Connection': 'keep-alive',
    'Accept-Encoding': 'gzip, deflate'
}

# Mapy.cz specific headers
MAPY_REFERER = "https://en.mapy.cz/"

# =============================================================================
# CACHING AND OPTIMIZATION
# =============================================================================
# Cache sizes
FAILED_CACHE_MAX_SIZE = 20000

# Progress reporting intervals
PROGRESS_REPORT_INTERVAL = 10  # BFS iterations
CACHE_CLEANUP_INTERVAL = 500   # BFS iterations

# Python optimization
SWITCH_INTERVAL = 0.005  # seconds

# =============================================================================
# FILE OUTPUT SETTINGS
# =============================================================================
# Parquet compression
PARQUET_COMPRESSION = 'snappy'
PARQUET_USE_DICTIONARY = False
PARQUET_WRITE_STATISTICS = False

# Output filename pattern
OUTPUT_FILENAME_PATTERN = "mapy_proxy_optimized_{timestamp}.parquet"
TIMESTAMP_FORMAT = "%Y%m%d_%H%M%S"

# =============================================================================
# PROXY CONFIGURATION
# =============================================================================
# Proxy URL format
PROXY_URL_FORMAT = "http://{user}:{pwd}@{ip}:{port}"

# Proxy statistics initialization
PROXY_STATS_INIT = {'requests': 0, 'errors': 0, 'last_used': 0}

# =============================================================================
# BENCHMARK AND COMPARISON
# =============================================================================
# Original script speed for comparison (panoramas/second)
ORIGINAL_SCRIPT_SPEED = 82

# =============================================================================
# LOGGING AND DISPLAY
# =============================================================================
# Console display emojis and symbols
EMOJI_GLOBE = "🌐"
EMOJI_ROCKET = "🚀"
EMOJI_TARGET = "🎯"
EMOJI_LIGHTNING = "⚡"
EMOJI_FIRE = "🔥"
EMOJI_FOLDER = "📁"
EMOJI_CHART = "📊"
EMOJI_FACTORY = "🏭"
EMOJI_LOCATION = "📍"
EMOJI_PACKAGE = "📦"
EMOJI_DISK = "💾"
EMOJI_MEMORY = "💾"
EMOJI_TIME = "⏱️"
EMOJI_SUCCESS = "✅"
EMOJI_ERROR = "❌"
EMOJI_WARNING = "⚠️"
EMOJI_PARTY = "🎉"
EMOJI_LINK = "🔗"
EMOJI_CYCLE = "🔄"
EMOJI_CHART_UP = "📈"

# Progress display formatting
PROGRESS_SEPARATOR = "=" * 80

# =============================================================================
# DEFAULT SETTINGS FOR STREETLEVEL INTEGRATION
# =============================================================================
# Default parameters for panorama fetching
FETCH_LINKS_DEFAULT = True
FETCH_HISTORICAL_DEFAULT = True

# =============================================================================
# CONFIGURATION POUR COLLECTE TOTALE (ILLIMITÉE)
# =============================================================================
# Mode collecte totale - tous les panoramas existants
UNLIMITED_MODE = True  # True = collecte illimitée, False = limite fixe
UNLIMITED_TARGET = float('inf')  # Collecte jusqu'à épuisement

# Critères d'arrêt pour mode illimité
UNLIMITED_STOP_EMPTY_ITERATIONS = 50  # Arrêt si 50 itérations sans nouveaux panoramas
UNLIMITED_STOP_EMPTY_QUEUE_TIME = 300  # Arrêt si queue vide pendant 5min

# Métriques spéciales pour collecte totale
UNLIMITED_PROGRESS_INTERVAL = 100  # Affichage toutes les 100 itérations
UNLIMITED_CHECKPOINT_INTERVAL = 100000  # Checkpoint tous les 100k panoramas

# =============================================================================
# CONFIGURATION POUR 50 MILLIONS DE PANORAMAS
# =============================================================================
# Taille des batches pour éviter l'OOM
MEGA_BATCH_SIZE = 10000  # Écriture tous les 10k panoramas

# Limites de cache pour gros volumes
MEGA_FAILED_CACHE_MAX = 50000  # Limite du cache d'échecs
MEGA_FAILED_CACHE_KEEP = 25000  # Nombre à garder lors du nettoyage

# Limite de la queue to_visit (garde-fou)
MEGA_TO_VISIT_LIMIT = 5000000  # 5M max dans la queue
MEGA_TO_VISIT_KEEP = 2000000   # 2M à garder lors du nettoyage

# Fréquence de nettoyage pour gros volumes
MEGA_CLEANUP_INTERVAL = 100  # Toutes les 100 itérations BFS

# =============================================================================
# SYSTEM PATHS
# =============================================================================
# Local streetlevel module path (relative to script)
STREETLEVEL_LOCAL_PATH = "." 
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

# Multiple starting points for multi-source BFS
# Format: List of (latitude, longitude) tuples
DEFAULT_START_POINTS = [
    (50.0755, 14.4378),   # Prague, Czech Republic
    (49.1951, 16.6068),   # Brno, Czech Republic
    (49.7384, 13.3736),   # Plzen, Czech Republic
    (50.7663, 15.0561),   # Hradec Kralove, Czech Republic
    (49.5955, 17.2517),   # Olomouc, Czech Republic
]

# Advanced multi-source configuration
MULTI_SOURCE_MODE = True  # Enable multi-source BFS
DISTRIBUTE_START_POINTS = True  # Distribute geographically for better coverage

# Grid-based start points generation
GRID_START_POINTS_ENABLED = False  # Enable grid-based distribution
GRID_LAT_MIN = 48.5  # Southern border of Czech Republic
GRID_LAT_MAX = 51.1  # Northern border of Czech Republic  
GRID_LON_MIN = 12.1  # Western border of Czech Republic
GRID_LON_MAX = 18.9  # Eastern border of Czech Republic
GRID_DENSITY = 0.5   # Degrees between grid points (0.5° ≈ 55km)

def generate_grid_start_points():
    """Generate a grid of start points covering Czech Republic"""
    points = []
    lat = GRID_LAT_MIN
    while lat <= GRID_LAT_MAX:
        lon = GRID_LON_MIN
        while lon <= GRID_LON_MAX:
            points.append((round(lat, 4), round(lon, 4)))
            lon += GRID_DENSITY
        lat += GRID_DENSITY
    return points

# Major European cities for extended coverage
EXTENDED_START_POINTS = [
    # Czech Republic (primary)
    (50.0755, 14.4378),   # Prague
    (49.1951, 16.6068),   # Brno
    (49.7384, 13.3736),   # Plzen
    (50.7663, 15.0561),   # Hradec Kralove
    (49.5955, 17.2517),   # Olomouc
    (49.8349, 18.2820),   # Ostrava
    (50.2103, 15.8318),   # Pardubice
    (49.2041, 14.4669),   # Ceske Budejovice
    
    # Neighboring countries for extended coverage
    (48.1486, 17.1077),   # Bratislava, Slovakia
    (48.7164, 21.2611),   # Kosice, Slovakia
    (47.4979, 19.0402),   # Budapest, Hungary
    (52.2297, 21.0122),   # Warsaw, Poland
    (50.0647, 19.9450),   # Krakow, Poland
    (51.1079, 17.0385),   # Wroclaw, Poland
    (48.2082, 16.3738),   # Vienna, Austria
    (47.0707, 15.4395),   # Graz, Austria
    (52.5200, 13.4050),   # Berlin, Germany
    (50.1109, 8.6821),    # Frankfurt, Germany
    (48.1351, 11.5820),   # Munich, Germany
]

# Predefined point sets for different strategies
COVERAGE_STRATEGIES = {
    'czech_only': DEFAULT_START_POINTS,
    'major_cities': DEFAULT_START_POINTS + [
        (49.8349, 18.2820),   # Ostrava
        (50.2103, 15.8318),   # Pardubice
        (49.2041, 14.4669),   # Ceske Budejovice
    ],
    'central_europe': EXTENDED_START_POINTS,
    'grid_coverage': generate_grid_start_points() if GRID_START_POINTS_ENABLED else DEFAULT_START_POINTS,
}

# Select coverage strategy
COVERAGE_STRATEGY = 'major_cities'  # Options: 'czech_only', 'major_cities', 'central_europe', 'grid_coverage'

# Search parameters
DEFAULT_SEARCH_RADIUS = 100.0  # meters

# =============================================================================
# COLLECTION PARAMETERS
# =============================================================================
# Target number of panoramas to collect
DEFAULT_TARGET_COUNT = 10000

# Output directory (sera remplacé par le système de sessions)
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
PROGRESS_TIME_INTERVAL = 10    # seconds - affichage toutes les 10 secondes

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
EMOJI_SEARCH = "🔍"
EMOJI_QUEUE = "📥"
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
UNLIMITED_STOP_EMPTY_ITERATIONS = None  # Désactivé - pas d'arrêt sur itérations vides
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
# SYSTÈME DE SESSIONS NUMÉROTÉES
# =============================================================================
# Configuration pour sessions numérotées avec reprise automatique
SESSION_BASE_DIR = "data/mapy/raw"
CHECKPOINT_BASE_DIR = "checkpoints"

# Fichier de tracking de la session courante
SESSION_TRACKER_FILE = "current_session.json"

# =============================================================================
# SYSTÈME DE CHECKPOINT ET REPRISE
# =============================================================================
# Activation du système de sauvegarde/reprise
ENABLE_CHECKPOINTS = True

# Fréquence des checkpoints
CHECKPOINT_INTERVAL = 10000  # Sauvegarde tous les 10000 panoramas
CHECKPOINT_TIME_INTERVAL = 300  # Sauvegarde toutes les 5 minutes minimum

# Fichiers de sauvegarde (dans le dossier de session)
CHECKPOINT_STATE_FILE = "collection_state.json"
CHECKPOINT_VISITED_FILE = "visited_set.pkl"
CHECKPOINT_TO_VISIT_FILE = "to_visit_queue.pkl"
CHECKPOINT_FAILED_CACHE_FILE = "failed_cache.pkl"
CHECKPOINT_METADATA_FILE = "metadata.json"

# Options de reprise
AUTO_RESUME = True  # Reprise automatique si checkpoint détecté
RESUME_PROMPT = False  # Demander confirmation avant reprise
CHECKPOINT_BACKUP_COUNT = 3  # Garder 3 backups rotatifs

# =============================================================================
# FAILED PANORAMA RETRY CONFIGURATION
# =============================================================================
# Configuration spécifique pour le retry des panoramas échoués
FAILED_RETRY_OUTPUT_DIR = "data/mapy_failed_retry"
FAILED_RETRY_TIMEOUT = 30  # Timeout plus long pour les échecs
FAILED_RETRY_MAX_CONCURRENT_PER_PROXY = 50  # Concurrence plus agressive
FAILED_RETRY_BATCH_SIZE = 100  # Batches plus petits
FAILED_RETRY_MAX_ATTEMPTS = 3  # Nombre max de tentatives par panorama
FAILED_RETRY_DELAY_BETWEEN_ATTEMPTS = 0.5  # Délai réduit entre tentatives (0.5s)
FAILED_RETRY_CHECKPOINT_INTERVAL = 1000  # Checkpoint tous les 1000 panoramas

# Emojis spécifiques au retry
EMOJI_RECYCLE = "♻️"

# =============================================================================
# SYSTEM PATHS
# =============================================================================
# Local streetlevel module path (relative to script)
STREETLEVEL_LOCAL_PATH = "." 
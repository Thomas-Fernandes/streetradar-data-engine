"""
Configuration pour la collecte de données Mapy.cz
Tous les paramètres configurables sont centralisés ici.
"""

# =============================================================================
# GÉOGRAPHIE
# =============================================================================
# Point de départ (Prague)
START_LATITUDE = 50.0755
START_LONGITUDE = 14.4378
SEARCH_RADIUS = 100.0  # mètres

# =============================================================================
# COLLECTE
# =============================================================================
# Nombre de panoramas à collecter (None = illimité)
TARGET_COUNT = None  # Collecte exhaustive de tous les panoramas accessibles

# Inclure les données historiques (peut impacter les performances)
INCLUDE_HISTORICAL = False

# Inclure les liens vers d'autres panoramas
INCLUDE_LINKS = True

# =============================================================================
# PERFORMANCE
# =============================================================================
# Concurrence maximale
MAX_CONCURRENT = 250

# Timeouts réseau (secondes)
NETWORK_TIMEOUT = 10.0

# =============================================================================
# STOCKAGE
# =============================================================================
# Dossier de sortie de base (sera auto-incrémenté)
OUTPUT_BASE_DIR = "../../data/mapy/raw"

# Taille des batches pour l'écriture
BATCH_SIZE = 25000

# Format de fichier
USE_PARQUET = True
PARQUET_COMPRESSION = 'snappy'

# =============================================================================
# AFFICHAGE
# =============================================================================
# Fréquence d'affichage des statistiques
PROGRESS_INTERVAL = 100  # toutes les 100 itérations

# =============================================================================
# PATHS
# =============================================================================
# Chemin vers le module streetlevel (depuis scripts/mapy/)
STREETLEVEL_PATH = "../../"
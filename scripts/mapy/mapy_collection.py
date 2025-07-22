import time
import json
import sys
import asyncio
import aiohttp
import os
from datetime import datetime
from collections import deque
from typing import Set, Dict, List, Optional, Tuple
import itertools
import random
import pickle
import shutil
import signal
import logging
from io import StringIO

# Import settings
from settings import *

# Ajout du dossier local streetlevel au path Python
sys.path.insert(0, os.path.join(os.path.dirname(__file__), STREETLEVEL_LOCAL_PATH))

# Import depuis les fichiers locaux
from streetlevel.mapy import find_panorama_async, find_panorama_by_id_async, set_proxy_manager
from streetlevel.mapy.api import MapyApi
from proxy import PROXIES

import pyarrow as pa
import pyarrow.parquet as pq
import gc
import psutil
import threading
from concurrent.futures import ThreadPoolExecutor

class LogManager:
    """Gestionnaire de logs qui capture tous les print et les écrit dans un fichier"""
    
    def __init__(self, session_dir: str):
        self.session_dir = session_dir
        self.log_file = os.path.join(session_dir, "session.log")
        self.lock = threading.Lock()
        
        # Créer le dossier si nécessaire
        os.makedirs(session_dir, exist_ok=True)
        
        # Initialiser le fichier de log
        with open(self.log_file, 'a', encoding='utf-8') as f:
            f.write(f"\n{'='*80}\n")
            f.write(f"SESSION STARTED: {datetime.now().isoformat()}\n")
            f.write(f"{'='*80}\n")
    
    def log_print(self, message: str):
        """Écrit un message dans le log avec timestamp"""
        timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        log_message = f"[{timestamp}] {message}\n"
        
        with self.lock:
            try:
                with open(self.log_file, 'a', encoding='utf-8') as f:
                    f.write(log_message)
            except Exception as e:
                # En cas d'erreur de log, ne pas planter le programme
                pass
    
    def get_log_file(self) -> str:
        """Retourne le chemin du fichier de log"""
        return self.log_file

# Instance globale du LogManager (sera initialisée dans main())
_log_manager = None

def setup_logging(session_dir: str):
    """Configure le système de logging"""
    global _log_manager
    _log_manager = LogManager(session_dir)

def log_print(*args, **kwargs):
    """Fonction print améliorée qui log et affiche"""
    # Convertir les arguments en string comme le fait print()
    if args:
        if len(args) == 1:
            message = str(args[0])
        else:
            message = ' '.join(str(arg) for arg in args)
    else:
        message = ''
    
    # Afficher sur la console en utilisant la fonction print originale
    original_print(*args, **kwargs)
    
    # Logger si le log manager est configuré
    if _log_manager:
        _log_manager.log_print(message)

# Remplacer la fonction print globale par notre version
original_print = print
print = log_print

class SessionManager:
    """Gestionnaire de sessions numérotées avec reprise automatique"""
    
    def __init__(self):
        self.session_tracker_path = SESSION_TRACKER_FILE
        self.current_session = None
        self.session_dir = None
        self.checkpoint_dir = None
        
    def get_current_session_info(self) -> Optional[Dict]:
        """Récupère les informations de la session courante"""
        if os.path.exists(self.session_tracker_path):
            try:
                with open(self.session_tracker_path, 'r') as f:
                    return json.load(f)
            except Exception as e:
                print(f"{EMOJI_ERROR} Erreur lecture session: {e}")
        return None
    
    def has_active_session(self) -> bool:
        """Vérifie s'il y a une session active (non terminée)"""
        session_info = self.get_current_session_info()
        if not session_info:
            return False
        
        # Vérifier si la session est marquée comme terminée
        if session_info.get('completed', False):
            return False
            
        # Vérifier si le dossier de checkpoint existe
        session_num = session_info.get('session_number')
        if session_num is None:
            return False
            
        checkpoint_dir = os.path.join(CHECKPOINT_BASE_DIR, str(session_num))
        checkpoint_file = os.path.join(checkpoint_dir, CHECKPOINT_STATE_FILE)
        
        return os.path.exists(checkpoint_file)
    
    def get_next_session_number(self) -> int:
        """Détermine le numéro de la prochaine session"""
        session_info = self.get_current_session_info()
        
        if session_info and not session_info.get('completed', False):
            # Session active, continuer avec le même numéro
            return session_info['session_number']
        
        # Nouvelle session, incrémenter
        if session_info:
            return session_info['session_number'] + 1
        
        # Première session
        return 1
    
    def initialize_session(self, resume_from_checkpoint: bool = False) -> Tuple[int, str, str]:
        """Initialise une session et retourne (numéro, dossier_output, dossier_checkpoint)"""
        session_number = self.get_next_session_number()
        
        # Créer les dossiers
        session_dir = os.path.join(SESSION_BASE_DIR, str(session_number))
        checkpoint_dir = os.path.join(CHECKPOINT_BASE_DIR, str(session_number))
        
        os.makedirs(session_dir, exist_ok=True)
        os.makedirs(checkpoint_dir, exist_ok=True)
        
        # Sauvegarder les infos de session
        session_info = {
            'session_number': session_number,
            'created_at': datetime.now().isoformat(),
            'session_dir': session_dir,
            'checkpoint_dir': checkpoint_dir,
            'completed': False,
            'resumed': resume_from_checkpoint
        }
        
        with open(self.session_tracker_path, 'w') as f:
            json.dump(session_info, f, indent=2)
        
        self.current_session = session_number
        self.session_dir = session_dir
        self.checkpoint_dir = checkpoint_dir
        
        print(f"{EMOJI_FOLDER} Session #{session_number}")
        print(f"   • Output: {session_dir}/")
        print(f"   • Checkpoints: {checkpoint_dir}/")
        
        if resume_from_checkpoint:
            print(f"   • Mode: Reprise de session existante")
        else:
            print(f"   • Mode: Nouvelle session")
        
        return session_number, session_dir, checkpoint_dir
    
    def mark_session_completed(self):
        """Marque la session courante comme terminée"""
        try:
            session_info = self.get_current_session_info()
            if session_info:
                session_info['completed'] = True
                session_info['completed_at'] = datetime.now().isoformat()
                
                with open(self.session_tracker_path, 'w') as f:
                    json.dump(session_info, f, indent=2)
                
                print(f"{EMOJI_SUCCESS} Session #{session_info['session_number']} marquée comme terminée")
        except Exception as e:
            print(f"{EMOJI_ERROR} Erreur marquage session: {e}")
    
    def get_session_dirs(self) -> Tuple[str, str]:
        """Retourne les dossiers de la session courante"""
        return self.session_dir, self.checkpoint_dir

class CheckpointManager:
    """Gestionnaire de checkpoints pour sauvegarde/reprise automatique"""
    
    def __init__(self, checkpoint_dir: str):
        self.checkpoint_dir = checkpoint_dir
        self.last_checkpoint_time = time.time()
        self.last_checkpoint_count = 0
        
        # Créer le dossier de checkpoints
        os.makedirs(checkpoint_dir, exist_ok=True)
        
        print(f"{EMOJI_DISK} Système de checkpoints activé: {checkpoint_dir}/")
    
    def should_checkpoint(self, current_count: int) -> bool:
        """Détermine si il faut faire un checkpoint"""
        if not ENABLE_CHECKPOINTS:
            return False
            
        # Checkpoint basé sur le nombre
        count_trigger = current_count - self.last_checkpoint_count >= CHECKPOINT_INTERVAL
        
        # Checkpoint basé sur le temps
        time_trigger = time.time() - self.last_checkpoint_time >= CHECKPOINT_TIME_INTERVAL
        
        return count_trigger or time_trigger
    
    def save_checkpoint(self, collector):
        """Sauvegarde l'état complet du collecteur"""
        try:
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            
            # État principal
            state = {
                'timestamp': timestamp,
                'total_panoramas': len(collector.visited),
                'exploration_order': collector.exploration_order,
                'network_calls': collector.network_calls,
                'timeouts': collector.timeouts,
                'errors': collector.errors,
                'bfs_iterations': collector.bfs_iterations,
                'start_time': collector.start_time,
                'total_files_written': collector.total_files_written,
                'total_records_written': collector.total_records_written,
                'last_progress_time': collector.last_progress_time,
                'last_progress_count': collector.last_progress_count
            }
            
            # Sauvegarder l'état principal
            state_file = os.path.join(self.checkpoint_dir, CHECKPOINT_STATE_FILE)
            with open(state_file, 'w') as f:
                json.dump(state, f, indent=2)
            
            # Sauvegarder les structures de données
            self._save_pickle_data(CHECKPOINT_VISITED_FILE, set(collector.visited))
            self._save_pickle_data(CHECKPOINT_TO_VISIT_FILE, list(collector.to_visit))
            self._save_pickle_data(CHECKPOINT_FAILED_CACHE_FILE, set(collector.failed_cache))
            
            # Métadonnées de checkpoint
            metadata = {
                'checkpoint_time': time.time(),
                'checkpoint_timestamp': timestamp,
                'version': '1.0',
                'total_panoramas': len(collector.visited),
                'queue_size': len(collector.to_visit),
                'failed_cache_size': len(collector.failed_cache)
            }
            
            metadata_file = os.path.join(self.checkpoint_dir, CHECKPOINT_METADATA_FILE)
            with open(metadata_file, 'w') as f:
                json.dump(metadata, f, indent=2)
            
            self.last_checkpoint_time = time.time()
            self.last_checkpoint_count = len(collector.visited)
            
            print(f"{EMOJI_SUCCESS} Checkpoint sauvé: {len(collector.visited):,} panoramas")
            
            # Rotation des backups
            self._rotate_backups()
            
        except Exception as e:
            print(f"{EMOJI_ERROR} Erreur sauvegarde checkpoint: {e}")
    
    def _save_pickle_data(self, filename: str, data):
        """Sauvegarde des données avec pickle"""
        filepath = os.path.join(self.checkpoint_dir, filename)
        with open(filepath, 'wb') as f:
            pickle.dump(data, f)
    
    def _load_pickle_data(self, filename: str):
        """Charge des données depuis pickle"""
        filepath = os.path.join(self.checkpoint_dir, filename)
        if os.path.exists(filepath):
            with open(filepath, 'rb') as f:
                return pickle.load(f)
        return None
    
    def has_checkpoint(self) -> bool:
        """Vérifie si un checkpoint existe"""
        state_file = os.path.join(self.checkpoint_dir, CHECKPOINT_STATE_FILE)
        return os.path.exists(state_file)
    
    def get_checkpoint_info(self) -> Optional[Dict]:
        """Récupère les informations du checkpoint"""
        if not self.has_checkpoint():
            return None
            
        try:
            # Charger les métadonnées
            metadata_file = os.path.join(self.checkpoint_dir, CHECKPOINT_METADATA_FILE)
            if os.path.exists(metadata_file):
                with open(metadata_file, 'r') as f:
                    metadata = json.load(f)
                return metadata
            
            # Fallback sur l'état principal
            state_file = os.path.join(self.checkpoint_dir, CHECKPOINT_STATE_FILE)
            with open(state_file, 'r') as f:
                state = json.load(f)
            return {
                'checkpoint_timestamp': state.get('timestamp'),
                'total_panoramas': state.get('total_panoramas', 0),
                'checkpoint_time': time.time()
            }
            
        except Exception as e:
            print(f"{EMOJI_ERROR} Erreur lecture checkpoint: {e}")
            return None
    
    def load_checkpoint(self) -> Optional[Dict]:
        """Charge l'état complet depuis le checkpoint"""
        if not self.has_checkpoint():
            return None
            
        try:
            # Charger l'état principal
            state_file = os.path.join(self.checkpoint_dir, CHECKPOINT_STATE_FILE)
            with open(state_file, 'r') as f:
                state = json.load(f)
            
            # Charger les structures de données
            visited = self._load_pickle_data(CHECKPOINT_VISITED_FILE)
            to_visit = self._load_pickle_data(CHECKPOINT_TO_VISIT_FILE)
            failed_cache = self._load_pickle_data(CHECKPOINT_FAILED_CACHE_FILE)
            
            if visited is None or to_visit is None:
                print(f"{EMOJI_ERROR} Données de checkpoint incomplètes")
                return None
            
            return {
                'state': state,
                'visited': set(visited),
                'to_visit': deque(to_visit),
                'failed_cache': set(failed_cache) if failed_cache else set()
            }
            
        except Exception as e:
            print(f"{EMOJI_ERROR} Erreur chargement checkpoint: {e}")
            return None
    
    def clear_checkpoint(self):
        """Supprime les fichiers de checkpoint"""
        try:
            files_to_remove = [
                CHECKPOINT_STATE_FILE,
                CHECKPOINT_VISITED_FILE, 
                CHECKPOINT_TO_VISIT_FILE,
                CHECKPOINT_FAILED_CACHE_FILE,
                CHECKPOINT_METADATA_FILE
            ]
            
            for filename in files_to_remove:
                filepath = os.path.join(self.checkpoint_dir, filename)
                if os.path.exists(filepath):
                    os.remove(filepath)
            
            print(f"{EMOJI_SUCCESS} Checkpoints nettoyés")
            
        except Exception as e:
            print(f"{EMOJI_ERROR} Erreur nettoyage checkpoints: {e}")
    
    def _rotate_backups(self):
        """Rotation des backups de checkpoint"""
        try:
            if CHECKPOINT_BACKUP_COUNT <= 0:
                return
                
            # Créer dossier backup
            backup_dir = os.path.join(self.checkpoint_dir, "backups")
            os.makedirs(backup_dir, exist_ok=True)
            
            # Timestamp pour le backup
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            
            # Copier les fichiers principaux
            files_to_backup = [
                CHECKPOINT_STATE_FILE,
                CHECKPOINT_VISITED_FILE,
                CHECKPOINT_TO_VISIT_FILE,
                CHECKPOINT_FAILED_CACHE_FILE,
                CHECKPOINT_METADATA_FILE
            ]
            
            for filename in files_to_backup:
                src = os.path.join(self.checkpoint_dir, filename)
                if os.path.exists(src):
                    dst = os.path.join(backup_dir, f"{timestamp}_{filename}")
                    shutil.copy2(src, dst)
            
            # Supprimer les anciens backups
            self._cleanup_old_backups(backup_dir)
            
        except Exception as e:
            print(f"{EMOJI_WARNING} Erreur rotation backups: {e}")
    
    def _cleanup_old_backups(self, backup_dir: str):
        """Nettoie les anciens backups"""
        try:
            # Lister tous les backups
            backup_files = []
            for filename in os.listdir(backup_dir):
                if filename.endswith(CHECKPOINT_STATE_FILE):
                    filepath = os.path.join(backup_dir, filename)
                    timestamp = os.path.getmtime(filepath)
                    backup_files.append((timestamp, filename.split('_')[0]))
            
            # Trier par timestamp (plus récent d'abord)
            backup_files.sort(reverse=True)
            
            # Supprimer les anciens
            if len(backup_files) > CHECKPOINT_BACKUP_COUNT:
                for _, prefix in backup_files[CHECKPOINT_BACKUP_COUNT:]:
                    for filename in os.listdir(backup_dir):
                        if filename.startswith(prefix):
                            os.remove(os.path.join(backup_dir, filename))
            
        except Exception as e:
            print(f"{EMOJI_WARNING} Erreur nettoyage backups: {e}")

class ProxyManager:
    """Gestionnaire de proxies avec rotation intelligente"""
    
    def __init__(self, proxies: List[Tuple]):
        self.proxies = proxies
        self.proxy_cycle = itertools.cycle(proxies)
        self.proxy_stats = {i: PROXY_STATS_INIT.copy() for i in range(len(proxies))}
        self.current_proxy_index = 0
        self.lock = asyncio.Lock()
        
        print(f"{EMOJI_GLOBE} {len(proxies)} proxies chargés")
        for i, (ip, port, user, pwd) in enumerate(proxies):
            print(f"   Proxy {i+1}: {ip}:{port}")
    
    def get_proxy_url(self, proxy_tuple: Tuple) -> str:
        """Convertit un tuple proxy en URL"""
        ip, port, user, pwd = proxy_tuple
        return PROXY_URL_FORMAT.format(user=user, pwd=pwd, ip=ip, port=port)
    
    async def get_next_proxy(self) -> Tuple[int, str]:
        """Retourne le prochain proxy avec rotation"""
        async with self.lock:
            proxy_index = self.current_proxy_index
            proxy_tuple = self.proxies[proxy_index]
            proxy_url = self.get_proxy_url(proxy_tuple)
            
            self.current_proxy_index = (self.current_proxy_index + 1) % len(self.proxies)
            self.proxy_stats[proxy_index]['last_used'] = time.time()
            
            return proxy_index, proxy_url
    
    def record_request(self, proxy_index: int, success: bool = True):
        """Enregistre une requête pour les statistiques"""
        self.proxy_stats[proxy_index]['requests'] += 1
        if not success:
            self.proxy_stats[proxy_index]['errors'] += 1
    
    def get_stats(self) -> Dict:
        """Retourne les statistiques des proxies"""
        total_requests = sum(stats['requests'] for stats in self.proxy_stats.values())
        total_errors = sum(stats['errors'] for stats in self.proxy_stats.values())
        
        return {
            'total_requests': total_requests,
            'total_errors': total_errors,
            'success_rate': (total_requests - total_errors) / total_requests * 100 if total_requests > 0 else 0,
            'proxy_details': self.proxy_stats
        }

class ProxyRotatingConnector:
    """Connecteur qui utilise différents proxies pour chaque requête"""
    
    def __init__(self, proxy_manager: ProxyManager, max_concurrent_per_proxy: int = DEFAULT_MAX_CONCURRENT_PER_PROXY):
        self.proxy_manager = proxy_manager
        self.max_concurrent_per_proxy = max_concurrent_per_proxy
        self.sessions = {}
        self.semaphores = {}

        # Créer un connecteur TCP partagé pour tous les proxies
        total_limit = len(proxy_manager.proxies) * max_concurrent_per_proxy
        self.connector = MapyApi.get_shared_connector(limit=total_limit)

        # Créer un semaphore par proxy pour limiter la concurrence
        for i in range(len(proxy_manager.proxies)):
            self.semaphores[i] = asyncio.Semaphore(max_concurrent_per_proxy)
    
    async def create_session_for_proxy(self, proxy_index: int, proxy_url: str) -> aiohttp.ClientSession:
        """Crée une session pour un proxy spécifique"""
        if proxy_index not in self.sessions:
            timeout = aiohttp.ClientTimeout(total=HTTP_TOTAL_TIMEOUT, connect=HTTP_CONNECT_TIMEOUT)

            # Utiliser le connecteur partagé pour toutes les sessions
            self.sessions[proxy_index] = aiohttp.ClientSession(
                connector=self.connector,
                timeout=timeout,
                headers={
                    'User-Agent': USER_AGENT_BROWSER,
                    **HTTP_HEADERS
                }
            )
        
        return self.sessions[proxy_index]
    
    async def make_request_with_proxy(self, url: str, **kwargs):
        """Fait une requête en utilisant un proxy rotatif"""
        proxy_index, proxy_url = await self.proxy_manager.get_next_proxy()
        
        async with self.semaphores[proxy_index]:
            try:
                session = await self.create_session_for_proxy(proxy_index, proxy_url)
                
                # Configuration du proxy pour aiohttp
                kwargs['proxy'] = proxy_url
                
                async with session.get(url, **kwargs) as response:
                    self.proxy_manager.record_request(proxy_index, True)
                    return await response.read()
                    
            except Exception as e:
                self.proxy_manager.record_request(proxy_index, False)
                raise e
    
    async def close_all_sessions(self):
        """Ferme toutes les sessions"""
        for session in self.sessions.values():
            await session.close()
        self.sessions.clear()

class UltraProxyOptimizedCollector:
    """Collecteur avec optimisations proxy pour saturer l'API + Système de checkpoints"""
    
    def __init__(self, output_dir: str, checkpoint_dir: str, proxies: List[Tuple], max_concurrent_per_proxy: int = DEFAULT_MAX_CONCURRENT_PER_PROXY, resume_from_checkpoint: bool = False):
        self.output_dir = output_dir
        self.proxy_manager = ProxyManager(proxies)
        
        # Configurer le proxy manager pour streetlevel
        set_proxy_manager(self.proxy_manager)
        
        self.proxy_connector = ProxyRotatingConnector(self.proxy_manager, max_concurrent_per_proxy)
        
        # Calcul de la concurrence totale
        self.total_max_concurrent = len(proxies) * max_concurrent_per_proxy
        
        # Système de checkpoints avec dossier de session
        self.checkpoint_manager = CheckpointManager(checkpoint_dir)
        self.resume_from_checkpoint = resume_from_checkpoint
        
        # Métriques - OPTIMISÉ POUR 50M
        # Utiliser des sets plus efficaces en mémoire
        self.visited: Set[int] = set()  # int au lieu de str
        self.to_visit: deque = deque()
        self.network_calls = 0
        self.timeouts = 0
        self.errors = 0
        self.exploration_order = 0
        self.start_time = time.time()
        
        # Optimisations
        self.semaphore = asyncio.Semaphore(self.total_max_concurrent)
        
        # Cache et déduplication - LIMITÉS EN TAILLE
        self.processing_cache: Set[int] = set()  # int au lieu de str
        self.failed_cache: Set[int] = set()  # int au lieu de str
        
        # Batch writer optimisé - TAILLE RÉDUITE
        self.batch_data = []
        self.write_executor = ThreadPoolExecutor(max_workers=FILE_WRITER_MAX_WORKERS)
        
        # Métriques de performance
        self.last_progress_time = time.time()
        self.last_progress_count = 0
        self.bfs_iterations = 0
        
        # Affichage temporel (toutes les 10 secondes)
        self.last_time_display = time.time()
        
        # Compteurs pour fichiers multiples
        self.total_files_written = 0
        self.total_records_written = 0
        
        # Flag pour gestion gracieuse des interruptions
        self.should_stop = False
        self._setup_signal_handlers()
        
        # Créer le dossier
        os.makedirs(output_dir, exist_ok=True)
        
        print(f"{EMOJI_ROCKET} COLLECTEUR PROXY ULTRA-OPTIMISÉ [50M SAFE + CHECKPOINTS]")
        print(f"{EMOJI_GLOBE} {len(proxies)} proxies × {max_concurrent_per_proxy} = {self.total_max_concurrent} connexions simultanées")
        print(f"{EMOJI_FOLDER} Sortie: {output_dir}/")
        print(f"{EMOJI_PACKAGE} Mode 50M panoramas - Écriture par batches de {MEGA_BATCH_SIZE:,}")
        
        if ENABLE_CHECKPOINTS:
            print(f"{EMOJI_DISK} Checkpoints activés: tous les {CHECKPOINT_INTERVAL:,} panoramas")

    def _setup_signal_handlers(self):
        """Configure les gestionnaires de signaux pour arrêt gracieux"""
        def signal_handler(signum, frame):
            print(f"\n{EMOJI_WARNING} Signal {signum} reçu - Arrêt gracieux en cours...")
            self.should_stop = True
            
        signal.signal(signal.SIGINT, signal_handler)
        signal.signal(signal.SIGTERM, signal_handler)

    async def load_from_checkpoint(self) -> bool:
        """Charge l'état depuis un checkpoint"""
        if not self.resume_from_checkpoint:
            return False
            
        checkpoint_data = self.checkpoint_manager.load_checkpoint()
        if not checkpoint_data:
            return False
            
        try:
            # Restaurer l'état
            state = checkpoint_data['state']
            self.visited = checkpoint_data['visited']
            self.to_visit = checkpoint_data['to_visit']
            self.failed_cache = checkpoint_data['failed_cache']
            
            # Restaurer les métriques
            self.exploration_order = state['exploration_order']
            self.network_calls = state['network_calls']
            self.timeouts = state['timeouts']
            self.errors = state['errors']
            self.bfs_iterations = state['bfs_iterations']
            self.start_time = state['start_time']
            self.total_files_written = state['total_files_written']
            self.total_records_written = state['total_records_written']
            self.last_progress_time = state['last_progress_time']
            self.last_progress_count = state['last_progress_count']
            
            # Initialiser l'affichage temporel depuis la reprise
            self.last_time_display = time.time()
            
            print(f"{EMOJI_SUCCESS} État restauré depuis checkpoint:")
            print(f"   • {len(self.visited):,} panoramas déjà collectés")
            print(f"   • {len(self.to_visit):,} panoramas en attente")
            print(f"   • {self.total_files_written} fichiers déjà écrits")
            print(f"   • Reprise depuis l'itération BFS #{self.bfs_iterations}")
            
            return True
            
        except Exception as e:
            print(f"{EMOJI_ERROR} Erreur chargement checkpoint: {e}")
            return False

    async def save_checkpoint_if_needed(self):
        """Sauvegarde un checkpoint si nécessaire"""
        if self.checkpoint_manager.should_checkpoint(len(self.visited)):
            await self._save_checkpoint_async()

    async def _save_checkpoint_async(self):
        """Sauvegarde asynchrone du checkpoint"""
        try:
            # Exécuter la sauvegarde dans un thread séparé pour ne pas bloquer
            loop = asyncio.get_event_loop()
            await loop.run_in_executor(
                self.write_executor, 
                self.checkpoint_manager.save_checkpoint, 
                self
            )
        except Exception as e:
            print(f"{EMOJI_ERROR} Erreur sauvegarde checkpoint: {e}")
    
    async def __aenter__(self):
        return self
    
    async def __aexit__(self, exc_type, exc_val, exc_tb):
        # Sauvegarde finale avant fermeture
        if ENABLE_CHECKPOINTS and not self.should_stop:
            print(f"{EMOJI_DISK} Sauvegarde finale...")
            await self._save_checkpoint_async()
        
        await self.proxy_connector.close_all_sessions()
        
        # Écriture finale
        if self.batch_data:
            await self._write_batch_if_needed(force=True)
        
        self.write_executor.shutdown(wait=True)
        
        # Statistiques finales des proxies
        proxy_stats = self.proxy_manager.get_stats()
        print(f"\n{EMOJI_CHART} STATISTIQUES PROXIES:")
        print(f"   • Total requêtes: {proxy_stats['total_requests']:,}")
        print(f"   • Taux de succès: {proxy_stats['success_rate']:.1f}%")
        print(f"   • Erreurs: {proxy_stats['total_errors']:,}")
        
        # Ne pas nettoyer les checkpoints automatiquement - ils restent dans le dossier de session
        # Le SessionManager gère la finalisation

    def panorama_to_dict_minimal(self, pano, exploration_order: int) -> Dict:
        """Conversion ultra-minimaliste pour vitesse maximale avec données historiques"""
        historical_data = []
        if pano.historical:
            for hist_pano in pano.historical:
                historical_data.append({
                    "id": hist_pano.id,
                    "date": hist_pano.date.isoformat() if hasattr(hist_pano.date, 'isoformat') else str(hist_pano.date),
                    "provider": hist_pano.provider,
                    "permalink": hist_pano.permalink() if hasattr(hist_pano, 'permalink') else None
                })
        
        return {
            "id": pano.id,
            "lat": pano.lat,
            "lon": pano.lon,
            "date": pano.date.isoformat() if hasattr(pano.date, 'isoformat') else str(pano.date),
            "provider": pano.provider,
            "elevation": pano.elevation,
            "permalink": pano.permalink() if hasattr(pano, 'permalink') else None,
            "order": exploration_order,
            "links": len(pano.links) if pano.links else 0,
            "link_ids": [link.pano.id for link in (pano.links or []) if hasattr(link, 'pano')],
            "historical_count": len(pano.historical) if pano.historical else 0,
            "historical": historical_data,
            "time": time.time()
        }
    
    async def fetch_panorama_with_proxy(self, panoid: str):
        """Récupération avec proxies rotatifs"""
        if panoid in self.processing_cache or panoid in self.failed_cache:
            return None
        
        self.processing_cache.add(panoid)
        
        async with self.semaphore:
            try:
                # Appel avec proxy rotatif - timeout agressif
                pano = await asyncio.wait_for(
                    find_panorama_by_id_async(panoid, links=FETCH_LINKS_DEFAULT, historical=FETCH_HISTORICAL_DEFAULT),
                    timeout=PANORAMA_FETCH_TIMEOUT
                )
                self.network_calls += 1
                return pano
                
            except asyncio.TimeoutError:
                self.timeouts += 1
                self.failed_cache.add(panoid)
                return None
            except Exception as e:
                self.errors += 1
                self.failed_cache.add(panoid)
                return None
            finally:
                self.processing_cache.discard(panoid)
    
    async def _write_final_batch(self):
        """Écriture finale en Parquet"""
        if not self.batch_data:
            return
        
        try:
            timestamp = datetime.now().strftime(TIMESTAMP_FORMAT)
            filename = OUTPUT_FILENAME_PATTERN.format(timestamp=timestamp)
            filepath = os.path.join(self.output_dir, filename)
            
            table = pa.Table.from_pylist(self.batch_data)
            
            pq.write_table(
                table, 
                filepath, 
                compression=PARQUET_COMPRESSION,
                use_dictionary=PARQUET_USE_DICTIONARY,
                write_statistics=PARQUET_WRITE_STATISTICS
            )
            
            print(f"{EMOJI_DISK} Dataset final: {len(self.batch_data):,} enregistrements -> {filename}")
            
        except Exception as e:
            print(f"{EMOJI_ERROR} Erreur écriture finale: {e}")
    
    async def _write_batch_if_needed(self, force=False):
        """Écrit le batch si nécessaire pour éviter l'OOM"""
        # Écrire tous les 10k panoramas ou si forcé
        BATCH_SIZE = MEGA_BATCH_SIZE
        
        if len(self.batch_data) >= BATCH_SIZE or force:
            if self.batch_data:
                timestamp = datetime.now().strftime(TIMESTAMP_FORMAT)
                filename = f"mapy_batch_{timestamp}_{len(self.batch_data)}.parquet"
                filepath = os.path.join(self.output_dir, filename)
                
                # Écriture asynchrone
                loop = asyncio.get_event_loop()
                await loop.run_in_executor(self.write_executor, self._write_batch_sync, filepath, self.batch_data.copy())
                
                print(f"{EMOJI_DISK} Batch écrit: {len(self.batch_data):,} → {filename}")
                
                # Compteurs globaux
                self.total_files_written += 1
                self.total_records_written += len(self.batch_data)
                
                # Libérer la mémoire
                self.batch_data.clear()
                gc.collect()

    def _write_batch_sync(self, filepath: str, data: List[Dict]):
        """Écriture synchrone du batch"""
        try:
            table = pa.Table.from_pylist(data)
            pq.write_table(
                table, 
                filepath,
                compression=PARQUET_COMPRESSION,
                use_dictionary=PARQUET_USE_DICTIONARY,
                write_statistics=PARQUET_WRITE_STATISTICS
            )
        except Exception as e:
            print(f"{EMOJI_ERROR} Erreur écriture batch: {e}")
    
    def print_proxy_metrics(self):
        """Métriques avec informations proxies"""
        current_time = time.time()
        elapsed = current_time - self.start_time
        current_count = len(self.visited)
        
        # Vitesses
        time_diff = current_time - self.last_progress_time
        count_diff = current_count - self.last_progress_count
        instant_rate = count_diff / time_diff if time_diff > 0 else 0
        avg_rate = current_count / elapsed if elapsed > 0 else 0
        
        # Efficacité réseau
        network_efficiency = self.network_calls / elapsed if elapsed > 0 else 0
        success_rate = (self.network_calls - self.timeouts - self.errors) / self.network_calls * 100 if self.network_calls > 0 else 0
        
        # Stats proxies
        proxy_stats = self.proxy_manager.get_stats()
        proxy_efficiency = proxy_stats['total_requests'] / elapsed if elapsed > 0 else 0
        
        # Métriques mémoire
        try:
            import psutil
            process = psutil.Process()
            memory_mb = process.memory_info().rss / 1024 / 1024
            memory_info = f" | RAM: {memory_mb:.0f}MB"
        except:
            memory_info = ""
        
        print(f"{EMOJI_ROCKET} {current_count:,} panos | {instant_rate:.0f}/s | {avg_rate:.0f}/s avg | "
              f"{network_efficiency:.0f} req/s | {success_rate:.1f}% | Queue: {len(self.to_visit):,} | "
              f"Batch: {len(self.batch_data)}/{MEGA_BATCH_SIZE} | Files: {self.total_files_written} | "
              f"Proxies: {proxy_efficiency:.0f} req/s | {elapsed:.0f}s{memory_info}")
        
        self.last_progress_time = current_time
        self.last_progress_count = current_count
    
    async def explore_with_proxies(self, start_points: List[Tuple[float, float]], target_count: int = None):
        """Exploration avec proxies multiples - MODE ILLIMITÉ supporté + CHECKPOINTS + MULTI-SOURCE BFS"""
        
        # Déterminer le mode
        unlimited_mode = target_count is None or target_count == float('inf') or UNLIMITED_MODE
        effective_target = float('inf') if unlimited_mode else target_count
        
        print(f"\n{EMOJI_ROCKET} EXPLORATION AVEC PROXIES MULTIPLES - BFS MULTI-SOURCE")
        print(f"{EMOJI_LOCATION} Points de départ: {len(start_points)} positions")
        for i, (lat, lon) in enumerate(start_points, 1):
            print(f"   Point #{i}: ({lat}, {lon})")
        
        if unlimited_mode:
            print(f"{EMOJI_TARGET} Objectif: COLLECTE TOTALE (tous les panoramas existants)")
            print(f"{EMOJI_WARNING} Critère d'arrêt: File d'attente vide (tous les panoramas accessibles traités)")
        else:
            print(f"{EMOJI_TARGET} Objectif: {target_count:,} panoramas")
            
        print(f"{EMOJI_GLOBE} {len(self.proxy_manager.proxies)} proxies simultanés")
        print(f"{EMOJI_LIGHTNING} Concurrence TOTALE: {self.total_max_concurrent}")
        
        # Tentative de reprise depuis checkpoint
        resumed_from_checkpoint = await self.load_from_checkpoint()
        
        # Variables pour mode illimité (simplifiées)
        # Plus besoin de tracking des itérations vides
        
        # Panoramas de départ (seulement si pas de reprise)
        if not resumed_from_checkpoint:
            print(f"\n{EMOJI_SEARCH} Recherche de panoramas aux points de départ...")
            successful_starts = 0
            
            for i, (start_lat, start_lon) in enumerate(start_points, 1):
                try:
                    start_pano = await find_panorama_async(start_lat, start_lon, links=FETCH_LINKS_DEFAULT, historical=FETCH_HISTORICAL_DEFAULT)
                    self.network_calls += 1
                    
                    if start_pano:
                        print(f"{EMOJI_SUCCESS} Point #{i}: Panorama {start_pano.id} trouvé ({len(start_pano.links or [])} liens, {len(start_pano.historical or [])} historiques)")
                        
                        # Vérifier si déjà visité (éviter les doublons entre points de départ)
                        if start_pano.id not in self.visited:
                            # Initialisation
                            self.visited.add(start_pano.id)
                            self.to_visit.append(start_pano.id)
                            successful_starts += 1
                            
                            # Enregistrement
                            self.exploration_order += 1
                            record = self.panorama_to_dict_minimal(start_pano, self.exploration_order)
                            self.batch_data.append(record)
                            
                            # Ajouter les voisins
                            if start_pano.links:
                                for link in start_pano.links:
                                    try:
                                        if link.pano.id not in self.visited:
                                            self.visited.add(link.pano.id)
                                            self.to_visit.append(link.pano.id)
                                    except:
                                        continue
                        else:
                            print(f"{EMOJI_WARNING} Point #{i}: Panorama {start_pano.id} déjà visité (doublon)")
                    else:
                        print(f"{EMOJI_ERROR} Point #{i}: Aucun panorama trouvé à ({start_lat}, {start_lon})")
                        
                except Exception as e:
                    print(f"{EMOJI_ERROR} Point #{i}: Erreur - {e}")
                    continue
            
            # Écriture par batch pour éviter l'OOM
            await self._write_batch_if_needed()
            
            if successful_starts == 0:
                print(f"{EMOJI_ERROR} Aucun panorama trouvé aux points de départ!")
                return None
            
            print(f"{EMOJI_SUCCESS} {successful_starts}/{len(start_points)} points de départ valides")
            print(f"{EMOJI_QUEUE} {len(self.to_visit):,} panoramas en file d'attente")
        
        print(f"\n{EMOJI_GLOBE} BFS AVEC PROXIES MULTIPLES...")
        if unlimited_mode:
            print(f"{EMOJI_WARNING} MODE ILLIMITÉ ACTIVÉ - Collecte jusqu'à épuisement des panoramas")
            print(f"{EMOJI_TARGET} Arrêt automatique quand la file d'attente sera vide")
        
        if resumed_from_checkpoint:
            print(f"{EMOJI_CYCLE} REPRISE DEPUIS CHECKPOINT - Continuation de la collecte")
        
        # Exploration BFS avec proxies
        try:
            while not self.should_stop:  # Vérification d'arrêt gracieux
                self.bfs_iterations += 1
                
                # Vérification des conditions d'arrêt
                current_count = len(self.visited)
                
                # Mode limité : arrêt classique
                if not unlimited_mode and current_count >= effective_target:
                    print(f"{EMOJI_SUCCESS} Objectif atteint: {current_count:,} panoramas")
                    break
                
                # Mode illimité : vérifications spéciales
                if unlimited_mode:
                    # Arrêt simple : vérifier si la file est vide
                    if not self.to_visit:
                        print(f"{EMOJI_SUCCESS} Tous les panoramas connus traités – file vide, fin du BFS")
                        break
                
                # Sortir si plus rien à explorer
                if not self.to_visit:
                    if unlimited_mode:
                        continue  # Attendre un peu plus en mode illimité
                    else:
                        break
                
                # Métriques fréquentes (basé sur itérations OU temps)
                progress_interval = UNLIMITED_PROGRESS_INTERVAL if unlimited_mode else PROGRESS_REPORT_INTERVAL
                current_time = time.time()
                time_since_display = current_time - self.last_time_display
                
                # Afficher si: assez d'itérations OU assez de temps écoulé
                if (self.bfs_iterations % progress_interval == 0) or (time_since_display >= PROGRESS_TIME_INTERVAL):
                    self.print_proxy_metrics()
                    self.last_time_display = current_time
                
                # Checkpoint spécial pour gros volumes
                if unlimited_mode and current_count > 0 and current_count % UNLIMITED_CHECKPOINT_INTERVAL == 0:
                    print(f"\n{EMOJI_PARTY} CHECKPOINT: {current_count:,} panoramas collectés!")
                    print(f"{EMOJI_DISK} Fichiers écrits: {self.total_files_written}")
                    print(f"{EMOJI_MEMORY} RAM: {self._get_memory_usage():.0f} MB")
                
                # Sauvegarde checkpoint si nécessaire
                await self.save_checkpoint_if_needed()
                
                # Batch pour saturer tous les proxies
                batch_size = min(
                    self.total_max_concurrent * BATCH_SIZE_MULTIPLIER,  # 2x la capacité théorique
                    len(self.to_visit),
                    max(1000, len(self.to_visit))  # Pas de limite en mode illimité
                )
                
                current_batch = []
                for _ in range(batch_size):
                    if self.to_visit:
                        current_batch.append(self.to_visit.popleft())
                
                if not current_batch:
                    continue
                
                # Randomiser l'ordre pour équilibrer la charge
                random.shuffle(current_batch)
                
                # Lancement MASSIF avec proxies rotatifs
                try:
                    tasks = [self.fetch_panorama_with_proxy(panoid) for panoid in current_batch]
                    panoramas = await asyncio.gather(*tasks, return_exceptions=True)
                    
                    # Traitement ultra-rapide
                    for pano in panoramas:
                        if isinstance(pano, Exception) or not pano:
                            continue
                        
                        # Ajout ultra-rapide
                        self.exploration_order += 1
                        record = self.panorama_to_dict_minimal(pano, self.exploration_order)
                        self.batch_data.append(record)
                        
                        # Écriture par batch pour éviter l'OOM
                        await self._write_batch_if_needed()
                        
                        # Expansion du graphe
                        if pano.links:
                            for link in pano.links:
                                try:
                                    if link.pano.id not in self.visited and link.pano.id not in self.failed_cache:
                                        self.visited.add(link.pano.id)
                                        self.to_visit.append(link.pano.id)
                                except:
                                    continue
                    
                    # Nettoyage agressif pour très gros volumes
                    if self.bfs_iterations % MEGA_CLEANUP_INTERVAL == 0:
                        self.processing_cache.clear()
                        
                        # Limiter failed_cache selon config
                        if len(self.failed_cache) > MEGA_FAILED_CACHE_MAX:
                            failed_list = list(self.failed_cache)
                            self.failed_cache.clear()
                            self.failed_cache.update(failed_list[-MEGA_FAILED_CACHE_KEEP:])
                        
                        # Limiter to_visit si trop gros (garde-fou)
                        if len(self.to_visit) > MEGA_TO_VISIT_LIMIT:
                            print(f"{EMOJI_WARNING} to_visit trop gros ({len(self.to_visit):,}), limitation...")
                            temp_list = []
                            for _ in range(MEGA_TO_VISIT_KEEP):
                                if self.to_visit:
                                    temp_list.append(self.to_visit.popleft())
                            self.to_visit.clear()
                            self.to_visit.extend(temp_list)
                        
                        gc.collect()
                    
                except Exception as e:
                    self.errors += 1
                    continue
                    
        except Exception as e:
            print(f"{EMOJI_ERROR} Erreur exploration: {e}")
        
        # Gestion de l'arrêt gracieux
        if self.should_stop:
            print(f"{EMOJI_WARNING} Arrêt gracieux demandé")
            await self._save_checkpoint_async()
            print(f"{EMOJI_SUCCESS} État sauvé - Reprise possible avec le même script")
        
        # Finalisation
        end_time = time.time()
        total_time = end_time - self.start_time
        
        speed = len(self.visited) / total_time if total_time > 0 else 0
        proxy_stats = self.proxy_manager.get_stats()
        
        return {
            'test_name': "PROXY_ULTRA_OPTIMIZED_UNLIMITED" if unlimited_mode else "PROXY_ULTRA_OPTIMIZED",
            'unlimited_mode': unlimited_mode,
            'resumed_from_checkpoint': resumed_from_checkpoint,
            'graceful_stop': self.should_stop,
            'num_proxies': len(self.proxy_manager.proxies),
            'total_max_concurrent': self.total_max_concurrent,
            'total_panoramas': len(self.visited),
            'total_time': total_time,
            'speed': speed,
            'network_calls': self.network_calls,
            'timeouts': self.timeouts,
            'errors': self.errors,
            'success_rate': (self.network_calls - self.timeouts - self.errors) / self.network_calls * 100 if self.network_calls > 0 else 0,
            'proxy_requests': proxy_stats['total_requests'],
            'proxy_success_rate': proxy_stats['success_rate'],
            'proxy_details': proxy_stats['proxy_details'],
            'completed': (unlimited_mode or len(self.visited) >= effective_target) and not self.should_stop,
            'output_dir': self.output_dir,
            'bfs_iterations': self.bfs_iterations,
            'failed_cache_size': len(self.failed_cache),
            'total_files_written': self.total_files_written,
            'total_records_written': self.total_records_written
        }

    def _get_memory_usage(self) -> float:
        """Obtient l'utilisation mémoire actuelle en MB"""
        try:
            import psutil
            process = psutil.Process()
            return process.memory_info().rss / 1024 / 1024
        except:
            return 0.0

def print_proxy_results(stats: Dict):
    """Résultats avec informations proxies - Support mode illimité + Checkpoints"""
    if not stats:
        return
    
    total_minutes = int(stats['total_time'] // 60)
    total_seconds = int(stats['total_time'] % 60)
    total_hours = stats['total_time'] / 3600
    
    print(f"\n{PROGRESS_SEPARATOR}")
    if stats.get('unlimited_mode'):
        print(f"{EMOJI_GLOBE} RÉSULTATS COLLECTE TOTALE (MODE ILLIMITÉ)")
    else:
        print(f"{EMOJI_GLOBE} RÉSULTATS AVEC PROXIES MULTIPLES")
    print(PROGRESS_SEPARATOR)
    
    print(f"{EMOJI_TARGET} Panoramas: {stats['total_panoramas']:,}")
    if total_hours >= 1:
        print(f"⏱️ Temps: {total_hours:.1f}h ({total_minutes}m {total_seconds}s)")
    else:
        print(f"⏱️ Temps: {total_minutes}m {total_seconds}s ({stats['total_time']:.1f}s)")
        
    print(f"{EMOJI_LIGHTNING} Vitesse: {stats['speed']:.1f} panoramas/seconde")
    print(f"{EMOJI_GLOBE} Proxies: {stats['num_proxies']} proxies")
    print(f"{EMOJI_LINK} Concurrence totale: {stats['total_max_concurrent']:,}")
    print(f"{EMOJI_CYCLE} Itérations BFS: {stats['bfs_iterations']:,}")
    print(f"{EMOJI_DISK} Fichiers écrits: {stats.get('total_files_written', 'N/A')}")
    print(f"{EMOJI_PACKAGE} Records totaux: {stats.get('total_records_written', 'N/A'):,}")
    print(f"{EMOJI_FOLDER} Sortie: {stats['output_dir']}")
    
    # Informations sur checkpoint et reprise
    if stats.get('resumed_from_checkpoint'):
        print(f"{EMOJI_CYCLE} Session reprise depuis checkpoint")
    if stats.get('graceful_stop'):
        print(f"{EMOJI_SUCCESS} Arrêt gracieux - État sauvegardé")
    
    # Efficacité des proxies
    proxy_throughput = stats['proxy_requests'] / stats['total_time']
    efficiency = stats['proxy_requests'] / stats['total_panoramas'] if stats['total_panoramas'] > 0 else 0
    
    print(f"\n{EMOJI_CHART} EFFICACITÉ PROXIES:")
    print(f"   • Débit proxy total: {proxy_throughput:.1f} requêtes/seconde")
    print(f"   • Requêtes proxy: {stats['proxy_requests']:,}")
    print(f"   • Succès proxy: {stats['proxy_success_rate']:.1f}%")
    print(f"   • Efficacité: {efficiency:.3f} requêtes/panorama")

    # Détail par proxy
    if 'proxy_details' in stats:
        print(f"\n{EMOJI_CHART} DÉTAIL PAR PROXY:")
        for idx, st in stats['proxy_details'].items():
            print(f"   • Proxy {idx+1}: {st['requests']:,} req | {st['errors']} err")
    
    print(f"\n{EMOJI_CHART_UP} PERFORMANCE:")
    print(f"   • Timeouts: {stats['timeouts']} ({stats['timeouts']/stats['network_calls']*100:.1f}%)")
    print(f"   • Erreurs: {stats['errors']} ({stats['errors']/stats['network_calls']*100:.1f}%)")
    print(f"   • Cache échecs: {stats['failed_cache_size']:,}")
    
    # Estimation de couverture pour mode illimité
    if stats.get('unlimited_mode'):
        print(f"\n{EMOJI_TARGET} COUVERTURE TOTALE:")
        print(f"   • Mode: Collecte exhaustive")
        status = 'Terminé' if stats['completed'] else ('Interrompu' if stats.get('graceful_stop') else 'En cours')
        print(f"   • Statut: {status}")
        if stats['total_panoramas'] > 1000000:
            print(f"   • Volume: {stats['total_panoramas']/1000000:.1f}M panoramas")
        print(f"   • Territoire: République Tchèque + connexions")
    
    # Comparaison avec script original
    if stats['total_panoramas'] > 0:
        improvement = (stats['speed'] / ORIGINAL_SCRIPT_SPEED - 1) * 100
        print(f"\n{EMOJI_ROCKET} AMÉLIORATION:")
        print(f"   • vs Script original: +{improvement:.0f}%")
        print(f"   • Facteur d'accélération: {stats['speed']/ORIGINAL_SCRIPT_SPEED:.1f}x")
    
    # Projection et estimation
    if not stats.get('unlimited_mode'):
        theoretical_max = proxy_throughput / efficiency if efficiency > 0 else 0
        print(f"\n{EMOJI_TARGET} PROJECTION THÉORIQUE:")
        print(f"   • Avec cette latence API: ~{theoretical_max:.0f} panoramas/seconde")
        print(f"   • Limitation: Bande passante API + latence réseau")

async def main():
    """Version proxy ultra-optimisée - MODE ILLIMITÉ + CHECKPOINTS + SESSIONS"""
    
    # Gestionnaire de sessions
    session_manager = SessionManager()
    
    # Vérifier s'il y a une session active
    if session_manager.has_active_session():
        session_info = session_manager.get_current_session_info()
        print(f"\n{EMOJI_CYCLE} SESSION ACTIVE DÉTECTÉE !")
        print(f"   • Session #{session_info['session_number']}")
        print(f"   • Créée le: {session_info.get('created_at', 'N/A')}")
        print(f"   • Dossier: {session_info['session_dir']}")
        
        # Vérifier les checkpoints dans cette session
        checkpoint_dir = session_info['checkpoint_dir']
        checkpoint_manager = CheckpointManager(checkpoint_dir)
        
        if checkpoint_manager.has_checkpoint():
            checkpoint_info = checkpoint_manager.get_checkpoint_info()
            print(f"   • Panoramas collectés: {checkpoint_info.get('total_panoramas', 0):,}")
            print(f"   • Queue: {checkpoint_info.get('queue_size', 0):,} panoramas en attente")
            
            if AUTO_RESUME:
                if RESUME_PROMPT:
                    response = input(f"\n{EMOJI_TARGET} Reprendre cette session ? (o/N): ").lower()
                    resume_choice = response in ['o', 'oui', 'y', 'yes']
                else:
                    resume_choice = True
                    print(f"{EMOJI_SUCCESS} Reprise automatique activée")
            else:
                resume_choice = False
                print(f"{EMOJI_WARNING} Reprise automatique désactivée - Nouvelle session")
        else:
            resume_choice = False
            print(f"{EMOJI_WARNING} Session active mais pas de checkpoint valide - Nouvelle session")
    else:
        resume_choice = False
        print(f"{EMOJI_SUCCESS} Nouvelle session - Aucune session active détectée")
    
    # Initialiser la session (nouvelle ou reprise)
    session_number, output_dir, checkpoint_dir = session_manager.initialize_session(resume_choice)
    
    # Configurer le système de logging pour cette session
    setup_logging(output_dir)
    print(f"{EMOJI_DISK} Log de session: {_log_manager.get_log_file()}")
    
    # Configuration pour collecte TOTALE avec points multiples
    if MULTI_SOURCE_MODE:
        START_POINTS = COVERAGE_STRATEGIES.get(COVERAGE_STRATEGY, DEFAULT_START_POINTS)
        print(f"{EMOJI_SEARCH} Stratégie de couverture: {COVERAGE_STRATEGY}")
    else:
        # Mode classique avec un seul point
        START_POINTS = [(DEFAULT_START_LATITUDE, DEFAULT_START_LONGITUDE)]
    
    # MODE ILLIMITÉ : supprimer la limite !
    TARGET_COUNT = None if UNLIMITED_MODE else DEFAULT_TARGET_COUNT
    
    MAX_CONCURRENT_PER_PROXY = ULTRA_MAX_CONCURRENT_PER_PROXY
    
    print(f"\n{EMOJI_FACTORY} COLLECTE MAPY.CZ - SESSION #{session_number}")
    if MULTI_SOURCE_MODE:
        print(f"{EMOJI_LOCATION} Mode multi-source: {len(START_POINTS)} points de départ")
        for i, (lat, lon) in enumerate(START_POINTS, 1):
            print(f"   Point #{i}: ({lat}, {lon})")
    else:
        print(f"{EMOJI_LOCATION} Départ: ({START_POINTS[0][0]}, {START_POINTS[0][1]})")
    
    if UNLIMITED_MODE:
        print(f"{EMOJI_TARGET} Objectif: COLLECTE TOTALE - TOUS LES PANORAMAS EXISTANTS")
        print(f"{EMOJI_WARNING} Pas de limite ! Collecte jusqu'à épuisement")
    else:
        print(f"{EMOJI_TARGET} Objectif: {TARGET_COUNT:,} panoramas")
        
    print(f"{EMOJI_GLOBE} {len(PROXIES)} proxies disponibles")
    print(f"{EMOJI_LIGHTNING} {MAX_CONCURRENT_PER_PROXY} connexions par proxy")
    print(f"{EMOJI_FIRE} TOTAL: {len(PROXIES) * MAX_CONCURRENT_PER_PROXY:,} connexions simultanées")
    print(f"{EMOJI_FOLDER} Sortie: {output_dir}/")
    print(f"{EMOJI_CHART} Collecte des données historiques activée")
    print(f"{EMOJI_MEMORY} Optimisations mémoire actives pour gros volumes")
    
    if ENABLE_CHECKPOINTS:
        print(f"{EMOJI_DISK} Checkpoints: {checkpoint_dir}/")
        print(f"{EMOJI_DISK} Sauvegarde tous les {CHECKPOINT_INTERVAL:,} panoramas")
        if resume_choice:
            print(f"{EMOJI_CYCLE} Mode reprise: Continuation session #{session_number}")
        else:
            print(f"{EMOJI_SUCCESS} Mode nouveau: Session #{session_number} créée")
    
    try:
        async with UltraProxyOptimizedCollector(
            output_dir,
            checkpoint_dir,
            PROXIES, 
            MAX_CONCURRENT_PER_PROXY,
            resume_from_checkpoint=resume_choice
        ) as collector:
            stats = await collector.explore_with_proxies(
                start_points=START_POINTS,
                target_count=TARGET_COUNT
            )
        
        print_proxy_results(stats)
        
        if stats:
            if stats.get('graceful_stop'):
                print(f"\n{EMOJI_SUCCESS} ARRÊT GRACIEUX - SESSION #{session_number}")
                print(f"{EMOJI_CYCLE} {stats['total_panoramas']:,} panoramas collectés")
                print(f"{EMOJI_DISK} Session sauvée dans: {output_dir}/")
                print(f"{EMOJI_DISK} Checkpoint sauvé dans: {checkpoint_dir}/")
                print(f"\n{EMOJI_TARGET} Pour reprendre: relancez simplement le même script")
                
            elif stats.get('unlimited_mode'):
                print(f"\n{EMOJI_PARTY} COLLECTE TOTALE TERMINÉE - SESSION #{session_number}!")
                print(f"{EMOJI_SUCCESS} {stats['total_panoramas']:,} panoramas collectés")
                print(f"{EMOJI_DISK} {stats['total_files_written']} fichiers écrits ({stats['total_records_written']:,} records)")
                print(f"{EMOJI_FOLDER} Données sauvées dans: {output_dir}/")
                print(f"{EMOJI_LIGHTNING} PERFORMANCE: {stats['speed']:.1f} panoramas/seconde")
                print(f"{EMOJI_TIME} Durée totale: {stats['total_time']/3600:.1f} heures")
                print(f"{EMOJI_GLOBE} Couverture: Exhaustive (tous les panoramas accessibles)")
                
                # Marquer la session comme terminée
                session_manager.mark_session_completed()
                
                if stats.get('resumed_from_checkpoint'):
                    print(f"{EMOJI_CYCLE} Cette session a repris depuis un checkpoint")
                    
            elif stats['completed']:
                print(f"\n{EMOJI_PARTY} TERMINÉ - SESSION #{session_number}!")
                print(f"{EMOJI_SUCCESS} {TARGET_COUNT:,} panoramas en {stats['total_time']:.1f}s")
                print(f"{EMOJI_FOLDER} Données sauvées dans: {output_dir}/")
                print(f"{EMOJI_LIGHTNING} PERFORMANCE FINALE: {stats['speed']:.1f} panoramas/seconde")
                
                # Marquer la session comme terminée
                session_manager.mark_session_completed()
            else:
                print(f"\n{EMOJI_WARNING} Incomplet - SESSION #{session_number}: {stats['total_panoramas']:,} panoramas")
                print(f"{EMOJI_FOLDER} Données partielles dans: {output_dir}/")
        
        # Finaliser le log de session
        if _log_manager:
            _log_manager.log_print(f"SESSION COMPLETED: {datetime.now().isoformat()}")
            print(f"{EMOJI_DISK} Log complet disponible: {_log_manager.get_log_file()}")
        
    except KeyboardInterrupt:
        print(f"\n{EMOJI_WARNING} Interruption clavier (Ctrl+C) - SESSION #{session_number}")
        print(f"{EMOJI_DISK} Session sauvée dans: {output_dir}/")
        print(f"{EMOJI_DISK} Checkpoint sauvé dans: {checkpoint_dir}/")
        print(f"{EMOJI_CYCLE} Reprise possible: relancez le script")
        
        # Finaliser le log en cas d'interruption
        if _log_manager:
            _log_manager.log_print(f"SESSION INTERRUPTED: {datetime.now().isoformat()}")
            print(f"{EMOJI_DISK} Log disponible: {_log_manager.get_log_file()}")
        
        sys.exit(1)
    except Exception as e:
        print(f"\n{EMOJI_ERROR} Erreur - SESSION #{session_number}: {e}")
        print(f"{EMOJI_DISK} Session dans: {output_dir}/")
        print(f"{EMOJI_DISK} Checkpoint dans: {checkpoint_dir}/")
        print(f"{EMOJI_CYCLE} Tentez une reprise: relancez le script")
        import traceback
        traceback.print_exc()
        
        # Finaliser le log en cas d'erreur
        if _log_manager:
            _log_manager.log_print(f"SESSION ERROR: {datetime.now().isoformat()} - {e}")
            _log_manager.log_print(traceback.format_exc())
            print(f"{EMOJI_DISK} Log avec erreur: {_log_manager.get_log_file()}")
        
        sys.exit(1)

if __name__ == "__main__":
    # Utiliser uvloop si disponible
    try:
        import uvloop
        uvloop.install()
        print(f"{EMOJI_ROCKET} uvloop activé pour performance maximale")
    except ImportError:
        pass
    
    # Optimisations Python
    import sys
    sys.setswitchinterval(SWITCH_INTERVAL)
    
    asyncio.run(main())
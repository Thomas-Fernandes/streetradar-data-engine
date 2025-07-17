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
import sqlite3
import threading
from contextlib import contextmanager

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

class SQLiteStateManager:
    """Gestionnaire d'état persistant avec SQLite pour éviter les OOM et permettre la reprise"""
    
    def __init__(self, db_path: str = "exploration_state.db"):
        self.db_path = db_path
        self.lock = threading.Lock()
        self._init_database()
        print(f"{EMOJI_DISK} Base de données d'état: {db_path}")
    
    def _init_database(self):
        """Initialise les tables SQLite"""
        with self._get_connection() as conn:
            # Table pour les panoramas visités
            conn.execute("""
                CREATE TABLE IF NOT EXISTS visited (
                    pano_id TEXT PRIMARY KEY,
                    timestamp REAL,
                    exploration_order INTEGER
                )
            """)
            
            # Table pour la queue d'exploration
            conn.execute("""
                CREATE TABLE IF NOT EXISTS queue (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    pano_id TEXT UNIQUE,
                    pano_data TEXT,
                    priority INTEGER DEFAULT 0,
                    added_timestamp REAL
                )
            """)
            
            # Table pour les échecs (cache des IDs qui ont échoué)
            conn.execute("""
                CREATE TABLE IF NOT EXISTS failed (
                    pano_id TEXT PRIMARY KEY,
                    timestamp REAL,
                    retry_count INTEGER DEFAULT 1
                )
            """)
            
            # Index pour les performances
            conn.execute("CREATE INDEX IF NOT EXISTS idx_queue_priority ON queue(priority, id)")
            conn.execute("CREATE INDEX IF NOT EXISTS idx_visited_order ON visited(exploration_order)")
            conn.execute("CREATE INDEX IF NOT EXISTS idx_failed_timestamp ON failed(timestamp)")
            
            conn.commit()
    
    @contextmanager
    def _get_connection(self):
        """Context manager pour les connexions SQLite thread-safe"""
        conn = sqlite3.connect(self.db_path, timeout=30.0)
        conn.execute("PRAGMA journal_mode=WAL")  # Pour la performance
        conn.execute("PRAGMA synchronous=NORMAL")
        conn.execute("PRAGMA cache_size=10000")
        try:
            yield conn
        finally:
            conn.close()
    
    def is_visited(self, pano_id: str) -> bool:
        """Vérifie si un panorama a été visité"""
        with self._get_connection() as conn:
            cursor = conn.execute("SELECT 1 FROM visited WHERE pano_id = ?", (pano_id,))
            return cursor.fetchone() is not None
    
    def mark_visited(self, pano_id: str, exploration_order: int):
        """Marque un panorama comme visité"""
        with self.lock:
            with self._get_connection() as conn:
                conn.execute("""
                    INSERT OR REPLACE INTO visited (pano_id, timestamp, exploration_order)
                    VALUES (?, ?, ?)
                """, (pano_id, time.time(), exploration_order))
                conn.commit()
    
    def add_to_queue(self, panorama_data: List[Dict], priority: int = 0):
        """Ajoute des panoramas à la queue avec leurs métadonnées complètes (ignore les doublons)"""
        if not panorama_data:
            return
        
        with self.lock:
            with self._get_connection() as conn:
                timestamp = time.time()
                # Stocker les données complètes en JSON
                data = [(pano_data['id'], json.dumps(pano_data), priority, timestamp) for pano_data in panorama_data]
                conn.executemany("""
                    INSERT OR IGNORE INTO queue (pano_id, pano_data, priority, added_timestamp)
                    VALUES (?, ?, ?, ?)
                """, data)
                conn.commit()
    
    def get_batch_from_queue(self, batch_size: int) -> List[Dict]:
        """Récupère un batch de panoramas de la queue avec leurs données"""
        with self.lock:
            with self._get_connection() as conn:
                cursor = conn.execute("""
                    SELECT pano_id, pano_data, id FROM queue 
                    ORDER BY priority DESC, id ASC 
                    LIMIT ?
                """, (batch_size,))
                
                results = cursor.fetchall()
                if not results:
                    return []
                
                panorama_data = []
                queue_ids = []
                for row in results:
                    try:
                        pano_data = json.loads(row[1])
                        panorama_data.append(pano_data)
                        queue_ids.append(row[2])
                    except json.JSONDecodeError:
                        # Données corrompues, supprimer
                        queue_ids.append(row[2])
                
                # Supprimer les éléments récupérés
                conn.executemany("DELETE FROM queue WHERE id = ?", [(qid,) for qid in queue_ids])
                conn.commit()
                
                return panorama_data
    
    def queue_size(self) -> int:
        """Retourne la taille de la queue"""
        with self._get_connection() as conn:
            cursor = conn.execute("SELECT COUNT(*) FROM queue")
            return cursor.fetchone()[0]
    
    def visited_count(self) -> int:
        """Retourne le nombre de panoramas visités"""
        with self._get_connection() as conn:
            cursor = conn.execute("SELECT COUNT(*) FROM visited")
            return cursor.fetchone()[0]
    
    def mark_failed(self, pano_id: str):
        """Marque un panorama comme ayant échoué"""
        with self.lock:
            with self._get_connection() as conn:
                conn.execute("""
                    INSERT OR REPLACE INTO failed (pano_id, timestamp, retry_count)
                    VALUES (?, ?, COALESCE((SELECT retry_count + 1 FROM failed WHERE pano_id = ?), 1))
                """, (pano_id, time.time(), pano_id))
                conn.commit()
    
    def is_failed(self, pano_id: str, max_retries: int = 3) -> bool:
        """Vérifie si un panorama a échoué trop de fois"""
        with self._get_connection() as conn:
            cursor = conn.execute("""
                SELECT retry_count FROM failed WHERE pano_id = ?
            """, (pano_id,))
            result = cursor.fetchone()
            return result is not None and result[0] >= max_retries
    
    def get_exploration_stats(self) -> Dict:
        """Retourne les statistiques d'exploration"""
        with self._get_connection() as conn:
            cursor = conn.execute("""
                SELECT 
                    (SELECT COUNT(*) FROM visited) as visited_count,
                    (SELECT COUNT(*) FROM queue) as queue_size,
                    (SELECT COUNT(*) FROM failed) as failed_count,
                    (SELECT MAX(exploration_order) FROM visited) as max_order
            """)
            row = cursor.fetchone()
            
            return {
                'visited_count': row[0] or 0,
                'queue_size': row[1] or 0,
                'failed_count': row[2] or 0,
                'max_exploration_order': row[3] or 0
            }
    
    def cleanup_old_failed(self, days: int = 7):
        """Nettoie les anciens échecs pour permettre de réessayer"""
        cutoff_time = time.time() - (days * 24 * 3600)
        with self.lock:
            with self._get_connection() as conn:
                cursor = conn.execute("DELETE FROM failed WHERE timestamp < ?", (cutoff_time,))
                deleted = cursor.rowcount
                conn.commit()
                if deleted > 0:
                    print(f"{EMOJI_CLEAN} Nettoyage: {deleted} anciens échecs supprimés")
    
    def can_resume(self) -> bool:
        """Vérifie si une exploration peut être reprise"""
        stats = self.get_exploration_stats()
        return stats['visited_count'] > 0 or stats['queue_size'] > 0
    
    def print_resume_info(self):
        """Affiche les informations de reprise"""
        stats = self.get_exploration_stats()
        if stats['visited_count'] > 0:
            print(f"{EMOJI_CYCLE} REPRISE DÉTECTÉE:")
            print(f"   • Panoramas déjà visités: {stats['visited_count']:,}")
            print(f"   • Queue restante: {stats['queue_size']:,}")
            print(f"   • Échecs enregistrés: {stats['failed_count']:,}")
            print(f"   • Dernier ordre d'exploration: {stats['max_exploration_order']:,}")
            return True
        return False

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
    """Collecteur avec optimisations proxy pour saturer l'API + persistance SQLite"""
    
    def __init__(self, output_dir: str, proxies: List[Tuple], max_concurrent_per_proxy: int = DEFAULT_MAX_CONCURRENT_PER_PROXY, db_path: str = "exploration_state.db"):
        self.output_dir = output_dir
        self.proxy_manager = ProxyManager(proxies)
        
        # Configurer le proxy manager pour streetlevel
        set_proxy_manager(self.proxy_manager)
        
        self.proxy_connector = ProxyRotatingConnector(self.proxy_manager, max_concurrent_per_proxy)
        
        # Calcul de la concurrence totale
        self.total_max_concurrent = len(proxies) * max_concurrent_per_proxy
        
        # Gestionnaire d'état persistant SQLite
        self.state_manager = SQLiteStateManager(db_path)
        
        # Métriques (gardées en mémoire pour performance)
        self.network_calls = 0
        self.timeouts = 0
        self.errors = 0
        self.start_time = time.time()
        
        # Optimisations
        self.semaphore = asyncio.Semaphore(self.total_max_concurrent)
        
        # Cache temporaire en mémoire pour déduplication dans le batch actuel
        self.processing_cache: Set[str] = set()
        
        # Batch writer optimisé
        self.batch_data = []
        self.write_executor = ThreadPoolExecutor(max_workers=FILE_WRITER_MAX_WORKERS)
        
        # Métriques de performance
        self.last_progress_time = time.time()
        self.last_progress_count = 0
        self.bfs_iterations = 0
        
        # Créer le dossier
        os.makedirs(output_dir, exist_ok=True)
        
        print(f"{EMOJI_ROCKET} COLLECTEUR PROXY ULTRA-OPTIMISÉ + SQLITE")
        print(f"{EMOJI_GLOBE} {len(proxies)} proxies × {max_concurrent_per_proxy} = {self.total_max_concurrent} connexions simultanées")
        print(f"{EMOJI_FOLDER} Sortie: {output_dir}/")
        print(f"{EMOJI_DISK} Base de données: {db_path}")
        print(f"{EMOJI_PACKAGE} Utilisation de streetlevel local")
    
    def link_to_dict(self, link) -> Dict:
        """Convertit un objet Link en dictionnaire pour stockage JSON"""
        return {
            'id': link.pano.id,
            'lat': link.pano.lat,
            'lon': link.pano.lon,
            'date': link.pano.date.isoformat() if hasattr(link.pano.date, 'isoformat') else str(link.pano.date),
            'provider': link.pano.provider,
            'elevation': link.pano.elevation,
            'direction': link.direction
        }
    
    def dict_to_panorama_minimal(self, pano_dict: Dict, exploration_order: int) -> Dict:
        """Convertit un dictionnaire en enregistrement minimal pour Parquet"""
        return {
            "id": pano_dict['id'],
            "lat": pano_dict['lat'],
            "lon": pano_dict['lon'],
            "date": pano_dict['date'],
            "provider": pano_dict['provider'],
            "elevation": pano_dict['elevation'],
            "permalink": f"https://en.mapy.cz/zakladni?pano=1&pid={pano_dict['id']}&x={pano_dict['lon']}&y={pano_dict['lat']}&z=17&ovl=8",
            "order": exploration_order,
            "links": 0,  # Sera mis à jour après récupération des liens
            "link_ids": [],  # Sera mis à jour après récupération des liens
            "historical_count": 0,  # Les données historiques ne sont pas dans les liens
            "historical": [],
            "time": time.time()
        }
    
    async def __aenter__(self):
        return self
    
    async def __aexit__(self, exc_type, exc_val, exc_tb):
        await self.proxy_connector.close_all_sessions()
        
        # Écriture finale
        if self.batch_data:
            await self._write_final_batch()
        
        self.write_executor.shutdown(wait=True)
        
        # Statistiques finales des proxies
        proxy_stats = self.proxy_manager.get_stats()
        state_stats = self.state_manager.get_exploration_stats()
        print(f"\n{EMOJI_CHART} STATISTIQUES FINALES:")
        print(f"   • Total requêtes proxy: {proxy_stats['total_requests']:,}")
        print(f"   • Taux de succès: {proxy_stats['success_rate']:.1f}%")
        print(f"   • Panoramas en base: {state_stats['visited_count']:,}")
        print(f"   • Queue restante: {state_stats['queue_size']:,}")
    
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
        """Récupération avec proxies rotatifs + vérification SQLite"""
        # Vérifications rapides
        if panoid in self.processing_cache:
            return None
        
        if self.state_manager.is_visited(panoid) or self.state_manager.is_failed(panoid):
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
                
                # Si l'API retourne None (panorama inexistant), marquer comme failed
                if pano is None:
                    self.state_manager.mark_failed(panoid)
                    return None
                
                return pano
                
            except asyncio.TimeoutError:
                self.timeouts += 1
                self.state_manager.mark_failed(panoid)
                return None
            except Exception as e:
                self.errors += 1
                self.state_manager.mark_failed(panoid)
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
    
    def print_proxy_metrics(self):
        """Métriques avec informations proxies + SQLite"""
        current_time = time.time()
        elapsed = current_time - self.start_time
        
        # Statistiques depuis SQLite
        state_stats = self.state_manager.get_exploration_stats()
        current_count = state_stats['visited_count']
        queue_size = state_stats['queue_size']
        
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
        
        print(f"{EMOJI_ROCKET} {current_count:,} panos | {instant_rate:.0f}/s | {avg_rate:.0f}/s avg | "
              f"{network_efficiency:.0f} req/s | {success_rate:.1f}% | Queue: {queue_size:,} | "
              f"Proxies: {proxy_efficiency:.0f} req/s | {elapsed:.0f}s")
        
        self.last_progress_time = current_time
        self.last_progress_count = current_count
    
    def initialize_or_resume(self, start_lat: float, start_lon: float) -> Optional[Dict]:
        """Initialise l'exploration ou reprend depuis SQLite"""
        # Vérifier s'il y a quelque chose à reprendre
        if self.state_manager.can_resume():
            self.state_manager.print_resume_info()
            
            # Nettoyer les anciens échecs pour permettre des re-tentatives
            self.state_manager.cleanup_old_failed()
            
            # Récupérer les statistiques actuelles
            state_stats = self.state_manager.get_exploration_stats()
            if state_stats['max_exploration_order'] > 0:
                self.exploration_order = state_stats['max_exploration_order']
            else:
                self.exploration_order = 0
            
            print(f"{EMOJI_SUCCESS} Reprise de l'exploration depuis l'ordre {self.exploration_order}")
            return True
        
        # Nouvelle exploration
        print(f"{EMOJI_NEW} Nouvelle exploration depuis ({start_lat}, {start_lon})")
        return False
    
    async def start_new_exploration(self, start_lat: float, start_lon: float) -> bool:
        """Démarre une nouvelle exploration depuis les coordonnées"""
        try:
            start_pano = await find_panorama_async(start_lat, start_lon, links=FETCH_LINKS_DEFAULT, historical=FETCH_HISTORICAL_DEFAULT)
            self.network_calls += 1
            
            if not start_pano:
                print(f"{EMOJI_ERROR} Aucun panorama trouvé!")
                return False
            
            print(f"{EMOJI_SUCCESS} Départ: {start_pano.id} ({len(start_pano.links or [])} liens, {len(start_pano.historical or [])} historiques)")
            
            # Initialisation de l'ordre d'exploration
            self.exploration_order = 1
            
            # Marquer comme visité dans SQLite
            self.state_manager.mark_visited(start_pano.id, self.exploration_order)
            
            # Enregistrement en Parquet
            record = self.panorama_to_dict_minimal(start_pano, self.exploration_order)
            self.batch_data.append(record)
            
            # Ajouter les voisins à la queue SQLite
            if start_pano.links:
                link_data = []
                for link in start_pano.links:
                    try:
                        if not self.state_manager.is_visited(link.pano.id) and not self.state_manager.is_failed(link.pano.id):
                            link_dict = self.link_to_dict(link)
                            link_data.append(link_dict)
                    except:
                        continue
                
                if link_data:
                    self.state_manager.add_to_queue(link_data)
                    print(f"{EMOJI_PLUS} {len(link_data)} nouveaux panoramas ajoutés à la queue")
            
            return True
            
        except Exception as e:
            print(f"{EMOJI_ERROR} Erreur démarrage: {e}")
            return False
    
    async def explore_with_proxies(self, start_lat: float, start_lon: float, target_count: Optional[int] = None):
        """Exploration avec proxies multiples + SQLite - exploration complète par défaut ou limitée pour tests"""
        print(f"\n{EMOJI_ROCKET} EXPLORATION AVEC PROXIES MULTIPLES + SQLITE")
        print(f"{EMOJI_LOCATION} Départ: ({start_lat}, {start_lon})")
        
        if target_count is not None:
            print(f"{EMOJI_TARGET} Mode test - Limite: {target_count:,} panoramas")
        else:
            print(f"{EMOJI_TARGET} Mode complet - Exploration jusqu'à épuisement")
            
        print(f"{EMOJI_GLOBE} {len(self.proxy_manager.proxies)} proxies simultanés")
        print(f"{EMOJI_LIGHTNING} Concurrence TOTALE: {self.total_max_concurrent}")
        
        # Initialisation ou reprise
        is_resume = self.initialize_or_resume(start_lat, start_lon)
        
        if not is_resume:
            # Nouvelle exploration
            if not await self.start_new_exploration(start_lat, start_lon):
                return None
        else:
            # Récupérer l'ordre d'exploration actuel
            state_stats = self.state_manager.get_exploration_stats()
            self.exploration_order = state_stats['max_exploration_order']
        
        print(f"\n{EMOJI_GLOBE} BFS AVEC PROXIES MULTIPLES + SQLITE...")
        
        # Exploration BFS avec SQLite
        try:
            while True:
                # Vérifier les conditions d'arrêt
                state_stats = self.state_manager.get_exploration_stats()
                current_visited = state_stats['visited_count']
                current_queue_size = state_stats['queue_size']
                
                # Vérifier la limite seulement si elle est définie
                if target_count is not None and current_visited >= target_count:
                    print(f"{EMOJI_SUCCESS} Limite de test atteinte: {target_count:,} panoramas")
                    break
                
                # Plus rien à explorer
                if current_queue_size == 0:
                    if target_count is None:
                        print(f"{EMOJI_SUCCESS} Exploration complète terminée - Plus de panoramas à explorer")
                    else:
                        print(f"{EMOJI_WARNING} Queue vide avant d'atteindre la limite")
                    break
                
                self.bfs_iterations += 1
                
                # Métriques fréquentes
                if self.bfs_iterations % PROGRESS_REPORT_INTERVAL == 0:
                    self.print_proxy_metrics()
                
                # Calcul dynamique de la taille du batch
                remaining_slots = None
                if target_count is not None:
                    remaining_slots = target_count - current_visited
                
                batch_size = min(
                    self.total_max_concurrent * BATCH_SIZE_MULTIPLIER,  # 2x la capacité théorique
                    current_queue_size,
                    remaining_slots if remaining_slots is not None else float('inf')
                )
                
                # Récupérer un batch depuis SQLite
                current_batch = self.state_manager.get_batch_from_queue(int(batch_size))
                
                if not current_batch:
                    break
                
                # Traitement direct des données sans appels API supplémentaires
                try:
                    new_links = []
                    successful_panos = 0
                    
                    for pano_dict in current_batch:
                        try:
                            pano_id = pano_dict['id']
                            
                            # Vérifier si déjà traité
                            if self.state_manager.is_visited(pano_id) or self.state_manager.is_failed(pano_id):
                                continue
                            
                            # Vérifier la limite avant d'ajouter
                            current_visited = self.state_manager.visited_count()
                            if target_count is not None and current_visited >= target_count:
                                break
                            
                            successful_panos += 1
                            
                            # Ajout ultra-rapide
                            self.exploration_order += 1
                            
                            # Marquer comme visité dans SQLite
                            self.state_manager.mark_visited(pano_id, self.exploration_order)
                            
                            # Enregistrer en Parquet
                            record = self.dict_to_panorama_minimal(pano_dict, self.exploration_order)
                            self.batch_data.append(record)
                            
                            # Récupérer les liens de ce panorama via API pour expansion du graphe
                            try:
                                from streetlevel.mapy import get_links_async
                                links = await get_links_async(pano_id)
                                if links:
                                    for link in links:
                                        try:
                                            if not self.state_manager.is_visited(link.pano.id) and not self.state_manager.is_failed(link.pano.id):
                                                link_dict = self.link_to_dict(link)
                                                new_links.append(link_dict)
                                        except Exception:
                                            continue
                            except Exception:
                                # Pas critique, on continue sans les liens
                                pass
                                
                        except Exception:
                            continue
                    
                    # Ajouter les nouveaux liens à la queue SQLite en batch
                    if new_links:
                        self.state_manager.add_to_queue(new_links)
                    
                    # Nettoyage périodique
                    if self.bfs_iterations % CACHE_CLEANUP_INTERVAL == 0:
                        self.processing_cache.clear()
                        self.state_manager.cleanup_old_failed()
                        gc.collect()
                    
                except Exception as e:
                    self.errors += 1
                    continue
                
        except Exception as e:
            print(f"{EMOJI_ERROR} Erreur exploration: {e}")
        
        # Finalisation
        end_time = time.time()
        total_time = end_time - self.start_time
        
        # Statistiques finales depuis SQLite
        final_stats = self.state_manager.get_exploration_stats()
        speed = final_stats['visited_count'] / total_time if total_time > 0 else 0
        proxy_stats = self.proxy_manager.get_stats()
        
        # Déterminer si l'exploration est complète
        is_complete = False
        if target_count is not None:
            is_complete = final_stats['visited_count'] >= target_count
        else:
            is_complete = final_stats['queue_size'] == 0  # Plus de panoramas à explorer
        
        return {
            'test_name': "PROXY_ULTRA_OPTIMIZED_SQLITE",
            'num_proxies': len(self.proxy_manager.proxies),
            'total_max_concurrent': self.total_max_concurrent,
            'total_panoramas': final_stats['visited_count'],
            'total_time': total_time,
            'speed': speed,
            'network_calls': self.network_calls,
            'timeouts': self.timeouts,
            'errors': self.errors,
            'success_rate': (self.network_calls - self.timeouts - self.errors) / self.network_calls * 100 if self.network_calls > 0 else 0,
            'proxy_requests': proxy_stats['total_requests'],
            'proxy_success_rate': proxy_stats['success_rate'],
            'proxy_details': proxy_stats['proxy_details'],
            'completed': is_complete,
            'exploration_mode': 'limited' if target_count is not None else 'complete',
            'target_count': target_count,
            'output_dir': self.output_dir,
            'bfs_iterations': self.bfs_iterations,
            'failed_count': final_stats['failed_count'],
            'remaining_queue': final_stats['queue_size'],
            'db_path': self.state_manager.db_path,
            'resumable': True
        }

def print_proxy_results(stats: Dict):
    """Résultats avec informations proxies + SQLite"""
    if not stats:
        return
    
    total_minutes = int(stats['total_time'] // 60)
    total_seconds = int(stats['total_time'] % 60)
    
    print(f"\n{PROGRESS_SEPARATOR}")
    mode_text = "LIMITÉE (TEST)" if stats.get('exploration_mode') == 'limited' else "COMPLÈTE"
    print(f"{EMOJI_GLOBE} RÉSULTATS EXPLORATION {mode_text} + SQLITE")
    print(PROGRESS_SEPARATOR)
    print(f"{EMOJI_TARGET} Panoramas: {stats['total_panoramas']:,}")
    
    if stats.get('exploration_mode') == 'limited':
        print(f"{EMOJI_TARGET} Limite test: {stats.get('target_count', 'N/A'):,}")
    else:
        if stats.get('remaining_queue', 0) > 0:
            print(f"{EMOJI_WARNING} Queue restante: {stats['remaining_queue']:,} (exploration interrompue)")
        else:
            print(f"{EMOJI_SUCCESS} Exploration complète - Aucun panorama restant")
    
    print(f"⏱️ Temps: {total_minutes}m {total_seconds}s ({stats['total_time']:.1f}s)")
    print(f"{EMOJI_LIGHTNING} Vitesse: {stats['speed']:.1f} panoramas/seconde")
    print(f"{EMOJI_GLOBE} Proxies: {stats['num_proxies']} proxies")
    print(f"{EMOJI_LINK} Concurrence totale: {stats['total_max_concurrent']:,}")
    print(f"{EMOJI_CYCLE} Itérations BFS: {stats['bfs_iterations']:,}")
    print(f"{EMOJI_DISK} Sortie: {stats['output_dir']}")
    print(f"{EMOJI_DISK} Base de données: {stats.get('db_path', 'N/A')}")
    
    # Informations de persistance
    print(f"\n{EMOJI_DISK} PERSISTANCE SQLITE:")
    print(f"   • Panoramas en base: {stats['total_panoramas']:,}")
    print(f"   • Queue restante: {stats['remaining_queue']:,}")
    print(f"   • Échecs enregistrés: {stats.get('failed_count', 0):,}")
    if stats.get('resumable', False):
        print(f"   • ✅ Script reprennable automatiquement")
        if stats['remaining_queue'] > 0:
            print(f"   • 🔄 Pour reprendre: relancer le script")
    
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
    if stats['network_calls'] > 0:
        print(f"   • Timeouts: {stats['timeouts']} ({stats['timeouts']/stats['network_calls']*100:.1f}%)")
        print(f"   • Erreurs: {stats['errors']} ({stats['errors']/stats['network_calls']*100:.1f}%)")
    else:
        print(f"   • Timeouts: {stats['timeouts']} (0.0%)")
        print(f"   • Erreurs: {stats['errors']} (0.0%)")
    
    # Comparaison
    improvement = (stats['speed'] / ORIGINAL_SCRIPT_SPEED - 1) * 100
    print(f"\n{EMOJI_ROCKET} AMÉLIORATION:")
    print(f"   • vs Script original: +{improvement:.0f}%")
    print(f"   • Facteur d'accélération: {stats['speed']/ORIGINAL_SCRIPT_SPEED:.1f}x")
    print(f"   • Avantages SQLite: 🚫 OOM, ♻️ Reprise auto")
    
    # Projection selon le mode
    theoretical_max = proxy_throughput / efficiency if efficiency > 0 else 0
    print(f"\n{EMOJI_TARGET} PROJECTION:")
    if stats.get('exploration_mode') == 'limited':
        print(f"   • Pour exploration complète: ~{theoretical_max:.0f} panoramas/seconde")
        if stats.get('remaining_queue', 0) > 0:
            estimated_time = stats['remaining_queue'] / theoretical_max if theoretical_max > 0 else 0
            print(f"   • Temps estimé complet: ~{estimated_time/3600:.1f}h")
    else:
        print(f"   • Débit théorique max: ~{theoretical_max:.0f} panoramas/seconde")
        if stats['remaining_queue'] > 0:
            estimated_time = stats['remaining_queue'] / theoretical_max if theoretical_max > 0 else 0
            print(f"   • Temps estimé pour finir: ~{estimated_time/3600:.1f}h")
        print(f"   • Limitation: Bande passante API + latence réseau")

async def main(limit: Optional[int] = None):
    """Version proxy ultra-optimisée avec exploration complète par défaut + SQLite"""
    # Configuration avec TOUS les proxies
    PRAGUE_LAT = DEFAULT_START_LATITUDE
    PRAGUE_LON = DEFAULT_START_LONGITUDE
    MAX_CONCURRENT_PER_PROXY = ULTRA_MAX_CONCURRENT_PER_PROXY
    OUTPUT_DIR = DEFAULT_OUTPUT_DIR
    DB_PATH = "exploration_state.db"
    
    print(f"{EMOJI_FACTORY} COLLECTE MAPY.CZ - PROXIES MULTIPLES + SQLITE")
    print(f"{EMOJI_LOCATION} Départ: Prague ({PRAGUE_LAT}, {PRAGUE_LON})")
    
    if limit is not None:
        print(f"{EMOJI_TARGET} Mode test - Limite: {limit:,} panoramas")
    else:
        print(f"{EMOJI_TARGET} Mode complet - Exploration de TOUTE la République tchèque")
        
    print(f"{EMOJI_GLOBE} {len(PROXIES)} proxies disponibles")
    print(f"{EMOJI_LIGHTNING} {MAX_CONCURRENT_PER_PROXY} connexions par proxy")
    print(f"{EMOJI_FIRE} TOTAL: {len(PROXIES) * MAX_CONCURRENT_PER_PROXY:,} connexions simultanées")
    print(f"{EMOJI_FOLDER} Sortie: {OUTPUT_DIR}/")
    print(f"{EMOJI_DISK} Base de données: {DB_PATH}")
    print(f"{EMOJI_CHART} Collecte des données historiques activée")
    print(f"{EMOJI_CYCLE} Reprise automatique en cas d'interruption")
    
    try:
        async with UltraProxyOptimizedCollector(OUTPUT_DIR, PROXIES, MAX_CONCURRENT_PER_PROXY, DB_PATH) as collector:
            stats = await collector.explore_with_proxies(
                start_lat=PRAGUE_LAT,
                start_lon=PRAGUE_LON,
                target_count=limit
            )
        
        print_proxy_results(stats)
        
        if stats and stats['completed']:
            if limit is not None:
                print(f"\n{EMOJI_PARTY} TEST TERMINÉ! {limit:,} panoramas en {stats['total_time']:.1f}s")
            else:
                print(f"\n{EMOJI_PARTY} EXPLORATION COMPLÈTE TERMINÉE!")
                print(f"{EMOJI_SUCCESS} {stats['total_panoramas']:,} panoramas collectés en {stats['total_time']:.1f}s")
                print(f"{EMOJI_GLOBE} Toute la République tchèque explorée!")
            print(f"{EMOJI_LIGHTNING} PERFORMANCE FINALE: {stats['speed']:.1f} panoramas/seconde")
            print(f"{EMOJI_GLOBE} PROXIES: {stats['num_proxies']} proxies utilisés efficacement")
        else:
            if limit is not None:
                print(f"\n{EMOJI_WARNING} Test incomplet: {stats['total_panoramas']:,} panoramas (objectif: {limit:,})")
            else:
                print(f"\n{EMOJI_WARNING} Exploration interrompue: {stats['total_panoramas']:,} panoramas")
                print(f"{EMOJI_INFO} Queue restante: {stats['remaining_queue']:,} panoramas")
            
            # Instructions de reprise
            if stats.get('resumable', False) and stats['remaining_queue'] > 0:
                print(f"\n{EMOJI_CYCLE} REPRISE AUTOMATIQUE DISPONIBLE:")
                print(f"   • Pour continuer l'exploration, relancez simplement:")
                if limit is not None:
                    print(f"   • python {sys.argv[0]} {limit}")
                else:
                    print(f"   • python {sys.argv[0]}")
                print(f"   • Le script reprendra automatiquement là où il s'est arrêté")
        
    except KeyboardInterrupt:
        print(f"\n{EMOJI_ERROR} Interrompu par l'utilisateur")
        print(f"{EMOJI_INFO} L'état a été sauvegardé dans {DB_PATH}")
        print(f"{EMOJI_CYCLE} Pour reprendre, relancez le script avec les mêmes arguments")
        sys.exit(1)
    except Exception as e:
        print(f"\n{EMOJI_ERROR} Erreur: {e}")
        import traceback
        traceback.print_exc()
        print(f"{EMOJI_INFO} L'état a été sauvegardé dans {DB_PATH}")
        print(f"{EMOJI_CYCLE} Pour reprendre, relancez le script avec les mêmes arguments")
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
    
    # Gestion des arguments de ligne de commande
    limit = None
    if len(sys.argv) > 1:
        try:
            limit = int(sys.argv[1])
            print(f"{EMOJI_TARGET} Limite de test spécifiée: {limit:,} panoramas")
        except ValueError:
            print(f"{EMOJI_ERROR} Argument invalide: {sys.argv[1]} (doit être un nombre entier)")
            print(f"{EMOJI_INFO} Usage: python {sys.argv[0]} [limite_pour_test]")
            print(f"{EMOJI_INFO} Sans argument: exploration complète de la République tchèque")
            sys.exit(1)
    else:
        print(f"{EMOJI_GLOBE} Mode exploration complète - collecte de TOUS les panoramas")
    
    asyncio.run(main(limit))
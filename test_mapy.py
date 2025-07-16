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
                    'User-Agent': f'{USER_AGENT_PROXY_PREFIX}-{proxy_index}/{USER_AGENT_VERSION}',
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
    """Collecteur avec optimisations proxy pour saturer l'API"""
    
    def __init__(self, output_dir: str, proxies: List[Tuple], max_concurrent_per_proxy: int = DEFAULT_MAX_CONCURRENT_PER_PROXY):
        self.output_dir = output_dir
        self.proxy_manager = ProxyManager(proxies)
        
        # Configurer le proxy manager pour streetlevel
        set_proxy_manager(self.proxy_manager)
        
        self.proxy_connector = ProxyRotatingConnector(self.proxy_manager, max_concurrent_per_proxy)
        
        # Calcul de la concurrence totale
        self.total_max_concurrent = len(proxies) * max_concurrent_per_proxy
        
        # Métriques
        self.visited: Set[str] = set()
        self.to_visit: deque = deque()
        self.network_calls = 0
        self.timeouts = 0
        self.errors = 0
        self.exploration_order = 0
        self.start_time = time.time()
        
        # Optimisations
        self.semaphore = asyncio.Semaphore(self.total_max_concurrent)
        
        # Cache et déduplication
        self.processing_cache: Set[str] = set()
        self.failed_cache: Set[str] = set()
        
        # Batch writer optimisé
        self.batch_data = []
        self.write_executor = ThreadPoolExecutor(max_workers=FILE_WRITER_MAX_WORKERS)
        
        # Métriques de performance
        self.last_progress_time = time.time()
        self.last_progress_count = 0
        self.bfs_iterations = 0
        
        # Créer le dossier
        os.makedirs(output_dir, exist_ok=True)
        
        print(f"{EMOJI_ROCKET} COLLECTEUR PROXY ULTRA-OPTIMISÉ")
        print(f"{EMOJI_GLOBE} {len(proxies)} proxies × {max_concurrent_per_proxy} = {self.total_max_concurrent} connexions simultanées")
        print(f"{EMOJI_FOLDER} Sortie: {output_dir}/")
        print(f"{EMOJI_PACKAGE} Utilisation de streetlevel local")
    
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
        print(f"\n{EMOJI_CHART} STATISTIQUES PROXIES:")
        print(f"   • Total requêtes: {proxy_stats['total_requests']:,}")
        print(f"   • Taux de succès: {proxy_stats['success_rate']:.1f}%")
        print(f"   • Erreurs: {proxy_stats['total_errors']:,}")
    
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
        
        print(f"{EMOJI_ROCKET} {current_count:,} panos | {instant_rate:.0f}/s | {avg_rate:.0f}/s avg | "
              f"{network_efficiency:.0f} req/s | {success_rate:.1f}% | Queue: {len(self.to_visit):,} | "
              f"Proxies: {proxy_efficiency:.0f} req/s | {elapsed:.0f}s")
        
        self.last_progress_time = current_time
        self.last_progress_count = current_count
    
    async def explore_with_proxies(self, start_lat: float, start_lon: float, target_count: int):
        """Exploration avec proxies multiples"""
        print(f"\n{EMOJI_ROCKET} EXPLORATION AVEC PROXIES MULTIPLES")
        print(f"{EMOJI_LOCATION} Départ: ({start_lat}, {start_lon})")
        print(f"{EMOJI_TARGET} Objectif: {target_count:,} panoramas")
        print(f"{EMOJI_GLOBE} {len(self.proxy_manager.proxies)} proxies simultanés")
        print(f"{EMOJI_LIGHTNING} Concurrence TOTALE: {self.total_max_concurrent}")
        
        # Panorama de départ
        try:
            start_pano = await find_panorama_async(start_lat, start_lon, links=FETCH_LINKS_DEFAULT, historical=FETCH_HISTORICAL_DEFAULT)
            self.network_calls += 1
            
            if not start_pano:
                print(f"{EMOJI_ERROR} Aucun panorama trouvé!")
                return None
            
            print(f"{EMOJI_SUCCESS} Départ: {start_pano.id} ({len(start_pano.links or [])} liens, {len(start_pano.historical or [])} historiques)")
            
            # Initialisation
            self.visited.add(start_pano.id)
            self.to_visit.append(start_pano.id)
            
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
                        
        except Exception as e:
            print(f"{EMOJI_ERROR} Erreur démarrage: {e}")
            return None
        
        print(f"\n{EMOJI_GLOBE} BFS AVEC PROXIES MULTIPLES...")
        
        # Exploration BFS avec proxies
        try:
            while len(self.visited) < target_count and self.to_visit:
                self.bfs_iterations += 1
                
                # Métriques fréquentes
                if self.bfs_iterations % PROGRESS_REPORT_INTERVAL == 0:
                    self.print_proxy_metrics()
                
                # Batch ÉNORME pour saturer tous les proxies
                batch_size = min(
                    self.total_max_concurrent * BATCH_SIZE_MULTIPLIER,  # 2x la capacité théorique
                    len(self.to_visit),
                    target_count - len(self.visited)
                )
                
                current_batch = []
                for _ in range(batch_size):
                    if self.to_visit and len(self.visited) < target_count:
                        current_batch.append(self.to_visit.popleft())
                
                if not current_batch:
                    break
                
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
                        
                        # Expansion du graphe
                        if pano.links:
                            for link in pano.links:
                                try:
                                    if link.pano.id not in self.visited and link.pano.id not in self.failed_cache:
                                        self.visited.add(link.pano.id)
                                        self.to_visit.append(link.pano.id)
                                except:
                                    continue
                    
                    # Nettoyage agressif
                    if self.bfs_iterations % CACHE_CLEANUP_INTERVAL == 0:
                        self.processing_cache.clear()
                        if len(self.failed_cache) > FAILED_CACHE_MAX_SIZE:
                            self.failed_cache.clear()
                        gc.collect()
                    
                except Exception as e:
                    self.errors += 1
                    continue
                
                if len(self.visited) >= target_count:
                    break
                    
        except Exception as e:
            print(f"{EMOJI_ERROR} Erreur exploration: {e}")
        
        # Finalisation
        end_time = time.time()
        total_time = end_time - self.start_time
        
        speed = len(self.visited) / total_time if total_time > 0 else 0
        proxy_stats = self.proxy_manager.get_stats()
        
        return {
            'test_name': "PROXY_ULTRA_OPTIMIZED",
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
            'completed': len(self.visited) >= target_count,
            'output_dir': self.output_dir,
            'bfs_iterations': self.bfs_iterations,
            'failed_cache_size': len(self.failed_cache)
        }

def print_proxy_results(stats: Dict):
    """Résultats avec informations proxies"""
    if not stats:
        return
    
    total_minutes = int(stats['total_time'] // 60)
    total_seconds = int(stats['total_time'] % 60)
    
    print(f"\n{PROGRESS_SEPARATOR}")
    print(f"{EMOJI_GLOBE} RÉSULTATS AVEC PROXIES MULTIPLES")
    print(PROGRESS_SEPARATOR)
    print(f"{EMOJI_TARGET} Panoramas: {stats['total_panoramas']:,}")
    print(f"⏱️ Temps: {total_minutes}m {total_seconds}s ({stats['total_time']:.1f}s)")
    print(f"{EMOJI_LIGHTNING} Vitesse: {stats['speed']:.1f} panoramas/seconde")
    print(f"{EMOJI_GLOBE} Proxies: {stats['num_proxies']} proxies")
    print(f"{EMOJI_LINK} Concurrence totale: {stats['total_max_concurrent']:,}")
    print(f"{EMOJI_CYCLE} Itérations BFS: {stats['bfs_iterations']:,}")
    print(f"{EMOJI_DISK} Sortie: {stats['output_dir']}")
    
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
    
    # Comparaison
    improvement = (stats['speed'] / ORIGINAL_SCRIPT_SPEED - 1) * 100
    print(f"\n{EMOJI_ROCKET} AMÉLIORATION:")
    print(f"   • vs Script original: +{improvement:.0f}%")
    print(f"   • Facteur d'accélération: {stats['speed']/ORIGINAL_SCRIPT_SPEED:.1f}x")
    
    # Projection théorique
    theoretical_max = proxy_throughput / efficiency if efficiency > 0 else 0
    print(f"\n{EMOJI_TARGET} PROJECTION THÉORIQUE:")
    print(f"   • Avec cette latence API: ~{theoretical_max:.0f} panoramas/seconde")
    print(f"   • Limitation: Bande passante API + latence réseau")

async def main():
    """Version proxy ultra-optimisée"""
    # Configuration avec TOUS les proxies
    PRAGUE_LAT = DEFAULT_START_LATITUDE
    PRAGUE_LON = DEFAULT_START_LONGITUDE
    TARGET_COUNT = DEFAULT_TARGET_COUNT
    MAX_CONCURRENT_PER_PROXY = ULTRA_MAX_CONCURRENT_PER_PROXY
    OUTPUT_DIR = DEFAULT_OUTPUT_DIR
    
    print(f"{EMOJI_FACTORY} COLLECTE MAPY.CZ - PROXIES MULTIPLES")
    print(f"{EMOJI_LOCATION} Départ: Prague ({PRAGUE_LAT}, {PRAGUE_LON})")
    print(f"{EMOJI_TARGET} Objectif: {TARGET_COUNT:,} panoramas")
    print(f"{EMOJI_GLOBE} {len(PROXIES)} proxies disponibles")
    print(f"{EMOJI_LIGHTNING} {MAX_CONCURRENT_PER_PROXY} connexions par proxy")
    print(f"{EMOJI_FIRE} TOTAL: {len(PROXIES) * MAX_CONCURRENT_PER_PROXY:,} connexions simultanées")
    print(f"{EMOJI_FOLDER} Sortie: {OUTPUT_DIR}/")
    print(f"{EMOJI_CHART} Collecte des données historiques activée")
    
    try:
        async with UltraProxyOptimizedCollector(OUTPUT_DIR, PROXIES, MAX_CONCURRENT_PER_PROXY) as collector:
            stats = await collector.explore_with_proxies(
                start_lat=PRAGUE_LAT,
                start_lon=PRAGUE_LON,
                target_count=TARGET_COUNT
            )
        
        print_proxy_results(stats)
        
        if stats and stats['completed']:
            print(f"\n{EMOJI_PARTY} TERMINÉ! {TARGET_COUNT:,} panoramas en {stats['total_time']:.1f}s")
            print(f"{EMOJI_LIGHTNING} PERFORMANCE FINALE: {stats['speed']:.1f} panoramas/seconde")
            print(f"{EMOJI_GLOBE} PROXIES: {stats['num_proxies']} proxies utilisés efficacement")
        else:
            print(f"\n{EMOJI_WARNING} Incomplet: {stats['total_panoramas']:,} panoramas")
        
    except KeyboardInterrupt:
        print(f"\n{EMOJI_ERROR} Interrompu")
        sys.exit(1)
    except Exception as e:
        print(f"\n{EMOJI_ERROR} Erreur: {e}")
        import traceback
        traceback.print_exc()
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
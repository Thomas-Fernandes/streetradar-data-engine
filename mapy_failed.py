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
import signal

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

# Import du gestionnaire de checkpoints
from mapy_collection import CheckpointManager, ProxyManager, ProxyRotatingConnector

class FailedPanoramaRetryCollector:
    """Collecteur spécialisé pour réessayer les panoramas qui ont échoué"""
    
    def __init__(self, output_dir: str, proxies: List[Tuple], max_concurrent_per_proxy: int = FAILED_RETRY_MAX_CONCURRENT_PER_PROXY):
        self.output_dir = output_dir
        self.proxy_manager = ProxyManager(proxies)
        
        # Configurer le proxy manager pour streetlevel
        set_proxy_manager(self.proxy_manager)
        
        self.proxy_connector = ProxyRotatingConnector(self.proxy_manager, max_concurrent_per_proxy)
        
        # Calcul de la concurrence totale
        self.total_max_concurrent = len(proxies) * max_concurrent_per_proxy
        
        # Système de checkpoints spécialisé
        self.checkpoint_manager = CheckpointManager(checkpoint_dir=f"{CHECKPOINT_DIR}/failed_retry")
        
        # Métriques spécifiques aux échecs
        self.failed_to_retry: Set[int] = set()  # Panoramas à réessayer
        self.retry_succeeded: Set[int] = set()  # Succès de retry
        self.retry_failed: Set[int] = set()     # Échecs définitifs
        self.retry_attempts: Dict[int, int] = {}  # Nombre de tentatives par panorama
        
        # Métriques générales
        self.network_calls = 0
        self.timeouts = 0
        self.errors = 0
        self.start_time = time.time()
        
        # Optimisations
        self.semaphore = asyncio.Semaphore(self.total_max_concurrent)
        self.processing_cache: Set[int] = set()
        
        # Batch writer
        self.batch_data = []
        self.write_executor = ThreadPoolExecutor(max_workers=FILE_WRITER_MAX_WORKERS)
        
        # Métriques de performance
        self.last_progress_time = time.time()
        self.last_progress_count = 0
        
        # Flag pour gestion gracieuse des interruptions
        self.should_stop = False
        self._setup_signal_handlers()
        
        # Créer le dossier
        os.makedirs(output_dir, exist_ok=True)
        
        print(f"{EMOJI_RECYCLE} COLLECTEUR RETRY PANORAMAS ÉCHOUÉS")
        print(f"{EMOJI_GLOBE} {len(proxies)} proxies × {max_concurrent_per_proxy} = {self.total_max_concurrent} connexions")
        print(f"{EMOJI_FOLDER} Sortie: {output_dir}/")
        print(f"{EMOJI_TIME} Timeout augmenté: {FAILED_RETRY_TIMEOUT}s")
        print(f"{EMOJI_CYCLE} Max tentatives: {FAILED_RETRY_MAX_ATTEMPTS}")

    def _setup_signal_handlers(self):
        """Configure les gestionnaires de signaux pour arrêt gracieux"""
        def signal_handler(signum, frame):
            print(f"\n{EMOJI_WARNING} Signal {signum} reçu - Arrêt gracieux en cours...")
            self.should_stop = True
            
        signal.signal(signal.SIGINT, signal_handler)
        signal.signal(signal.SIGTERM, signal_handler)

    def load_failed_panoramas_from_checkpoints(self) -> Set[int]:
        """Charge les panoramas échoués depuis les checkpoints de la collecte principale"""
        failed_panoramas = set()
        
        # Chercher les checkpoints dans le dossier principal
        main_checkpoint_dir = CHECKPOINT_DIR
        failed_cache_file = os.path.join(main_checkpoint_dir, CHECKPOINT_FAILED_CACHE_FILE)
        
        if os.path.exists(failed_cache_file):
            try:
                with open(failed_cache_file, 'rb') as f:
                    failed_cache = pickle.load(f)
                    failed_panoramas.update(failed_cache)
                    print(f"{EMOJI_SUCCESS} Chargé {len(failed_cache):,} panoramas échoués depuis checkpoint principal")
            except Exception as e:
                print(f"{EMOJI_ERROR} Erreur lecture failed_cache: {e}")
        
        # Chercher aussi dans les backups
        backup_dir = os.path.join(main_checkpoint_dir, "backups")
        if os.path.exists(backup_dir):
            backup_count = 0
            for filename in os.listdir(backup_dir):
                if filename.endswith(CHECKPOINT_FAILED_CACHE_FILE):
                    try:
                        filepath = os.path.join(backup_dir, filename)
                        with open(filepath, 'rb') as f:
                            backup_failed = pickle.load(f)
                            before_count = len(failed_panoramas)
                            failed_panoramas.update(backup_failed)
                            new_count = len(failed_panoramas) - before_count
                            if new_count > 0:
                                backup_count += new_count
                    except Exception as e:
                        print(f"{EMOJI_WARNING} Erreur lecture backup {filename}: {e}")
            
            if backup_count > 0:
                print(f"{EMOJI_RECYCLE} Ajouté {backup_count:,} panoramas depuis {len([f for f in os.listdir(backup_dir) if f.endswith(CHECKPOINT_FAILED_CACHE_FILE)])} backups")
        
        return failed_panoramas

    def load_successful_panoramas_from_checkpoints(self) -> Set[int]:
        """Charge les panoramas déjà réussis pour éviter de les re-traiter"""
        successful_panoramas = set()
        
        # Chercher les checkpoints dans le dossier principal
        main_checkpoint_dir = CHECKPOINT_DIR
        visited_file = os.path.join(main_checkpoint_dir, CHECKPOINT_VISITED_FILE)
        
        if os.path.exists(visited_file):
            try:
                with open(visited_file, 'rb') as f:
                    visited = pickle.load(f)
                    successful_panoramas.update(visited)
                    print(f"{EMOJI_SUCCESS} Chargé {len(visited):,} panoramas déjà réussis depuis checkpoint principal")
            except Exception as e:
                print(f"{EMOJI_ERROR} Erreur lecture visited: {e}")
        
        return successful_panoramas

    async def load_from_retry_checkpoint(self) -> bool:
        """Charge l'état depuis un checkpoint de retry"""
        checkpoint_data = self.checkpoint_manager.load_checkpoint()
        if not checkpoint_data:
            return False
            
        try:
            # Restaurer l'état spécifique aux retries
            state = checkpoint_data['state']
            self.retry_succeeded = checkpoint_data.get('retry_succeeded', set())
            self.retry_failed = checkpoint_data.get('retry_failed', set())
            self.retry_attempts = checkpoint_data.get('retry_attempts', {})
            
            # Restaurer les métriques
            self.network_calls = state.get('network_calls', 0)
            self.timeouts = state.get('timeouts', 0)
            self.errors = state.get('errors', 0)
            self.start_time = state.get('start_time', time.time())
            
            print(f"{EMOJI_SUCCESS} État retry restauré depuis checkpoint:")
            print(f"   • {len(self.retry_succeeded):,} panoramas récupérés")
            print(f"   • {len(self.retry_failed):,} échecs définitifs")
            print(f"   • {len(self.retry_attempts):,} tentatives enregistrées")
            
            return True
            
        except Exception as e:
            print(f"{EMOJI_ERROR} Erreur chargement checkpoint retry: {e}")
            return False

    async def save_retry_checkpoint(self):
        """Sauvegarde l'état des retries"""
        try:
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            
            # État principal
            state = {
                'timestamp': timestamp,
                'retry_succeeded_count': len(self.retry_succeeded),
                'retry_failed_count': len(self.retry_failed),
                'network_calls': self.network_calls,
                'timeouts': self.timeouts,
                'errors': self.errors,
                'start_time': self.start_time
            }
            
            # Sauvegarder l'état principal
            state_file = os.path.join(self.checkpoint_manager.checkpoint_dir, CHECKPOINT_STATE_FILE)
            with open(state_file, 'w') as f:
                json.dump(state, f, indent=2)
            
            # Sauvegarder les structures de données spécifiques
            self._save_retry_pickle_data('retry_succeeded.pkl', self.retry_succeeded)
            self._save_retry_pickle_data('retry_failed.pkl', self.retry_failed)
            self._save_retry_pickle_data('retry_attempts.pkl', self.retry_attempts)
            
            print(f"{EMOJI_SUCCESS} Checkpoint retry sauvé: {len(self.retry_succeeded):,} récupérés")
            
        except Exception as e:
            print(f"{EMOJI_ERROR} Erreur sauvegarde checkpoint retry: {e}")

    def _save_retry_pickle_data(self, filename: str, data):
        """Sauvegarde des données avec pickle"""
        filepath = os.path.join(self.checkpoint_manager.checkpoint_dir, filename)
        with open(filepath, 'wb') as f:
            pickle.dump(data, f)

    async def __aenter__(self):
        return self
    
    async def __aexit__(self, exc_type, exc_val, exc_tb):
        # Sauvegarde finale
        await self.save_retry_checkpoint()
        
        await self.proxy_connector.close_all_sessions()
        
        # Écriture finale
        if self.batch_data:
            await self._write_batch_if_needed(force=True)
        
        self.write_executor.shutdown(wait=True)

    def panorama_to_dict_retry(self, pano, attempt_number: int) -> Dict:
        """Conversion pour panoramas récupérés"""
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
            "links": len(pano.links) if pano.links else 0,
            "link_ids": [link.pano.id for link in (pano.links or []) if hasattr(link, 'pano')],
            "historical_count": len(pano.historical) if pano.historical else 0,
            "historical": historical_data,
            "retry_attempt": attempt_number,
            "recovered_time": time.time(),
            "recovery_method": "failed_retry"
        }

    async def fetch_panorama_with_retry(self, panoid: int, max_attempts: int = FAILED_RETRY_MAX_ATTEMPTS):
        """Récupération avec plusieurs tentatives et délais"""
        if panoid in self.processing_cache:
            return None
        
        self.processing_cache.add(panoid)
        
        attempt = self.retry_attempts.get(panoid, 0)
        
        async with self.semaphore:
            try:
                for attempt_num in range(attempt + 1, max_attempts + 1):
                    try:
                        # Délai progressif entre tentatives
                        if attempt_num > 1:
                            delay = FAILED_RETRY_DELAY_BETWEEN_ATTEMPTS * (attempt_num - 1)
                            await asyncio.sleep(delay)
                        
                        # Tentative avec timeout plus long
                        pano = await asyncio.wait_for(
                            find_panorama_by_id_async(
                                str(panoid), 
                                links=FETCH_LINKS_DEFAULT, 
                                historical=FETCH_HISTORICAL_DEFAULT
                            ),
                            timeout=FAILED_RETRY_TIMEOUT
                        )
                        
                        self.network_calls += 1
                        
                        if pano:
                            self.retry_attempts[panoid] = attempt_num
                            return pano, attempt_num
                        else:
                            self.retry_attempts[panoid] = attempt_num
                            
                    except asyncio.TimeoutError:
                        self.timeouts += 1
                        self.retry_attempts[panoid] = attempt_num
                        print(f"{EMOJI_WARNING} Timeout tentative {attempt_num}/{max_attempts} pour {panoid}")
                        continue
                    except Exception as e:
                        self.errors += 1
                        self.retry_attempts[panoid] = attempt_num
                        continue
                
                # Toutes les tentatives ont échoué
                return None, max_attempts
                
            finally:
                self.processing_cache.discard(panoid)

    async def _write_batch_if_needed(self, force=False):
        """Écrit le batch si nécessaire"""
        if len(self.batch_data) >= FAILED_RETRY_BATCH_SIZE or force:
            if self.batch_data:
                timestamp = datetime.now().strftime(TIMESTAMP_FORMAT)
                filename = f"mapy_failed_retry_{timestamp}_{len(self.batch_data)}.parquet"
                filepath = os.path.join(self.output_dir, filename)
                
                # Écriture asynchrone
                loop = asyncio.get_event_loop()
                await loop.run_in_executor(self.write_executor, self._write_batch_sync, filepath, self.batch_data.copy())
                
                print(f"{EMOJI_DISK} Batch retry écrit: {len(self.batch_data):,} → {filename}")
                
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
            print(f"{EMOJI_ERROR} Erreur écriture batch retry: {e}")

    def print_retry_metrics(self):
        """Métriques spécifiques aux retries"""
        current_time = time.time()
        elapsed = current_time - self.start_time
        
        total_processed = len(self.retry_succeeded) + len(self.retry_failed)
        
        # Vitesses
        time_diff = current_time - self.last_progress_time
        count_diff = total_processed - self.last_progress_count
        instant_rate = count_diff / time_diff if time_diff > 0 else 0
        avg_rate = total_processed / elapsed if elapsed > 0 else 0
        
        # Taux de récupération
        recovery_rate = len(self.retry_succeeded) / total_processed * 100 if total_processed > 0 else 0
        
        # Efficacité réseau
        network_efficiency = self.network_calls / elapsed if elapsed > 0 else 0
        success_rate = (self.network_calls - self.timeouts - self.errors) / self.network_calls * 100 if self.network_calls > 0 else 0
        
        # Métriques mémoire
        try:
            import psutil
            process = psutil.Process()
            memory_mb = process.memory_info().rss / 1024 / 1024
            memory_info = f" | RAM: {memory_mb:.0f}MB"
        except:
            memory_info = ""
        
        remaining = len(self.failed_to_retry) - total_processed
        
        print(f"{EMOJI_RECYCLE} {len(self.retry_succeeded):,} récupérés | {len(self.retry_failed):,} définitifs | "
              f"{instant_rate:.0f}/s | {avg_rate:.0f}/s avg | {recovery_rate:.1f}% récup | "
              f"{network_efficiency:.0f} req/s | {success_rate:.1f}% | Reste: {remaining:,} | "
              f"Batch: {len(self.batch_data)}/{FAILED_RETRY_BATCH_SIZE} | {elapsed:.0f}s{memory_info}")
        
        self.last_progress_time = current_time
        self.last_progress_count = total_processed

    async def retry_failed_panoramas(self):
        """Réessaie tous les panoramas qui ont échoué"""
        
        print(f"\n{EMOJI_RECYCLE} RETRY PANORAMAS ÉCHOUÉS")
        
        # Charger les panoramas échoués
        failed_panoramas = self.load_failed_panoramas_from_checkpoints()
        if not failed_panoramas:
            print(f"{EMOJI_WARNING} Aucun panorama échoué trouvé dans les checkpoints")
            return None
        
        # Charger les panoramas déjà réussis pour éviter les doublons
        successful_panoramas = self.load_successful_panoramas_from_checkpoints()
        
        # Filtrer les panoramas à réessayer (enlever ceux déjà réussis)
        self.failed_to_retry = failed_panoramas - successful_panoramas
        
        print(f"{EMOJI_TARGET} {len(failed_panoramas):,} panoramas échoués trouvés")
        print(f"{EMOJI_SUCCESS} {len(successful_panoramas):,} panoramas déjà réussis (ignorés)")
        print(f"{EMOJI_RECYCLE} {len(self.failed_to_retry):,} panoramas à réessayer")
        
        if not self.failed_to_retry:
            print(f"{EMOJI_SUCCESS} Tous les panoramas échoués ont déjà été récupérés!")
            return None
        
        # Tentative de reprise depuis checkpoint retry
        resumed_from_retry_checkpoint = await self.load_from_retry_checkpoint()
        if resumed_from_retry_checkpoint:
            # Enlever ceux déjà traités
            already_processed = self.retry_succeeded | self.retry_failed
            self.failed_to_retry -= already_processed
            print(f"{EMOJI_CYCLE} Reprise retry: {len(self.failed_to_retry):,} panoramas restants")
        
        # Convertir en liste pour traitement
        panoramas_to_retry = list(self.failed_to_retry)
        random.shuffle(panoramas_to_retry)  # Mélanger pour équilibrer la charge
        
        print(f"{EMOJI_LIGHTNING} Concurrence: {self.total_max_concurrent}")
        print(f"{EMOJI_TIME} Timeout: {FAILED_RETRY_TIMEOUT}s par tentative")
        print(f"{EMOJI_CYCLE} Max tentatives: {FAILED_RETRY_MAX_ATTEMPTS}")
        
        start_time = time.time()
        processed_count = 0
        
        # Traitement par batches
        try:
            while panoramas_to_retry and not self.should_stop:
                
                # Métriques périodiques
                if processed_count % 100 == 0 and processed_count > 0:
                    self.print_retry_metrics()
                
                # Checkpoint périodique
                if processed_count % FAILED_RETRY_CHECKPOINT_INTERVAL == 0 and processed_count > 0:
                    await self.save_retry_checkpoint()
                
                # Batch pour cette itération
                current_batch = panoramas_to_retry[:FAILED_RETRY_BATCH_SIZE]
                panoramas_to_retry = panoramas_to_retry[FAILED_RETRY_BATCH_SIZE:]
                
                if not current_batch:
                    break
                
                # Lancement des tentatives
                try:
                    tasks = [self.fetch_panorama_with_retry(panoid) for panoid in current_batch]
                    results = await asyncio.gather(*tasks, return_exceptions=True)
                    
                    # Traitement des résultats
                    for panoid, result in zip(current_batch, results):
                        processed_count += 1
                        
                        if isinstance(result, Exception):
                            self.retry_failed.add(panoid)
                            continue
                        
                        pano, attempt_number = result
                        
                        if pano:
                            # Succès !
                            self.retry_succeeded.add(panoid)
                            
                            # Enregistrement
                            record = self.panorama_to_dict_retry(pano, attempt_number)
                            self.batch_data.append(record)
                            
                            # Écriture par batch
                            await self._write_batch_if_needed()
                            
                        else:
                            # Échec définitif
                            self.retry_failed.add(panoid)
                    
                except Exception as e:
                    print(f"{EMOJI_ERROR} Erreur batch retry: {e}")
                    # Marquer le batch comme échec
                    for panoid in current_batch:
                        self.retry_failed.add(panoid)
                        processed_count += 1
                    continue
                    
        except Exception as e:
            print(f"{EMOJI_ERROR} Erreur exploration retry: {e}")
        
        # Gestion de l'arrêt gracieux
        if self.should_stop:
            print(f"{EMOJI_WARNING} Arrêt gracieux demandé")
            await self.save_retry_checkpoint()
        
        # Finalisation
        end_time = time.time()
        total_time = end_time - start_time
        
        total_processed = len(self.retry_succeeded) + len(self.retry_failed)
        recovery_rate = len(self.retry_succeeded) / total_processed * 100 if total_processed > 0 else 0
        speed = total_processed / total_time if total_time > 0 else 0
        
        proxy_stats = self.proxy_manager.get_stats()
        
        return {
            'test_name': "FAILED_PANORAMA_RETRY",
            'total_failed_to_retry': len(self.failed_to_retry),
            'total_processed': total_processed,
            'retry_succeeded': len(self.retry_succeeded),
            'retry_failed': len(self.retry_failed),
            'recovery_rate': recovery_rate,
            'total_time': total_time,
            'speed': speed,
            'network_calls': self.network_calls,
            'timeouts': self.timeouts,
            'errors': self.errors,
            'success_rate': (self.network_calls - self.timeouts - self.errors) / self.network_calls * 100 if self.network_calls > 0 else 0,
            'proxy_requests': proxy_stats['total_requests'],
            'proxy_success_rate': proxy_stats['success_rate'],
            'graceful_stop': self.should_stop,
            'completed': len(panoramas_to_retry) == 0 and not self.should_stop,
            'output_dir': self.output_dir,
            'max_attempts_used': FAILED_RETRY_MAX_ATTEMPTS
        }

def print_retry_results(stats: Dict):
    """Affichage des résultats de retry"""
    if not stats:
        return
    
    total_minutes = int(stats['total_time'] // 60)
    total_seconds = int(stats['total_time'] % 60)
    
    print(f"\n{PROGRESS_SEPARATOR}")
    print(f"{EMOJI_RECYCLE} RÉSULTATS RETRY PANORAMAS ÉCHOUÉS")
    print(PROGRESS_SEPARATOR)
    
    print(f"{EMOJI_TARGET} Panoramas à réessayer: {stats['total_failed_to_retry']:,}")
    print(f"{EMOJI_CYCLE} Panoramas traités: {stats['total_processed']:,}")
    print(f"{EMOJI_SUCCESS} Récupérés avec succès: {stats['retry_succeeded']:,}")
    print(f"{EMOJI_ERROR} Échecs définitifs: {stats['retry_failed']:,}")
    print(f"{EMOJI_CHART} Taux de récupération: {stats['recovery_rate']:.1f}%")
    print(f"⏱️ Temps: {total_minutes}m {total_seconds}s")
    print(f"{EMOJI_LIGHTNING} Vitesse: {stats['speed']:.1f} panoramas/seconde")
    print(f"{EMOJI_FOLDER} Sortie: {stats['output_dir']}")
    
    print(f"\n{EMOJI_CHART} PERFORMANCE RÉSEAU:")
    print(f"   • Appels réseau: {stats['network_calls']:,}")
    print(f"   • Timeouts: {stats['timeouts']} ({stats['timeouts']/stats['network_calls']*100:.1f}%)")
    print(f"   • Erreurs: {stats['errors']} ({stats['errors']/stats['network_calls']*100:.1f}%)")
    print(f"   • Succès réseau: {stats['success_rate']:.1f}%")
    print(f"   • Tentatives max: {stats['max_attempts_used']}")
    
    print(f"\n{EMOJI_CHART} EFFICACITÉ PROXY:")
    print(f"   • Requêtes proxy: {stats['proxy_requests']:,}")
    print(f"   • Succès proxy: {stats['proxy_success_rate']:.1f}%")
    
    if stats['graceful_stop']:
        print(f"\n{EMOJI_WARNING} Session interrompue - État sauvegardé")
        print(f"{EMOJI_CYCLE} Reprise possible: relancez le script")
    elif stats['completed']:
        print(f"\n{EMOJI_PARTY} RETRY TERMINÉ!")
        if stats['retry_succeeded'] > 0:
            print(f"{EMOJI_SUCCESS} {stats['retry_succeeded']:,} panoramas récupérés avec succès")
        if stats['retry_failed'] > 0:
            print(f"{EMOJI_WARNING} {stats['retry_failed']:,} panoramas définitivement perdus")
    else:
        print(f"\n{EMOJI_WARNING} Retry incomplet")

async def main():
    """Script principal pour réessayer les panoramas échoués"""
    
    print(f"\n{EMOJI_RECYCLE} SCRIPT RETRY PANORAMAS ÉCHOUÉS")
    print(f"{EMOJI_TARGET} Objectif: Récupérer les panoramas qui ont échoué lors de la collecte principale")
    print(f"{EMOJI_FOLDER} Lecture: Checkpoints de {CHECKPOINT_DIR}/")
    print(f"{EMOJI_FOLDER} Sortie: {FAILED_RETRY_OUTPUT_DIR}/")
    print(f"{EMOJI_GLOBE} {len(PROXIES)} proxies disponibles")
    print(f"{EMOJI_LIGHTNING} {FAILED_RETRY_MAX_CONCURRENT_PER_PROXY} connexions par proxy")
    print(f"{EMOJI_TIME} Timeout augmenté: {FAILED_RETRY_TIMEOUT}s")
    print(f"{EMOJI_CYCLE} Max tentatives par panorama: {FAILED_RETRY_MAX_ATTEMPTS}")
    
    try:
        async with FailedPanoramaRetryCollector(
            FAILED_RETRY_OUTPUT_DIR, 
            PROXIES, 
            FAILED_RETRY_MAX_CONCURRENT_PER_PROXY
        ) as collector:
            stats = await collector.retry_failed_panoramas()
        
        print_retry_results(stats)
        
        if stats:
            if stats['graceful_stop']:
                print(f"\n{EMOJI_SUCCESS} ARRÊT GRACIEUX")
                print(f"{EMOJI_RECYCLE} {stats['retry_succeeded']:,} panoramas récupérés")
                print(f"{EMOJI_DISK} État sauvé pour reprise ultérieure")
                
            elif stats['completed']:
                print(f"\n{EMOJI_PARTY} RETRY COMPLÈTEMENT TERMINÉ!")
                if stats['retry_succeeded'] > 0:
                    improvement = stats['retry_succeeded'] / stats['total_failed_to_retry'] * 100
                    print(f"{EMOJI_CHART_UP} Amélioration: +{improvement:.1f}% de panoramas récupérés")
                    print(f"{EMOJI_SUCCESS} {stats['retry_succeeded']:,} panoramas sauvés de l'échec")
            else:
                remaining = stats['total_failed_to_retry'] - stats['total_processed']
                print(f"\n{EMOJI_WARNING} Retry partiel: {remaining:,} panoramas restants")
        
    except KeyboardInterrupt:
        print(f"\n{EMOJI_WARNING} Interruption clavier (Ctrl+C)")
        print(f"{EMOJI_DISK} État sauvegardé automatiquement")
        print(f"{EMOJI_CYCLE} Reprise possible: relancez le script")
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
        print(f"{EMOJI_ROCKET} uvloop activé pour retry optimisé")
    except ImportError:
        pass
    
    # Optimisations Python
    import sys
    sys.setswitchinterval(SWITCH_INTERVAL)
    
    asyncio.run(main()) 
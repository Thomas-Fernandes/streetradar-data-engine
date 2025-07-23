#!/usr/bin/env python3
"""
Script de collecte de panoramas Mapy.cz
Version simplifiée et efficace utilisant streetlevel
"""

import asyncio
import time
import json
import sys
import os
from pathlib import Path
from collections import deque
from typing import Set, List, Dict, Optional
from datetime import datetime
import psutil

# Import des settings
from settings import *

# Ajout du module streetlevel au path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), STREETLEVEL_PATH))

# Import streetlevel
from streetlevel.mapy import find_panorama_async, find_panorama_by_id_async

# Formats de sortie
if USE_PARQUET:
    import pyarrow as pa
    import pyarrow.parquet as pq
else:
    import csv


class MapyCollector:
    """Collecteur de panoramas Mapy.cz simple et efficace"""
    
    def __init__(self):
        self.visited: Set[int] = set()
        self.to_visit: deque = deque()
        self.batch_data: List[Dict] = []
        
        # Métriques
        self.collected_count = 0
        self.network_calls = 0
        self.errors = 0
        self.timeouts = 0
        self.start_time = time.time()
        
        # Semaphore pour contrôler la concurrence
        self.semaphore = asyncio.Semaphore(MAX_CONCURRENT)
        
        # Trouver le prochain dossier de sortie disponible
        self.output_dir = self._get_next_output_dir()
        self.output_dir.mkdir(parents=True, exist_ok=True)
        
        # Configurer le logging
        self.log_file = self.output_dir / f"collection_log_{datetime.now().strftime('%Y%m%d_%H%M%S')}.txt"
        
        self.log_and_print(f"🚀 Collecteur Mapy.cz initialisé")
        self.log_and_print(f"📁 Sortie: {self.output_dir}")
        self.log_and_print(f"📝 Log: {self.log_file}")
        if TARGET_COUNT is None:
            self.log_and_print(f"🎯 Objectif: COLLECTE EXHAUSTIVE (tous les panoramas accessibles)")
        else:
            self.log_and_print(f"🎯 Objectif: {TARGET_COUNT:,} panoramas")
        self.log_and_print(f"⚡ Concurrence: {MAX_CONCURRENT}")
        self.log_and_print(f"📚 Données historiques: {'Oui' if INCLUDE_HISTORICAL else 'Non'}")
        self.log_and_print(f"📦 Taille batch: {BATCH_SIZE:,}")
    
    def _get_next_output_dir(self) -> Path:
        """Trouve le prochain dossier de sortie disponible (1, 2, 3, ...)"""
        base_dir = Path(OUTPUT_BASE_DIR)
        counter = 1
        
        while True:
            output_dir = base_dir / str(counter)
            if not output_dir.exists():
                return output_dir
            counter += 1
    
    def get_memory_usage(self) -> float:
        """Retourne l'utilisation mémoire en MB"""
        try:
            process = psutil.Process()
            return process.memory_info().rss / 1024 / 1024
        except:
            return 0.0
    
    def log_and_print(self, message: str):
        """Affiche le message et l'écrit dans le fichier de log"""
        timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        log_message = f"[{timestamp}] {message}"
        
        # Afficher sur la console
        print(message)
        
        # Écrire dans le fichier de log
        try:
            with open(self.log_file, 'a', encoding='utf-8') as f:
                f.write(log_message + '\n')
        except Exception:
            # En cas d'erreur de log, ne pas planter le programme
            pass
    
    def panorama_to_dict(self, pano, order: int) -> Dict:
        """Convertit un panorama en dictionnaire"""
        try:
            # Données de base
            data = {
                "id": pano.id,
                "lat": pano.lat,
                "lon": pano.lon,
                "date": pano.date.isoformat() if hasattr(pano.date, 'isoformat') else str(pano.date),
                "provider": pano.provider,
                "elevation": pano.elevation,
                "order": order,
                "collection_time": time.time()
            }
            
            # Permalink si disponible
            if hasattr(pano, 'permalink'):
                try:
                    data["permalink"] = pano.permalink()
                except:
                    data["permalink"] = None
            else:
                data["permalink"] = None
            
            # Liens si activés
            if INCLUDE_LINKS and pano.links:
                data["links_count"] = len(pano.links)
                data["link_ids"] = [link.pano.id for link in pano.links if hasattr(link, 'pano')]
            else:
                data["links_count"] = 0
                data["link_ids"] = []
            
            # Données historiques si activées
            if INCLUDE_HISTORICAL and pano.historical:
                historical_data = []
                for hist_pano in pano.historical:
                    hist_data = {
                        "id": hist_pano.id,
                        "date": hist_pano.date.isoformat() if hasattr(hist_pano.date, 'isoformat') else str(hist_pano.date),
                        "provider": hist_pano.provider
                    }
                    historical_data.append(hist_data)
                
                data["historical_count"] = len(historical_data)
                data["historical"] = historical_data
            else:
                data["historical_count"] = 0
                data["historical"] = []
            
            return data
            
        except Exception as e:
            self.log_and_print(f"❌ Erreur conversion panorama {pano.id}: {e}")
            return None
    
    async def fetch_panorama(self, panoid: int) -> Optional[object]:
        """Récupère un panorama par son ID"""
        async with self.semaphore:
            try:
                self.network_calls += 1
                
                pano = await asyncio.wait_for(
                    find_panorama_by_id_async(
                        panoid,
                        links=INCLUDE_LINKS,
                        historical=INCLUDE_HISTORICAL
                    ),
                    timeout=NETWORK_TIMEOUT
                )
                
                return pano
                
            except asyncio.TimeoutError:
                self.errors += 1
                self.timeouts += 1
                return None
            except Exception as e:
                self.errors += 1
                return None
    
    async def write_batch(self, force=False):
        """Écrit un batch de données"""
        if len(self.batch_data) >= BATCH_SIZE or force:
            if not self.batch_data:
                return
            
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            
            if USE_PARQUET:
                filename = f"mapy_batch_{timestamp}_{len(self.batch_data)}.parquet"
                filepath = self.output_dir / filename
                
                try:
                    table = pa.Table.from_pylist(self.batch_data)
                    pq.write_table(
                        table,
                        filepath,
                        compression=PARQUET_COMPRESSION
                    )
                    print(f"💾 Batch écrit: {len(self.batch_data)} → {filename}")
                except Exception as e:
                    self.log_and_print(f"❌ Erreur écriture Parquet: {e}")
            else:
                filename = f"mapy_batch_{timestamp}_{len(self.batch_data)}.json"
                filepath = self.output_dir / filename
                
                try:
                    with open(filepath, 'w', encoding='utf-8') as f:
                        json.dump(self.batch_data, f, ensure_ascii=False, indent=2)
                    print(f"💾 Batch écrit: {len(self.batch_data)} → {filename}")
                except Exception as e:
                    self.log_and_print(f"❌ Erreur écriture JSON: {e}")
            
            # Vider le batch
            self.batch_data.clear()
    
    def print_stats(self):
        """Affiche les statistiques"""
        elapsed = time.time() - self.start_time
        speed = self.collected_count / elapsed if elapsed > 0 else 0
        success_rate = (self.network_calls - self.errors) / self.network_calls * 100 if self.network_calls > 0 else 0
        memory_mb = self.get_memory_usage()
        
        if TARGET_COUNT is None:
            target_display = "∞"
        else:
            target_display = f"{TARGET_COUNT:,}"
        
        stats_msg = (f"📊 {self.collected_count:,}/{target_display} | "
                    f"{speed:.1f}/s | Queue: {len(self.to_visit):,} | "
                    f"Succès: {success_rate:.1f}% | Timeouts: {self.timeouts} | Erreurs: {self.errors} | "
                    f"RAM: {memory_mb:.0f}MB | {elapsed:.0f}s")
        
        self.log_and_print(stats_msg)
    
    async def collect(self):
        """Collecte principale avec BFS"""
        self.log_and_print(f"\n🔍 Recherche du panorama de départ...")
        
        # Trouver le panorama de départ
        try:
            start_pano = await find_panorama_async(
                START_LATITUDE,
                START_LONGITUDE,
                radius=SEARCH_RADIUS,
                links=INCLUDE_LINKS,
                historical=INCLUDE_HISTORICAL
            )
            
            if not start_pano:
                self.log_and_print(f"❌ Aucun panorama trouvé à ({START_LATITUDE}, {START_LONGITUDE})")
                return
            
            self.log_and_print(f"✅ Panorama de départ trouvé: {start_pano.id}")
            
            # Initialiser la collection
            self.visited.add(start_pano.id)
            self.to_visit.append(start_pano.id)
            
        except Exception as e:
            self.log_and_print(f"❌ Erreur recherche panorama de départ: {e}")
            return
        
        self.log_and_print(f"\n🌐 Début de la collecte BFS...")
        
        # BFS principal
        iteration = 0
        while self.to_visit:  # Continue tant qu'il y a des panoramas à explorer
            iteration += 1
            
            # Prendre un batch de panoramas à traiter
            if TARGET_COUNT is None:
                # Mode illimité - traiter jusqu'à MAX_CONCURRENT ou queue vide
                batch_size = min(MAX_CONCURRENT, len(self.to_visit))
            else:
                # Mode limité - ne pas dépasser l'objectif
                batch_size = min(MAX_CONCURRENT, len(self.to_visit), TARGET_COUNT - self.collected_count)
            
            current_batch = []
            
            for _ in range(batch_size):
                if self.to_visit:
                    current_batch.append(self.to_visit.popleft())
            
            if not current_batch:
                break
            
            # Récupérer les panoramas en parallèle
            tasks = [self.fetch_panorama(panoid) for panoid in current_batch]
            panoramas = await asyncio.gather(*tasks, return_exceptions=True)
            
            # Traiter les résultats
            for pano in panoramas:
                if isinstance(pano, Exception) or not pano:
                    continue
                
                # Convertir et ajouter au batch
                self.collected_count += 1
                data = self.panorama_to_dict(pano, self.collected_count)
                
                if data:
                    self.batch_data.append(data)
                
                # Ajouter les liens à la queue
                if INCLUDE_LINKS and pano.links:
                    for link in pano.links:
                        try:
                            if hasattr(link, 'pano') and link.pano.id not in self.visited:
                                self.visited.add(link.pano.id)
                                self.to_visit.append(link.pano.id)
                        except:
                            continue
                
                # Écriture par batch
                await self.write_batch()
                
                # Arrêt si objectif atteint (mode limité seulement)
                if TARGET_COUNT is not None and self.collected_count >= TARGET_COUNT:
                    break
            
            # Affichage des stats
            if iteration % PROGRESS_INTERVAL == 0:
                self.print_stats()
                
            # Arrêt si objectif atteint (mode limité seulement)
            if TARGET_COUNT is not None and self.collected_count >= TARGET_COUNT:
                break
        
        # Écriture finale
        await self.write_batch(force=True)
        
        # Stats finales
        total_time = time.time() - self.start_time
        avg_speed = self.collected_count / total_time if total_time > 0 else 0
        final_memory = self.get_memory_usage()
        
        self.log_and_print(f"\n🎉 Collecte terminée!")
        self.log_and_print(f"📊 {self.collected_count:,} panoramas collectés")
        
        if TARGET_COUNT is None:
            self.log_and_print(f"🇨🇿 Collecte exhaustive - Tous les panoramas accessibles récupérés")
        else:
            self.log_and_print(f"🎯 Objectif de {TARGET_COUNT:,} panoramas atteint")
            
        self.log_and_print(f"⏱️  Temps total: {total_time:.1f}s")
        self.log_and_print(f"⚡ Vitesse moyenne: {avg_speed:.1f} panoramas/seconde")
        self.log_and_print(f"🌐 Appels réseau: {self.network_calls:,}")
        self.log_and_print(f"❌ Erreurs: {self.errors} (dont {self.timeouts} timeouts)")
        self.log_and_print(f"💾 RAM finale: {final_memory:.0f}MB")
        self.log_and_print(f"📁 Données sauvées dans: {self.output_dir}")
        self.log_and_print(f"📝 Log sauvé dans: {self.log_file}")


async def main():
    """Fonction principale"""
    print("🚀 COLLECTEUR MAPY.CZ - VERSION SIMPLIFIÉE")
    print("=" * 50)
    
    try:
        collector = MapyCollector()
        await collector.collect()
        
    except KeyboardInterrupt:
        print(f"\n⚠️  Interruption par l'utilisateur")
        sys.exit(1)
    except Exception as e:
        print(f"\n❌ Erreur: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)


if __name__ == "__main__":
    # Optimisation asyncio si disponible
    try:
        import uvloop
        uvloop.install()
        print("🚀 uvloop activé")
    except ImportError:
        pass
    
    asyncio.run(main())
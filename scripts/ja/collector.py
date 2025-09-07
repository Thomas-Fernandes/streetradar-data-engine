#!/usr/bin/env python3
"""
JA360 Panorama Scraper - BFS Navigation
Collecte tous les panoramas d'Islande en naviguant de voisin en voisin
"""

import asyncio
import aiohttp
import json
import time
import sys
import os
from collections import deque
from pathlib import Path
from datetime import datetime
from typing import Set, Dict, Optional

# Ajouter streetlevel au path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "../../"))

from streetlevel.ja import find_panorama_async, find_panorama_by_id_async

# Configuration
OUTPUT_DIR = Path("../../data/ja360/raw")
REYKJAVIK_LAT = 64.1355
REYKJAVIK_LON = -21.8954
SEARCH_RADIUS = 100  # mètres
BATCH_SIZE = 2500  # Sauvegarder tous les 2500 panoramas (optimisé pour I/O)
MAX_CONCURRENT = 75  # Requêtes simultanées (réduit pour éviter 504)
NETWORK_TIMEOUT = 30.0  # Timeout pour les requêtes réseau (augmenté pour diagnostique)
PROGRESS_INTERVAL = 100  # Affichage des stats détaillées toutes les 1000 collectes
MONITORING_INTERVAL = 10.0  # Intervalle des logs de monitoring en temps réel (secondes)
BATCH_DELAY = 0.5  # Délai entre les batches pour éviter de surcharger le serveur (secondes)

class JA360Scraper:
    def __init__(self):
        self.output_dir = OUTPUT_DIR
        self.output_dir.mkdir(parents=True, exist_ok=True)
        
        # État du scraping
        self.visited: Set[int] = set()  # IDs des panoramas déjà traités
        self.to_visit: deque = deque()  # Queue BFS des panoramas à traiter
        self.panoramas: Dict[int, dict] = {}  # Données collectées
        
        # Métriques
        self.start_time = time.time()
        self.total_requests = 0
        self.errors = 0
        self.timeouts = 0
        self.last_stats_time = time.time()
        self.last_collected_count = 0
        self.last_periodic_log_time = time.time()  # Pour les logs périodiques
        
        # Semaphore pour limiter la concurrence
        self.semaphore = asyncio.Semaphore(MAX_CONCURRENT)
        
        print(f"🇮🇸 JA360 Panorama Scraper - Version Optimisée")
        print(f"📁 Sortie: {self.output_dir}")
        print(f"🎯 Point de départ: Reykjavik ({REYKJAVIK_LAT}, {REYKJAVIK_LON})")
        print(f"⚡ Concurrence: {MAX_CONCURRENT} requêtes simultanées")
        print(f"⏱️ Timeout réseau: {NETWORK_TIMEOUT}s")
        print(f"💾 Taille des batches: {BATCH_SIZE} panoramas")
        print(f"⏸️ Délai entre batches: {BATCH_DELAY}s")
        print(f"📊 Statistiques détaillées toutes les {PROGRESS_INTERVAL} collectes")
        print(f"⏰ Monitoring temps réel toutes les {MONITORING_INTERVAL}s")
    
    def panorama_to_dict(self, pano) -> dict:
        """Convertit un panorama JA en dictionnaire sérialisable"""
        data = {
            'id': pano.id,
            'lat': pano.lat,
            'lon': pano.lon,
            'heading': pano.heading,
            'pano_url': pano.pano_url,
            'blur_key': pano.blur_key,
            'collection_time': time.time()
        }
        
        # Date de capture
        if pano.date:
            data['date'] = {
                'year': pano.date.year,
                'month': pano.date.month
            }
        
        # Adresse
        if pano.address:
            data['address'] = {
                'street_name_and_house_number': pano.address.street_name_and_house_number,
                'postal_code': pano.address.postal_code,
                'place': pano.address.place
            }
        
        # Noms de rues
        if pano.street_names:
            data['street_names'] = []
            for street in pano.street_names:
                street_data = {
                    'name': street.name,
                    'angles': street.angles,
                    'distance': street.distance
                }
                data['street_names'].append(street_data)
        
        # Voisins (IDs seulement pour éviter la récursion)
        if pano.neighbors:
            data['neighbor_ids'] = [neighbor.id for neighbor in pano.neighbors]
        else:
            data['neighbor_ids'] = []
        
        return data
    
    async def fetch_panorama_details(self, session: aiohttp.ClientSession, panoid: int) -> Optional[dict]:
        """Récupère les détails complets d'un panorama"""
        async with self.semaphore:
            try:
                self.total_requests += 1
                
                # Ajouter timeout pour éviter les blocages
                pano = await asyncio.wait_for(
                    find_panorama_by_id_async(panoid, session),
                    timeout=NETWORK_TIMEOUT
                )
                
                if pano:
                    return self.panorama_to_dict(pano)
                else:
                    return None
                    
            except asyncio.TimeoutError:
                self.errors += 1
                self.timeouts += 1
                print(f"⏱️ Timeout panorama {panoid}")
                return None
            except Exception as e:
                self.errors += 1
                print(f"❌ Erreur panorama {panoid}: {e}")
                return None
    
    async def find_starting_panorama(self, session: aiohttp.ClientSession) -> Optional[int]:
        """Trouve le panorama de départ à Reykjavik"""
        print(f"🔍 Recherche du panorama de départ...")
        
        try:
            pano = await find_panorama_async(REYKJAVIK_LAT, REYKJAVIK_LON, session, SEARCH_RADIUS)
            if pano:
                print(f"✅ Panorama de départ trouvé: {pano.id} à ({pano.lat:.4f}, {pano.lon:.4f})")
                return pano.id
            else:
                print(f"❌ Aucun panorama trouvé à Reykjavik")
                return None
                
        except Exception as e:
            print(f"❌ Erreur recherche initiale: {e}")
            return None
    
    def save_batch(self, force=False):
        """Sauvegarde un batch de panoramas"""
        if len(self.panoramas) >= BATCH_SIZE or force:
            if not self.panoramas:
                return
            
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            filename = f"ja360_batch_{timestamp}_{len(self.panoramas)}.json"
            filepath = self.output_dir / filename
            
            try:
                with open(filepath, 'w', encoding='utf-8') as f:
                    json.dump(self.panoramas, f, indent=2, ensure_ascii=False)
                
                print(f"💾 Batch sauvé: {len(self.panoramas)} panoramas → {filename}")
                self.panoramas.clear()  # Vider pour le prochain batch
                
            except Exception as e:
                print(f"❌ Erreur sauvegarde: {e}")
    
    def print_stats(self):
        """Affiche les statistiques de progression avec métriques détaillées"""
        current_time = time.time()
        elapsed = current_time - self.start_time
        
        # Vitesse globale
        overall_speed = len(self.visited) / elapsed if elapsed > 0 else 0
        
        # Vitesse instantanée (depuis les dernières stats)
        time_since_last = current_time - self.last_stats_time
        collected_since_last = len(self.visited) - self.last_collected_count
        instant_speed = collected_since_last / time_since_last if time_since_last > 0 else 0
        
        # Taux de succès
        success_rate = (self.total_requests - self.errors) / self.total_requests * 100 if self.total_requests > 0 else 0
        
        # Estimation du temps restant (basé sur la queue actuelle)
        if instant_speed > 0 and len(self.to_visit) > 0:
            eta_seconds = len(self.to_visit) / instant_speed
            eta_minutes = eta_seconds / 60
            eta_str = f" | ETA: {eta_minutes:.1f}min"
        else:
            eta_str = ""
        
        print(f"📊 {len(self.visited):,} panoramas | "
              f"Queue: {len(self.to_visit):,} | "
              f"Vitesse: {overall_speed:.1f}/s (inst: {instant_speed:.1f}/s) | "
              f"Succès: {success_rate:.1f}% | "
              f"Timeouts: {self.timeouts} | Erreurs: {self.errors}{eta_str} | "
              f"{elapsed:.0f}s")
        
        # Mettre à jour les métriques pour le prochain calcul
        self.last_stats_time = current_time
        self.last_collected_count = len(self.visited)
    
    def print_periodic_status(self):
        """Affiche le statut périodique selon MONITORING_INTERVAL"""
        current_time = time.time()
        elapsed = current_time - self.start_time
        
        # Vitesse globale
        overall_speed = len(self.visited) / elapsed if elapsed > 0 else 0
        
        # Taux de succès
        success_rate = (self.total_requests - self.errors) / self.total_requests * 100 if self.total_requests > 0 else 0
        
        print(f"⏰ [{elapsed:.0f}s] {len(self.visited):,} panoramas collectés | "
              f"Vitesse: {overall_speed:.2f}/s | "
              f"Queue: {len(self.to_visit):,} | "
              f"Succès: {success_rate:.1f}% | "
              f"Timeouts: {self.timeouts} | "
              f"Requêtes: {self.total_requests}")
        
        # Mettre à jour le timestamp du dernier log
        self.last_periodic_log_time = current_time
    
    async def scrape(self):
        """Scraping principal avec BFS optimisé"""
        # Configuration optimisée du connecteur
        connector = aiohttp.TCPConnector(
            limit=MAX_CONCURRENT * 2,  # Pool de connexions plus large
            limit_per_host=MAX_CONCURRENT,
            keepalive_timeout=60,  # Maintenir les connexions plus longtemps
            enable_cleanup_closed=True
        )
        timeout = aiohttp.ClientTimeout(
            total=NETWORK_TIMEOUT + 5,  # Timeout global légèrement plus élevé
            connect=5,  # Timeout de connexion rapide
            sock_read=NETWORK_TIMEOUT  # Timeout de lecture
        )
        
        async with aiohttp.ClientSession(connector=connector, timeout=timeout) as session:
            
            # 1. Trouver le panorama de départ
            start_id = await self.find_starting_panorama(session)
            if not start_id:
                return
            
            # 2. Initialiser la queue BFS
            self.to_visit.append(start_id)
            
            print(f"\n🚀 Début du scraping BFS...")
            
            # 3. BFS principal
            while self.to_visit:
                
                # Préparer le batch de panoramas à traiter
                current_batch = []
                batch_size = min(MAX_CONCURRENT, len(self.to_visit))
                
                for _ in range(batch_size):
                    if self.to_visit:
                        panoid = self.to_visit.popleft()
                        if panoid not in self.visited:
                            current_batch.append(panoid)
                            self.visited.add(panoid)
                
                if not current_batch:
                    break
                
                # Traitement parallèle du batch
                tasks = [
                    self.fetch_panorama_details(session, panoid) 
                    for panoid in current_batch
                ]
                
                results = await asyncio.gather(*tasks, return_exceptions=True)
                
                # Traitement des résultats
                new_neighbors = 0
                successful_in_batch = 0
                
                for panoid, result in zip(current_batch, results):
                    if isinstance(result, Exception):
                        continue
                    
                    if result:
                        successful_in_batch += 1
                        # Stocker le panorama
                        self.panoramas[panoid] = result
                        
                        # Ajouter les voisins à la queue
                        for neighbor_id in result.get('neighbor_ids', []):
                            if neighbor_id not in self.visited and neighbor_id not in self.to_visit:
                                self.to_visit.append(neighbor_id)
                                new_neighbors += 1
                
                
                # Sauvegarde périodique
                self.save_batch()
                
                # Logs périodiques basés sur le temps
                current_time = time.time()
                if current_time - self.last_periodic_log_time >= MONITORING_INTERVAL:
                    self.print_periodic_status()
                
                # Statistiques détaillées périodiques (plus fréquentes)
                if len(self.visited) % PROGRESS_INTERVAL == 0 and len(self.visited) > 0:
                    self.print_stats()
                    print(f"   🆕 {new_neighbors} nouveaux voisins découverts")
                
                # Petit délai entre les batches pour éviter de surcharger le serveur
                if BATCH_DELAY > 0:
                    await asyncio.sleep(BATCH_DELAY)
                
                # Limite de sécurité (optionnel - désactivée pour collecte complète)
                # if len(self.visited) > 50000:  # Ajustez selon vos besoins
                #     print(f"⚠️ Limite de sécurité atteinte: {len(self.visited)} panoramas")
                #     break
        
        # Sauvegarde finale
        self.save_batch(force=True)
        
        # Statistiques finales
        total_time = time.time() - self.start_time
        avg_speed = len(self.visited) / total_time if total_time > 0 else 0
        
        print(f"\n🎉 Scraping terminé!")
        print(f"📊 {len(self.visited):,} panoramas collectés")
        print(f"⏱️ Temps total: {total_time:.1f}s")
        print(f"⚡ Vitesse moyenne: {avg_speed:.1f} panoramas/seconde")
        print(f"🌐 Requêtes totales: {self.total_requests}")
        print(f"❌ Erreurs: {self.errors}")
        print(f"📁 Données sauvées dans: {self.output_dir}")
        
        # Sauvegarder les statistiques finales avec métriques détaillées
        stats = {
            'total_panoramas': len(self.visited),
            'total_time': total_time,
            'avg_speed': avg_speed,
            'total_requests': self.total_requests,
            'errors': self.errors,
            'timeouts': self.timeouts,
            'success_rate': (self.total_requests - self.errors) / self.total_requests * 100 if self.total_requests > 0 else 0,
            'start_coordinates': [REYKJAVIK_LAT, REYKJAVIK_LON],
            'completion_time': datetime.now().isoformat(),
            'config': {
                'max_concurrent': MAX_CONCURRENT,
                'network_timeout': NETWORK_TIMEOUT,
                'batch_size': BATCH_SIZE,
                'progress_interval': PROGRESS_INTERVAL
            }
        }
        
        stats_file = self.output_dir / "scraping_stats.json"
        with open(stats_file, 'w') as f:
            json.dump(stats, f, indent=2)
        
        print(f"📊 Statistiques sauvées: {stats_file}")

async def main():
    """Fonction principale"""
    try:
        scraper = JA360Scraper()
        await scraper.scrape()
        
    except KeyboardInterrupt:
        print(f"\n⚠️ Interruption par l'utilisateur")
        sys.exit(1)
    except Exception as e:
        print(f"\n❌ Erreur: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)

if __name__ == "__main__":
    asyncio.run(main())
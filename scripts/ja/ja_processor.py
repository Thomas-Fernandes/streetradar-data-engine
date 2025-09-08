#!/usr/bin/env python3
"""
JA360 Data Processor
Convertit tous les batches JSON collectés en un seul fichier GeoJSON
avec toutes les métadonnées préservées.
"""

import json
import time
import sys
import os
from pathlib import Path
from typing import Dict, List, Any, Generator
from datetime import datetime
import glob

# Configuration
INPUT_DIR = Path("../../data/ja360/raw")
OUTPUT_DIR = Path("../../data/ja360/processed") 
OUTPUT_FILENAME = "ja360_iceland_panoramas.geojson"

class JA360Processor:
    """Processeur pour convertir les données JA360 en GeoJSON"""
    
    def __init__(self):
        self.input_dir = INPUT_DIR
        self.output_dir = OUTPUT_DIR
        self.output_dir.mkdir(parents=True, exist_ok=True)
        
        # Métriques de traitement
        self.total_panoramas = 0
        self.processed_batches = 0
        self.start_time = time.time()
        
        print(f"🔄 JA360 Data Processor")
        print(f"📁 Source: {self.input_dir}")
        print(f"📁 Destination: {self.output_dir}")
        print(f"📄 Fichier de sortie: {OUTPUT_FILENAME}")
    
    def find_batch_files(self) -> List[Path]:
        """Trouve tous les fichiers de batch JSON"""
        pattern = str(self.input_dir / "ja360_batch_*.json")
        batch_files = [Path(f) for f in glob.glob(pattern)]
        batch_files.sort()  # Tri par nom pour un ordre cohérent
        
        print(f"📊 {len(batch_files)} fichiers de batch trouvés")
        return batch_files
    
    def load_batch_file(self, filepath: Path) -> Dict[str, Any]:
        """Charge un fichier de batch JSON"""
        try:
            with open(filepath, 'r', encoding='utf-8') as f:
                data = json.load(f)
            return data
        except Exception as e:
            print(f"❌ Erreur lors du chargement de {filepath}: {e}")
            return {}
    
    def panorama_to_geojson_feature(self, pano_id: str, pano_data: Dict[str, Any]) -> Dict[str, Any]:
        """Convertit un panorama en feature GeoJSON"""
        
        # Géométrie du point
        geometry = {
            "type": "Point",
            "coordinates": [pano_data["lon"], pano_data["lat"]]  # [longitude, latitude]
        }
        
        # Propriétés (toutes les métadonnées)
        properties = {
            "id": pano_data["id"],
            "heading": pano_data.get("heading"),
            "pano_url": pano_data.get("pano_url"),
            "blur_key": pano_data.get("blur_key"),
            "collection_time": pano_data.get("collection_time")
        }
        
        # Ajouter la date si disponible
        if "date" in pano_data and pano_data["date"]:
            properties["capture_year"] = pano_data["date"].get("year")
            properties["capture_month"] = pano_data["date"].get("month")
        
        # Ajouter l'adresse si disponible
        if "address" in pano_data and pano_data["address"]:
            addr = pano_data["address"]
            properties["address_street"] = addr.get("street_name_and_house_number")
            properties["address_postal_code"] = addr.get("postal_code")
            properties["address_place"] = addr.get("place")
        
        # Ajouter les noms de rues si disponibles
        if "street_names" in pano_data and pano_data["street_names"]:
            street_names = []
            for street in pano_data["street_names"]:
                street_info = {
                    "name": street.get("name"),
                    "angles": street.get("angles", []),
                    "distance": street.get("distance")
                }
                street_names.append(street_info)
            properties["street_names"] = street_names
        
        # Compter les voisins
        if "neighbor_ids" in pano_data:
            properties["neighbor_count"] = len(pano_data["neighbor_ids"])
            properties["neighbor_ids"] = pano_data["neighbor_ids"]
        
        return {
            "type": "Feature",
            "geometry": geometry,
            "properties": properties
        }
    
    def process_all_batches(self) -> Generator[Dict[str, Any], None, None]:
        """Générateur qui traite tous les batches et yield les features GeoJSON"""
        batch_files = self.find_batch_files()
        
        for batch_file in batch_files:
            print(f"🔄 Traitement de {batch_file.name}...")
            
            batch_data = self.load_batch_file(batch_file)
            if not batch_data:
                continue
            
            batch_count = 0
            for pano_id, pano_data in batch_data.items():
                try:
                    feature = self.panorama_to_geojson_feature(pano_id, pano_data)
                    yield feature
                    batch_count += 1
                    self.total_panoramas += 1
                except Exception as e:
                    print(f"❌ Erreur avec panorama {pano_id}: {e}")
                    continue
            
            self.processed_batches += 1
            print(f"✅ {batch_count:,} panoramas traités dans {batch_file.name}")
    
    def create_geojson(self) -> None:
        """Crée le fichier GeoJSON consolidé"""
        output_path = self.output_dir / OUTPUT_FILENAME
        
        print(f"\n🚀 Début de la conversion en GeoJSON...")
        
        # Structure GeoJSON
        geojson = {
            "type": "FeatureCollection",
            "metadata": {
                "generated": datetime.now().isoformat(),
                "source": "JA360 Iceland Panoramas",
                "processor": "ja_processor.py",
                "total_features": 0,  # Sera mis à jour à la fin
                "bounds": None  # Sera calculé pendant le traitement
            },
            "features": []
        }
        
        # Variables pour calculer les bounds
        min_lon = min_lat = float('inf')
        max_lon = max_lat = float('-inf')
        
        # Traiter tous les batches
        for feature in self.process_all_batches():
            geojson["features"].append(feature)
            
            # Mettre à jour les bounds
            lon, lat = feature["geometry"]["coordinates"]
            min_lon = min(min_lon, lon)
            max_lon = max(max_lon, lon)
            min_lat = min(min_lat, lat)
            max_lat = max(max_lat, lat)
            
            # Affichage de progression
            if len(geojson["features"]) % 10000 == 0:
                print(f"📊 {len(geojson['features']):,} features ajoutées...")
        
        # Finaliser les métadonnées
        geojson["metadata"]["total_features"] = len(geojson["features"])
        geojson["metadata"]["bounds"] = {
            "min_longitude": min_lon,
            "max_longitude": max_lon,
            "min_latitude": min_lat,
            "max_latitude": max_lat
        }
        
        # Sauvegarder le GeoJSON
        print(f"\n💾 Sauvegarde du GeoJSON...")
        try:
            with open(output_path, 'w', encoding='utf-8') as f:
                json.dump(geojson, f, ensure_ascii=False, separators=(',', ':'))
            
            print(f"✅ GeoJSON sauvegardé: {output_path}")
            
        except Exception as e:
            print(f"❌ Erreur lors de la sauvegarde: {e}")
            return
        
        # Statistiques finales
        elapsed = time.time() - self.start_time
        file_size_mb = output_path.stat().st_size / (1024 * 1024)
        
        print(f"\n🎉 Traitement terminé!")
        print(f"📊 {self.total_panoramas:,} panoramas traités")
        print(f"📁 {self.processed_batches} fichiers de batch traités")
        print(f"⏱️ Temps total: {elapsed:.1f}s")
        print(f"📄 Taille du fichier: {file_size_mb:.1f} MB")
        print(f"🗺️ Bounds: ({min_lat:.4f}, {min_lon:.4f}) → ({max_lat:.4f}, {max_lon:.4f})")

def main():
    """Fonction principale"""
    try:
        processor = JA360Processor()
        processor.create_geojson()
        
    except KeyboardInterrupt:
        print(f"\n⚠️ Interruption par l'utilisateur")
        sys.exit(1)
    except Exception as e:
        print(f"\n❌ Erreur: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)

if __name__ == "__main__":
    main()

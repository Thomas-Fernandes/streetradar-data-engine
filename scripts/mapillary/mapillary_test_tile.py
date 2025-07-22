#!/usr/bin/env python3
"""
Test Mapillary zoom 12 - Téléchargement et analyse
Télécharge plusieurs tuiles zoom 12 et les convertit en GeoJSON pour QGIS
"""

import requests
import mapbox_vector_tile
import json
from pathlib import Path
import math

# Configuration
TOKEN = "MLY|9653269311406286|98577d80638669c67e94b1bacb8d8b72"
INPUT_DIR = Path("../../data/mapillary/raw")
OUTPUT_DIR = Path("../../data/mapillary/processed")

def latlon_to_tile(lat, lon, zoom):
    """Convertit lat/lon en coordonnées de tuile"""
    lat_rad = math.radians(lat)
    n = 2.0 ** zoom
    x = int((lon + 180.0) / 360.0 * n)
    y = int((1.0 - math.asinh(math.tan(lat_rad)) / math.pi) / 2.0 * n)
    return x, y

def download_tile(z, x, y, filename):
    """Télécharge une tuile Mapillary"""
    url = f"https://tiles.mapillary.com/maps/vtp/mly1_computed_public/2/{z}/{x}/{y}?access_token={TOKEN}"
    
    print(f"🔄 Téléchargement tuile {z}/{x}/{y}...")
    response = requests.get(url)
    
    if response.status_code == 200:
        filepath = INPUT_DIR / filename
        with open(filepath, 'wb') as f:
            f.write(response.content)
        print(f"✅ Sauvé: {filepath} ({len(response.content):,} bytes)")
        return filepath, len(response.content)
    else:
        print(f"❌ Erreur {response.status_code}")
        return None, 0

def analyze_mvt_content(mvt_file):
    """Analyse le contenu d'un fichier MVT"""
    print(f"\n🔍 Analyse: {mvt_file}")
    
    with open(mvt_file, 'rb') as f:
        mvt_data = f.read()
    
    try:
        decoded = mapbox_vector_tile.decode(mvt_data)
        
        total_features = 0
        layer_stats = {}
        
        for layer_name, layer_data in decoded.items():
            features = layer_data.get('features', [])
            total_features += len(features)
            
            # Analyser les types de géométries
            geom_types = {}
            for feature in features:
                geom_type = feature.get('geometry', {}).get('type', 'Unknown')
                geom_types[geom_type] = geom_types.get(geom_type, 0) + 1
            
            layer_stats[layer_name] = {
                'features': len(features),
                'geometry_types': geom_types
            }
            
            print(f"   📊 Couche '{layer_name}': {len(features):,} features")
            for geom_type, count in geom_types.items():
                print(f"      • {geom_type}: {count:,}")
        
        print(f"   🔢 Total features: {total_features:,}")
        return layer_stats, total_features
        
    except Exception as e:
        print(f"   ❌ Erreur décodage: {e}")
        return {}, 0

def convert_mvt_to_geojson(mvt_file, output_file, max_features_per_layer=None):
    """Convertit MVT en GeoJSON"""
    
    print(f"🔄 Conversion {mvt_file.name} → {output_file.name}")
    
    with open(mvt_file, 'rb') as f:
        mvt_data = f.read()
    
    decoded = mapbox_vector_tile.decode(mvt_data)
    
    geojson = {
        "type": "FeatureCollection",
        "features": []
    }
    
    total_features = 0
    for layer_name, layer_data in decoded.items():
        features = layer_data.get('features', [])
        
        # Limiter si nécessaire
        if max_features_per_layer and len(features) > max_features_per_layer:
            features = features[:max_features_per_layer]
            print(f"   ⚠️  Couche '{layer_name}' limitée à {max_features_per_layer:,} features")
        
        print(f"   📦 Couche '{layer_name}': {len(features):,} features")
        
        for feature in features:
            # Ajouter le nom de la couche dans les propriétés
            if 'properties' not in feature:
                feature['properties'] = {}
            feature['properties']['layer'] = layer_name
            
            geojson['features'].append(feature)
            total_features += 1
    
    # Sauvegarder
    with open(output_file, 'w') as f:
        json.dump(geojson, f)
    
    print(f"✅ {total_features:,} features sauvées dans {output_file.name}")
    return total_features

def main():
    """Fonction principale"""
    
    # Créer les dossiers
    INPUT_DIR.mkdir(parents=True, exist_ok=True)
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    
    print("🚀 TEST MAPILLARY BERLIN - ZOOMS 11/12/13")
    print("=" * 50)
    
    # Définir Berlin uniquement avec 3 niveaux de zoom
    test_zones = [
        {
            "name": "Berlin_centre",
            "lat": 52.5200, "lon": 13.4050,
            "description": "Berlin centre"
        }
    ]
    
    zoom_levels = [11, 12, 13]
    downloaded_files = []
    
    print(f"\n📍 Téléchargement Berlin aux zooms {zoom_levels}:")
    
    for zoom in zoom_levels:
        zone = test_zones[0]  # Berlin seulement
        x, y = latlon_to_tile(zone["lat"], zone["lon"], zoom)
        filename = f"tile_{zoom}_{x}_{y}_{zone['name']}.mvt"
        
        print(f"\n🇩🇪 {zone['description']} - ZOOM {zoom}")
        print(f"   📍 Coordonnées: ({zone['lat']:.4f}, {zone['lon']:.4f})")
        print(f"   🔢 Tuile: {zoom}/{x}/{y}")
        
        filepath, size = download_tile(zoom, x, y, filename)
        
        if filepath:
            downloaded_files.append((filepath, zone, zoom))
            
            # Analyser le contenu
            layer_stats, total_features = analyze_mvt_content(filepath)
            
            if total_features > 0:
                # Convertir en GeoJSON
                geojson_file = OUTPUT_DIR / f"{zone['name']}_zoom{zoom}.geojson"
                
                # Limiter les features si trop nombreuses pour QGIS
                max_features = 20000 if total_features > 100000 else None
                if max_features:
                    print(f"   ⚠️  Limitation à {max_features:,} features pour QGIS")
                
                convert_mvt_to_geojson(filepath, geojson_file, max_features)
            else:
                print(f"   ⚠️  Tuile vide - pas de conversion GeoJSON")
    
    # Résumé comparatif des 3 zooms
    print(f"\n📊 COMPARAISON BERLIN - ZOOMS 11/12/13")
    print("=" * 50)
    
    for filepath, zone, zoom in downloaded_files:
        if filepath.exists():
            size_mb = filepath.stat().st_size / 1024 / 1024
            
            # Re-analyser pour les stats finales
            with open(filepath, 'rb') as f:
                mvt_data = f.read()
            
            try:
                decoded = mapbox_vector_tile.decode(mvt_data)
                total_features = sum(len(layer_data.get('features', [])) for layer_data in decoded.values())
                
                print(f"🔍 ZOOM {zoom}:")
                print(f"   • Taille fichier: {size_mb:.2f} MB")
                print(f"   • Features totales: {total_features:,}")
                
                # Calculer la surface couverte approximative
                # Chaque niveau de zoom divise par 4 la surface
                if zoom == 11:
                    coverage = "~80km × 80km (6,400 km²)"
                elif zoom == 12:
                    coverage = "~40km × 40km (1,600 km²)"
                elif zoom == 13:
                    coverage = "~20km × 20km (400 km²)"
                    
                print(f"   • Surface couverte: {coverage}")
                
                # Densité features/km²
                area_km2 = [6400, 1600, 400][zoom-11]
                density = total_features / area_km2 if area_km2 > 0 else 0
                print(f"   • Densité: {density:.1f} features/km²")
                
                geojson_file = OUTPUT_DIR / f"{zone['name']}_zoom{zoom}.geojson"
                if geojson_file.exists():
                    geojson_size = geojson_file.stat().st_size / 1024 / 1024
                    print(f"   • GeoJSON: {geojson_size:.1f} MB")
                
            except Exception as e:
                print(f"   ❌ Erreur analyse: {e}")
    
    print(f"\n📁 Fichiers créés dans: {OUTPUT_DIR}")
    print("💡 Comparer les 3 fichiers GeoJSON Berlin dans QGIS !")
    
    return

if __name__ == "__main__":
    main()
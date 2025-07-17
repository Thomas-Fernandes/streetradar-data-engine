# 🚀 Guide : Collecte ILLIMITÉE de Panoramas

## 🌍 **Mode Collecte Totale : TOUS les panoramas existants**

**NOUVELLE FONCTIONNALITÉ** : Vous pouvez maintenant supprimer complètement la limite et collecter **TOUS** les panoramas existants sur Mapy.cz !

## ⚠️ Problème OOM sans optimisations

**Sans les optimisations**, votre script aura un **Out Of Memory (OOM)** avec 50+ millions de panoramas car :

### Consommation mémoire estimée :
- **`self.visited`** : 50M × 8 bytes (int) = **400 MB**
- **`self.to_visit`** : jusqu'à 5M éléments = **200-400 MB**  
- **`self.batch_data`** : 50M × 630 bytes = **~31.5 GB** 💥

## ✅ Solutions implémentées

### 1. **Mode Illimité Activé**
```python
UNLIMITED_MODE = True  # Collecte TOUS les panoramas existants
UNLIMITED_TARGET = float('inf')  # Pas de limite !
```

### 2. **Critères d'arrêt intelligents**
```python
# Arrêt automatique quand :
UNLIMITED_STOP_EMPTY_ITERATIONS = 50   # 50 itérations sans nouveaux panoramas
UNLIMITED_STOP_EMPTY_QUEUE_TIME = 300  # Queue vide pendant 5 minutes
```

### 3. **Écriture par batches automatique**
```python
MEGA_BATCH_SIZE = 10000  # Écriture tous les 10k panoramas
```
- Libère la mémoire régulièrement
- Divise en fichiers multiples
- Évite l'accumulation massive

### 4. **Optimisation des structures de données**
```python
self.visited: Set[int] = set()  # int au lieu de str (-20% mémoire)
self.processing_cache: Set[int] = set()
self.failed_cache: Set[int] = set()
```

### 5. **Nettoyage agressif de la mémoire**
```python
# Toutes les 100 itérations BFS :
- Cache processing vidé
- Cache échecs limité à 50k éléments
- Queue to_visit limitée à 5M éléments
- gc.collect() forcé
```

### 6. **Checkpoints pour gros volumes**
```python
UNLIMITED_CHECKPOINT_INTERVAL = 100000  # Checkpoint tous les 100k panoramas
```

## 🖥️ Monitoring mémoire avancé

Le script affiche maintenant :
```
🚀 2,457,891 panos | 156/s | 142/s avg | 890 req/s | 94.2% | 
Queue: 1,234,567 | Batch: 3,421/10,000 | Files: 245 | 
Proxies: 1,240 req/s | 17,293s | RAM: 2,847MB

🎉 CHECKPOINT: 100,000 panoramas collectés!
💾 Fichiers écrits: 10
💾 RAM: 2,847 MB
```

## 📂 Structure de sortie

Au lieu d'un seul fichier énorme :
```
raw/
├── mapy_batch_20241220_143052_10000.parquet  # 10k records
├── mapy_batch_20241220_143127_10000.parquet  # 10k records
├── mapy_batch_20241220_143201_10000.parquet  # 10k records
├── mapy_batch_20241220_144315_10000.parquet  # 10k records
└── ... (centaines/milliers de fichiers selon le volume total)
```

## ⚙️ Configuration

### Mode illimité (par défaut) :
```python
UNLIMITED_MODE = True          # Collecte illimitée
target_count = None            # Pas de limite !
```

### Retour au mode limité :
```python
UNLIMITED_MODE = False         # Mode limité
target_count = 50_000_000     # Limite à 50M
```

### Ressources système recommandées :
- **RAM** : 8-16 GB (au lieu de 32+ GB sans optimisations)
- **Stockage** : 100-500 GB selon le volume total (estimation : 100M-1B panoramas)
- **Temps estimé** : 1-4 semaines selon débit API et volume total

## 🔧 Ajustements possibles

### Si vous manquez de stockage :
```python
MEGA_BATCH_SIZE = 50000        # Fichiers plus gros, moins nombreux
```

### Si vous manquez encore de mémoire :
```python
MEGA_BATCH_SIZE = 5000         # Batches plus petits
MEGA_TO_VISIT_LIMIT = 2000000  # Queue plus petite
MEGA_CLEANUP_INTERVAL = 50     # Nettoyage plus fréquent
```

### Si vous avez beaucoup de RAM :
```python
MEGA_BATCH_SIZE = 100000       # Batches énormes
MEGA_TO_VISIT_LIMIT = 10000000 # Queue plus grande
```

## 📊 Architecture de données

Le script produit des fichiers Parquet **streamables** :
- Chaque fichier = batch indépendant
- Lecture parallèle possible  
- Compression Snappy optimale
- Compatible avec Pandas, Polars, DuckDB

## 🚨 Signaux d'alerte

Le script vous avertira si :
```
⚠️ to_visit trop gros (5,234,567), limitation...
⚠️ Queue vide - Attente de nouveaux panoramas...
⚠️ Arrêt: 50 itérations sans nouveaux panoramas
```

## 🎯 Critères d'arrêt automatique

En mode illimité, le script s'arrête automatiquement quand :

1. **50 itérations consécutives** sans découvrir de nouveaux panoramas
2. **Queue vide pendant 5 minutes** (plus rien à explorer)
3. **Interruption manuelle** (Ctrl+C)

## 🌍 Estimation de couverture

Avec le mode illimité, vous obtiendrez :
- **Tous les panoramas de République Tchèque**
- **Tous les panoramas connectés** (pays limitrophes)
- **Estimation totale** : 50M à 1B+ panoramas selon la connectivité

## 🎉 Résultat final

Avec ces optimisations, vous pouvez collecter **TOUS les panoramas existants** sans OOM sur une machine standard (8-16 GB RAM) !

Le script gérera automatiquement :
- ✅ Mémoire optimisée
- ✅ Fichiers multiples
- ✅ Arrêt intelligent
- ✅ Checkpoints réguliers
- ✅ Monitoring en temps réel 
# 🚀 Guide : Collecte ILLIMITÉE + Système de Reprise

## 🔄 **NOUVEAU : Système de Checkpoint et Reprise Automatique**

**Vous pouvez maintenant arrêter et reprendre le script à tout moment !** Le système sauvegarde automatiquement l'état et permet une reprise transparente.

## 🌍 **Mode Collecte Totale : TOUS les panoramas existants**

**NOUVELLE FONCTIONNALITÉ** : Vous pouvez maintenant supprimer complètement la limite et collecter **TOUS** les panoramas existants sur Mapy.cz !

## 🔄 **Système de Checkpoint**

### **Fonctionnalités:**
- ✅ **Sauvegarde automatique** tous les 1000 panoramas
- ✅ **Sauvegarde temporelle** toutes les 5 minutes  
- ✅ **Arrêt gracieux** avec Ctrl+C
- ✅ **Reprise automatique** après crash
- ✅ **Backups rotatifs** (3 versions gardées)
- ✅ **État complet** sauvé (visited, queue, métriques)

### **Utilisation:**

#### **Première exécution:**
```bash
python3 test_mapy.py
# ✅ Nouveau démarrage - Aucun checkpoint détecté
# 🚀 COLLECTEUR PROXY ULTRA-OPTIMISÉ [50M SAFE + CHECKPOINTS]
```

#### **Arrêt et reprise:**
```bash
# Arrêt avec Ctrl+C
^C
# ⚠️ Interruption clavier (Ctrl+C)
# 💾 État sauvegardé automatiquement
# 🔄 Reprise possible: relancez le script

# Reprise immédiate
python3 test_mapy.py
# 🔄 CHECKPOINT DÉTECTÉ !
#    • Date: 20241220_143052
#    • Panoramas collectés: 15,847
#    • Queue: 2,456 panoramas en attente
# 🎯 Reprendre depuis ce checkpoint ? (o/N): o
# ✅ État restauré depuis checkpoint
```

#### **Après un crash:**
```bash
python3 test_mapy.py
# 🔄 CHECKPOINT DÉTECTÉ !
# ✅ Reprise automatique activée
# 🔄 REPRISE DEPUIS CHECKPOINT - Continuation de la collecte
```

## ⚙️ **Configuration du système de checkpoint**

```python
# Activation
ENABLE_CHECKPOINTS = True           # Activer les checkpoints

# Fréquence de sauvegarde
CHECKPOINT_INTERVAL = 10000         # Tous les 10000 panoramas
CHECKPOINT_TIME_INTERVAL = 300      # Toutes les 5 minutes

# Comportement de reprise
AUTO_RESUME = True                  # Reprise automatique
RESUME_PROMPT = False               # Reprise automatique sans confirmation
CHECKPOINT_BACKUP_COUNT = 3         # Garder 3 backups
```

## 📂 **Structure des checkpoints**

```
checkpoints/
├── collection_state.json          # État principal (métriques, compteurs)
├── visited_set.pkl                # Set des panoramas visités  
├── to_visit_queue.pkl             # Queue des panoramas à explorer
├── failed_cache.pkl               # Cache des échecs
├── metadata.json                  # Métadonnées du checkpoint
└── backups/                       # Backups rotatifs
    ├── 20241220_143052_collection_state.json
    ├── 20241220_143052_visited_set.pkl
    └── ...
```

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

✅ Checkpoint sauvé: 15,847 panoramas

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
⚠️ Signal 2 reçu - Arrêt gracieux en cours...
```

## 🎯 Critères d'arrêt automatique

En mode illimité, le script s'arrête automatiquement quand :

1. **50 itérations consécutives** sans découvrir de nouveaux panoramas
2. **Queue vide pendant 5 minutes** (plus rien à explorer)
3. **Interruption manuelle** (Ctrl+C)
4. **Crash système** (reprise automatique au redémarrage)

## 🌍 Estimation de couverture

Avec le mode illimité, vous obtiendrez :
- **Tous les panoramas de République Tchèque**
- **Tous les panoramas connectés** (pays limitrophes)
- **Estimation totale** : 50M à 1B+ panoramas selon la connectivité

## 🔄 **Scénarios d'utilisation**

### **Collecte longue durée (plusieurs jours/semaines):**
```bash
# Lancement
python3 test_mapy.py

# Le script sauvegarde automatiquement
# Vous pouvez l'arrêter à tout moment avec Ctrl+C
# Et le reprendre plus tard
```

### **Après un crash système:**
```bash
# Redémarrage machine
# Relancement immédiat - reprise automatique
python3 test_mapy.py
```

### **Gestion des ressources:**
```bash
# Arrêt pour libérer la machine
^C

# Plus tard, reprise exactement où on en était
python3 test_mapy.py
```

## 🎉 Résultat final

Avec ces optimisations, vous pouvez collecter **TOUS les panoramas existants** sans OOM sur une machine standard (8-16 GB RAM) !

Le script gérera automatiquement :
- ✅ Mémoire optimisée
- ✅ Fichiers multiples
- ✅ Arrêt intelligent
- ✅ Checkpoints réguliers
- ✅ Monitoring en temps réel
- ✅ **Reprise après arrêt/crash**
- ✅ **Sauvegarde automatique**
- ✅ **Robustesse maximale** 
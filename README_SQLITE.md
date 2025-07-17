# 🚀 Collecteur Mapy.cz avec SQLite - Guide d'utilisation

## 🎯 Nouvelles fonctionnalités

### ✅ **Résolution des problèmes de mémoire (OOM)**
- **Problème résolu** : Plus de limite de 50M+ panoramas en mémoire
- **Solution** : Stockage des états (`visited`, `queue`, `failed`) en base SQLite
- **Avantage** : Empreinte mémoire constante même pour des millions de panoramas

### ♻️ **Reprise automatique après crash**
- **Problème résolu** : Plus besoin de repartir de zéro après interruption
- **Solution** : Persistance de l'état d'exploration en SQLite
- **Avantage** : Reprise exacte là où le script s'est arrêté

## 🗃️ Structure de la base SQLite

Le fichier `exploration_state.db` contient 3 tables :

```sql
-- Panoramas déjà visités
CREATE TABLE visited (
    pano_id TEXT PRIMARY KEY,
    timestamp REAL,
    exploration_order INTEGER
);

-- Queue d'exploration (prochains panoramas à traiter)  
CREATE TABLE queue (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    pano_id TEXT UNIQUE,
    priority INTEGER DEFAULT 0,
    added_timestamp REAL
);

-- Panoramas qui ont échoué (retry avec nettoyage auto)
CREATE TABLE failed (
    pano_id TEXT PRIMARY KEY,
    timestamp REAL,
    retry_count INTEGER DEFAULT 1
);
```

## 🚀 Utilisation

### **Exploration complète (nouveau mode par défaut)**
```bash
python test_mapy.py
# → Collecte TOUS les panoramas de la République tchèque
# → Continue jusqu'à épuisement complet
# → Peut prendre plusieurs jours/semaines
```

### **Mode test avec limite**
```bash
python test_mapy.py 5000
# → Collecte seulement 5000 panoramas pour tester
# → S'arrête automatiquement à la limite
```

### **Reprise après interruption**
```bash
# Le script a été interrompu (Ctrl+C, crash, etc.)
python test_mapy.py
# → Détecte automatiquement l'état précédent
# → Reprend exactement là où il s'était arrêté
# → Aucune donnée perdue
```

## 📊 Exemple de sortie

```
🏭 COLLECTE MAPY.CZ - PROXIES MULTIPLES + SQLITE
📍 Départ: Prague (50.0755, 14.4378)
🎯 Mode complet - Exploration de TOUTE la République tchèque
🌐 8 proxies disponibles
⚡ 300 connexions par proxy
🔥 TOTAL: 2,400 connexions simultanées
📁 Sortie: raw/
💾 Base de données: exploration_state.db
📊 Collecte des données historiques activée
🔄 Reprise automatique en cas d'interruption

🔄 REPRISE DÉTECTÉE:
   • Panoramas déjà visités: 1,247,892
   • Queue restante: 89,234
   • Échecs enregistrés: 1,203
   • Dernier ordre d'exploration: 1,247,892
✅ Reprise de l'exploration depuis l'ordre 1247892

🌐 BFS AVEC PROXIES MULTIPLES + SQLITE...
🚀 1,250,000 panos | 127/s | 89/s avg | 245 req/s | 87.3% | Queue: 89,001 | Proxies: 287 req/s | 14045s
```

## 🔧 Gestion avancée

### **Nettoyer et recommencer**
```bash
rm exploration_state.db
python test_mapy.py
# → Nouvelle exploration depuis zéro
```

### **Voir l'état actuel**
```bash
sqlite3 exploration_state.db "SELECT COUNT(*) as visited FROM visited;"
sqlite3 exploration_state.db "SELECT COUNT(*) as queue FROM queue;"
sqlite3 exploration_state.db "SELECT COUNT(*) as failed FROM failed;"
```

### **Reprendre avec une limite différente**
```bash
# État actuel : 500k panoramas visités, 200k en queue
python test_mapy.py 600000
# → Continue jusqu'à 600k puis s'arrête
# → Garde l'état pour reprendre plus tard
```

## ⚡ Optimisations SQLite

### **Performance**
- **WAL mode** : Lectures/écritures simultanées
- **Index optimisés** : Requêtes rapides sur les IDs
- **Transactions batch** : Insertions massives efficaces
- **Cache mémoire** : 10k pages en cache

### **Thread-safety**
- **Connexions multiples** : Une par thread
- **Locks appropriés** : Pas de corruption de données
- **Timeout configurable** : Évite les blocages

## 🎯 Avantages vs Version précédente

| Aspect | Avant | Maintenant |
|--------|-------|------------|
| **Mémoire** | ❌ OOM à ~5M panoramas | ✅ Illimité (SQLite) |
| **Crash** | ❌ Tout perdu | ✅ Reprise auto |
| **Monitoring** | ❌ Opaque | ✅ État visible en SQL |
| **Flexibilité** | ❌ Limite fixe | ✅ Limite optionnelle |
| **Performance** | ✅ Rapide | ✅ Rapide + persistant |

## 🚨 Points d'attention

### **Espace disque**
- **Base SQLite** : ~50-100 bytes par panorama
- **50M panoramas** ≈ 2.5-5 GB de base
- **Fichiers Parquet** : Selon configuration

### **Interruption propre**
- **Ctrl+C** : ✅ État sauvegardé
- **Kill -9** : ⚠️ Possible perte du batch en cours
- **Crash système** : ⚠️ Possible corruption (rare)

## 🔮 Cas d'usage

### **Exploration complète de la Tchéquie**
```bash
python test_mapy.py
# → Lance l'exploration infinie
# → Laisse tourner plusieurs jours
# → Peut interrompre/reprendre à volonté
```

### **Tests et développement**
```bash
python test_mapy.py 1000
# → Test rapide avec 1000 panoramas
# → Vérifie que tout fonctionne
# → État préservé pour continuer
```

### **Exploration par tranches**
```bash
python test_mapy.py 1000000   # Première tranche
python test_mapy.py 2000000   # Continue jusqu'à 2M
python test_mapy.py           # Puis exploration complète
```

---

🎉 **Le script est maintenant prêt pour collecter l'intégralité de la République tchèque de manière robuste et reprennable !** 
#!/bin/bash

# Script pour démarrer Airflow
# Ce script active l'environnement virtuel et démarre Airflow en mode standalone

# Aller dans le répertoire du projet (2 niveaux au-dessus)
cd "$(dirname "$0")/../.."

# Activer l'environnement virtuel
source venv/bin/activate

# Définir le répertoire home d'Airflow
export AIRFLOW_HOME=$(pwd)/airflow

echo "Démarrage d'Airflow..."
echo "Interface web disponible sur: http://localhost:8080"
echo "Identifiants:"
echo "  Username: admin"
echo "  Password: $(cat $AIRFLOW_HOME/simple_auth_manager_passwords.json.generated 2>/dev/null | grep -o '"admin": "[^"]*"' | cut -d'"' -f4 || echo 'sera généré au premier démarrage')"
echo ""
echo "Appuyez sur Ctrl+C pour arrêter Airflow"
echo ""

# Démarrer Airflow
airflow standalone

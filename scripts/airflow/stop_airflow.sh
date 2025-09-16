#!/bin/bash

# Script pour arrêter Airflow proprement

echo "Arrêt des processus Airflow..."

# Trouver et arrêter tous les processus Airflow
pids=$(ps aux | grep -E "(airflow|gunicorn)" | grep -v grep | awk '{print $2}')

if [ -z "$pids" ]; then
    echo "Aucun processus Airflow en cours d'exécution."
else
    echo "Arrêt des processus: $pids"
    for pid in $pids; do
        echo "Arrêt du processus $pid"
        kill -TERM $pid 2>/dev/null
    done
    
    # Attendre un peu puis forcer l'arrêt si nécessaire
    sleep 5
    
    remaining_pids=$(ps aux | grep -E "(airflow|gunicorn)" | grep -v grep | awk '{print $2}')
    if [ ! -z "$remaining_pids" ]; then
        echo "Forçage de l'arrêt des processus restants: $remaining_pids"
        for pid in $remaining_pids; do
            kill -KILL $pid 2>/dev/null
        done
    fi
fi

echo "Airflow arrêté."

"""
DAG pour le pipeline Naver
Séquence: collect_data -> process_data -> generate_tiles -> upload_r2
Déclenchement manuel uniquement, 3 retries par tâche
"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator
import os

# Configuration par défaut
default_args = {
    'owner': 'streetradar',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
}

# Définition du DAG
dag = DAG(
    'naver_pipeline',
    default_args=default_args,
    description='Pipeline de traitement des données Naver',
    schedule=None,  # Déclenchement manuel uniquement
    catchup=False,
    tags=['naver', 'streetradar', 'pipeline'],
)

# Chemin vers les scripts
scripts_path = '/home/nnb/github/streetradar-data-engine/scripts/naver'

# Tâche 1: Collecte des données
collect_data = BashOperator(
    task_id='collect_data',
    bash_command=f'cd {scripts_path} && python collect_data.py',
    dag=dag,
)

# Tâche 2: Traitement des données
process_data = BashOperator(
    task_id='process_data',
    bash_command=f'cd {scripts_path} && python process_data.py',
    dag=dag,
)

# Tâche 3: Génération des tuiles
generate_tiles = BashOperator(
    task_id='generate_tiles',
    bash_command=f'cd {scripts_path} && python generate_tiles.py',
    dag=dag,
)

# Tâche 4: Upload vers R2
upload_r2 = BashOperator(
    task_id='upload_r2',
    bash_command=f'cd {scripts_path} && python upload_r2.py',
    dag=dag,
)

# Définir la séquence des tâches
collect_data >> process_data >> generate_tiles >> upload_r2

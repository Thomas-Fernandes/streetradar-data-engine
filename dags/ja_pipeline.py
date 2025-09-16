"""
DAG pour le pipeline JA360
Séquence: collect_data -> process_data -> match_to_osm -> generate_tiles -> upload_r2
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
    'ja_pipeline',
    default_args=default_args,
    description='Pipeline de traitement des données JA360',
    schedule=None,  # Déclenchement manuel uniquement
    catchup=False,
    tags=['ja360', 'streetradar', 'pipeline'],
)

# Chemin vers les scripts
scripts_path = '/home/nnb/github/streetradar-data-engine/scripts/ja'

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

# Tâche 3: Correspondance avec OSM
match_to_osm = BashOperator(
    task_id='match_to_osm',
    bash_command=f'cd {scripts_path} && python match_to_osm.py',
    dag=dag,
)

# Tâche 4: Génération des tuiles
generate_tiles = BashOperator(
    task_id='generate_tiles',
    bash_command=f'cd {scripts_path} && python generate_tiles.py',
    dag=dag,
)

# Tâche 5: Upload vers R2
upload_r2 = BashOperator(
    task_id='upload_r2',
    bash_command=f'cd {scripts_path} && python upload_r2.py',
    dag=dag,
)

# Définir la séquence des tâches
collect_data >> process_data >> match_to_osm >> generate_tiles >> upload_r2

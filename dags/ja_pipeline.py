"""
DAG for JA360 pipeline
Sequence: collect_data -> process_data -> match_to_osm -> generate_tiles -> upload_r2
Manual trigger only, 3 retries per task
"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator
import os

# Default configuration
default_args = {
    'owner': 'streetradar',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
}

# DAG definition
dag = DAG(
    'ja_pipeline',
    default_args=default_args,
    description='JA360 data processing pipeline',
    schedule=None,  # Manual trigger only
    catchup=False,
    tags=['ja360', 'streetradar', 'pipeline'],
)

# Path to scripts
scripts_path = '/opt/streetradar/scripts/ja'

# Task 1: Data collection
collect_data = BashOperator(
    task_id='collect_data',
    bash_command=f'cd {scripts_path} && python collect_data.py',
    dag=dag,
)

# Task 2: Data processing
process_data = BashOperator(
    task_id='process_data',
    bash_command=f'cd {scripts_path} && python process_data.py',
    dag=dag,
)

# Task 3: OSM matching
match_to_osm = BashOperator(
    task_id='match_to_osm',
    bash_command=f'cd {scripts_path} && python match_to_osm.py',
    dag=dag,
)

# Task 4: Tile generation
generate_tiles = BashOperator(
    task_id='generate_tiles',
    bash_command=f'cd {scripts_path} && python generate_tiles.py',
    dag=dag,
)

# Task 5: R2 upload
upload_r2 = BashOperator(
    task_id='upload_r2',
    bash_command=f'cd {scripts_path} && python upload_r2.py',
    dag=dag,
)

# Define task sequence
collect_data >> process_data >> match_to_osm >> generate_tiles >> upload_r2

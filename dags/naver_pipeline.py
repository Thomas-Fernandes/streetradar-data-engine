"""
DAG for Naver pipeline
Sequence: collect_data -> process_data -> generate_tiles -> upload_r2
Manual trigger only, 3 retries per task
"""

import os
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator

# Default configuration
default_args = {
    "owner": "streetradar",
    "depends_on_past": False,
    "start_date": datetime(2024, 1, 1),
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 3,
    "retry_delay": timedelta(minutes=5),
}

# DAG definition
dag = DAG(
    "naver_pipeline",
    default_args=default_args,
    description="Naver data processing pipeline",
    schedule=None,  # Manual trigger only
    catchup=False,
    tags=["naver", "streetradar", "pipeline"],
)

# Path to scripts
scripts_path = "/opt/streetradar/scripts/naver"

# Task 1: Data collection
collect_data = BashOperator(
    task_id="collect_data",
    bash_command=f"cd {scripts_path} && python collect_data.py",
    dag=dag,
)

# Task 2: Data processing
process_data = BashOperator(
    task_id="process_data",
    bash_command=f"cd {scripts_path} && python process_data.py",
    dag=dag,
)

# Task 3: Tile generation
generate_tiles = BashOperator(
    task_id="generate_tiles",
    bash_command=f"cd {scripts_path} && python generate_tiles.py",
    dag=dag,
)

# Task 4: R2 upload
upload_r2 = BashOperator(
    task_id="upload_r2",
    bash_command=f"cd {scripts_path} && python upload_r2.py",
    dag=dag,
)

# Define task sequence
collect_data >> process_data >> generate_tiles >> upload_r2

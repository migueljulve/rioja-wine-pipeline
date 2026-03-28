import sys
import os
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator
from datetime import datetime, timedelta

# 1. Setup paths for DLT logic
# Where all the project is in airflow
LOGIC_PATH = "/opt/airflow/rioja_logic"

if LOGIC_PATH not in sys.path:
    sys.path.insert(0, LOGIC_PATH)

# Import the DLT load function
from dlt_data_ingestion import load_data

# 2. DAG Definition
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    dag_id='rioja_ingestion_orchestrator_v10', # Fresh ID to clear the "Broken" status
    description='DLT Ingest -> GCS -> BigQuery',
    schedule_interval='@daily',
    start_date=datetime(2024, 1, 1),
    catchup=False,
    default_args=default_args,
    tags=['rioja', 'stable']
) as dag:

    # TASK 1: Run the DLT ingestion script
    task_ingest = PythonOperator(
        task_id='run_dlt_ingestion_script',
        python_callable=load_data
    )

    # TASK 2: Load Weather Stations
    # Corrected wildcard to find your files
    task_load_weather = GCSToBigQueryOperator(
        task_id='load_weather_to_bq',
        bucket='rioja_wine_lake_raw',
        source_objects=['rioja_raw_data/weather_stations_raw/*'],
        destination_project_dataset_table='project-1314e5d0-dc4a-4a2f-804.rioja_wine_data.weather_stations_raw',
        source_format='PARQUET',
        write_disposition='WRITE_TRUNCATE',
        autodetect=True,
    )

    # TASK 3: Load History Metadata
    task_load_history = GCSToBigQueryOperator(
        task_id='load_history_to_bq',
        bucket='rioja_wine_lake_raw',
        source_objects=['rioja_raw_data/rioja_wine_history/*'],
        destination_project_dataset_table='project-1314e5d0-dc4a-4a2f-804.rioja_wine_data.rioja_wine_history',
        source_format='PARQUET',
        write_disposition='WRITE_TRUNCATE',
        autodetect=True,
    )

    # --- Flow Logic ---
    task_ingest >> [task_load_weather, task_load_history]
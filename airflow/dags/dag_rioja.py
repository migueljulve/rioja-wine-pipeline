import sys
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator
from datetime import datetime, timedelta

# Use the absolute path inside the container
LOGIC_PATH = "/opt/airflow/rioja_logic"

if LOGIC_PATH not in sys.path:
    sys.path.insert(0, LOGIC_PATH)

# Now try the import
try:
    from dlt_data_ingestion import load_data
except ImportError as e:
    # This will show up in the Airflow UI "Broken DAG" details
    raise ImportError(f"Could not find dlt_data_ingestion.py in {LOGIC_PATH}. sys.path is: {sys.path}") from e

with DAG(
    dag_id='rioja_ingestion_orchestrator_v1',
    description='Orchestrates the DLT ingestion for Rioja weather stations',
    schedule_interval='@daily',
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=['rioja', 'dlt']
) as dag:

    # task 1 : run the dlt ingestion script
    task_ingest = PythonOperator(
        task_id='run_dlt_ingestion_script',
        python_callable=load_data
    )

    # TASK 2: Load Weather Stations into BigQuery
    task_load_weather = GCSToBigQueryOperator(
        task_id='load_weather_tables_to_bq',
        bucket='rioja_wine_lake_raw',
        # DLT Path: dataset_name/resource_name/*.parquet
        source_objects=['rioja_raw_data/weather_stations_raw/*.parquet'],
        destination_project_dataset_table='project-1314e5d0-dc4a-4a2f-804.rioja_wine_data.weather_stations_raw',
        source_format='PARQUET',
        write_disposition='WRITE_TRUNCATE', # Overwrites to match dlt "replace"
        autodetect=True,
    )

    # TASK 3: Load History Metadata into BigQuery
    task_load_history = GCSToBigQueryOperator(
        task_id='load_history_table_to_bq',
        bucket='rioja_wine_lake_raw',
        source_objects=['rioja_raw_data/rioja_wine_history/*.parquet'],
        destination_project_dataset_table='project-1314e5d0-dc4a-4a2f-804.rioja_wine_data.rioja_wine_history',
        source_format='PARQUET',
        write_disposition='WRITE_TRUNCATE',
        autodetect=True,
    )

    # --- Flow Logic ---
    task_ingest >> [task_load_weather, task_load_history]
import sys
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta

# 1. Setup paths
# The path where your project lives inside the Airflow container
LOGIC_PATH = "/opt/airflow/rioja_logic"
DBT_PROJECT_DIR = f"{LOGIC_PATH}/rioja_dbt"

if LOGIC_PATH not in sys.path:
    sys.path.insert(0, LOGIC_PATH)

# Import the DLT load function from your custom script
from dlt_data_ingestion import load_data

# 2. DAG Definition
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    dag_id='rioja_ingestion_orchestrator_v11', # Incremented version
    description='Full Pipeline: DLT Ingest -> GCS -> BigQuery -> dbt Transform',
    schedule_interval='@daily',
    start_date=datetime(2024, 1, 1),
    catchup=False,
    default_args=default_args,
    tags=['rioja', 'production', 'dbt']
) as dag:

    # TASK 1: Run the DLT ingestion script (Extract & Load to GCS)
    task_ingest = PythonOperator(
        task_id='run_dlt_ingestion_script',
        python_callable=load_data
    )

    # TASK 2: Load Weather Stations Raw Data to BigQuery
    task_load_weather = GCSToBigQueryOperator(
        task_id='load_weather_to_bq',
        bucket='rioja_wine_lake_raw',
        source_objects=['rioja_raw_data/weather_stations_raw/*'],
        destination_project_dataset_table='project-1314e5d0-dc4a-4a2f-804.rioja_wine_data.weather_stations_raw',
        source_format='PARQUET',
        write_disposition='WRITE_TRUNCATE',
        autodetect=True,
    )

    # TASK 3: Load History Metadata to BigQuery
    task_load_history = GCSToBigQueryOperator(
        task_id='load_history_to_bq',
        bucket='rioja_wine_lake_raw',
        source_objects=['rioja_raw_data/rioja_wine_history/*'],
        destination_project_dataset_table='project-1314e5d0-dc4a-4a2f-804.rioja_wine_data.rioja_wine_history',
        source_format='PARQUET',
        write_disposition='WRITE_TRUNCATE',
        autodetect=True,
    )

    # TASK 4: dbt Transformation (The "T" in ELT)
    # This runs dbt inside the same container environment.
    # DBT_LOG_PATH/DBT_TARGET_PATH/PYTHONNOUSERSITE come from docker-compose.
    task_dbt_run = BashOperator(
        task_id='dbt_transform_rioja',
        bash_command=f'cd {DBT_PROJECT_DIR} && dbt run --profiles-dir .',
    )

    # --- Pipeline Flow ---
    # 1. Ingest -> 2. Load Tables in parallel -> 3. Run dbt transformations
    task_ingest >> [task_load_weather, task_load_history] >> task_dbt_run
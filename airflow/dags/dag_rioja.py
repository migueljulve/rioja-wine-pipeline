import sys
from airflow import DAG
from airflow.operators.python import PythonOperator
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

    task_ingest = PythonOperator(
        task_id='run_dlt_ingestion_script',
        python_callable=load_data
    )
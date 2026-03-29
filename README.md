# rioja-wine-pipeline

## Docker y Airflow

- **`Dockerfile.airflow`**: imagen usada por **Airflow** (`airflow-webserver`, `airflow-scheduler`, `airflow-init`). Es la vía principal si orquestas la ingesta con el DAG.
- **`Dockerfile`** (raíz): solo aplica al servicio opcional **`rioja_ingest`** en `docker-compose.yml` (perfil `manual`), para ejecutar la ingesta dlt sin Airflow. **No hace falta** si vas a correr todo con Airflow.

La ingesta que usa el DAG vive en `dlt_data_ingestion.py`; en los contenedores de Airflow se monta el repo en `/opt/airflow/rioja_logic` (incluye `rioja_data/`).

# vale

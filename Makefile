# Build and start the environment
setup:
	docker-compose up -d --build
	terraform init
	terraform apply -auto-approve

# Run the ingestion (dlt)
ingest:
	docker compose exec airflow-scheduler python dlt_scripts/rioja_ingestion.py

# Run and Test dbt models
dbt-build:
	docker compose exec airflow-scheduler bash -c "cd rioja_dbt && dbt build"

# Full pipeline shortcut
run-all: ingest dbt-build

# Clean up
clean:
	docker-compose down -v
	terraform destroy -auto-approve
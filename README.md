# Rioja Wine & Climate Analytics Pipeline
### *Data Engineering Zoomcamp 2025 - Final Project*

## Project Description
This project focuses on the intersection of **Viticulture and Climate Change**. It analyzes over 20 years of daily weather data from the Rioja region (Spain) to understand how thermal stress (frost and heatwaves) affects wine quality.

**The Problem:** Rioja winemakers face increasing climate volatility. While average temperatures rise, late spring frosts still threaten budding vines.
**The Solution:** An automated end-to-end pipeline that ingests, transforms, and visualizes daily weather metrics and historical vintage scores to provide actionable climate insights.


## 🏗️ Architecture
The project follows a **Medallion Architecture** managed by a modern data stack:

![Project Architecture](readme_images/project_architeture.png)


* **Infrastructure (IaC):** **Terraform** for provisioning Google Cloud Storage and BigQuery.
* **Orchestration:** **Apache Airflow** (Dockerized) managing the workflow.
* **Ingestion (ELT):** **dlt (Data Load Tool)** for robust ingestion of 23 CSV sources.
* **DataLake:** **Google Storage** Google Datalake in GCP.
* **Data Warehouse:** **Google BigQuery** (Storage & Compute).
* **Transformation:** **dbt (data build tool)** for SQL modeling and business logic.
* **Visualization:** **Looker Studio** for the final analytical dashboards.


---

## 🛠️ Data Pipeline Details

### 1. Orchestration (Apache Airflow)
The data lifecycle is managed by an Airflow DAG that ensures a strict execution order:
* **Task Dependencies:** The pipeline first triggers the `dlt` ingestion task. Only upon successful completion does it trigger the `dbt` transformation models.
* **Monitoring:** The Airflow UI provides real-time logs and health checks, ensuring that any data loading errors are caught and handled via retries.

![Airflow DAG](readme_images/airflow_dag.png)


### 2. Ingestion (dlt - Data Load Tool)
The ingestion layer leverages the `dlt` library to move data from local CSVs to the cloud:
* **Sources:** 23 CSV files, including one historical vintage record and 22 yearly files of daily weather station data.
* **Schema Evolution:** `dlt` automatically infers schemas and enforces data types, ensuring a structured load into **Bronze (Raw)** tables in BigQuery.
* **Efficiency:** Handles file flattening and normalization without manual SQL DDL commands.

### 3. Transformation (dbt - Data Build Tool)
Raw data is refined through a tiered modeling approach:
* **Staging Layer (Silver):** Cleans field names, casts data types, and filters out incomplete records (e.g., excluding 2026 data).
* **Analytics Layer (Gold - `fct_weather_trends`):** This model solves the "station duplication" challenge. It first calculates metrics per weather station and then averages them to provide a representative regional value.
* **Custom Business Logic:** 
    * **Spring Frost:** Days where $temp\_min \le 0°C$ during budding months (April–June).
    * **Extreme Heat:** Days where $temp\_max \ge 35°C$ during ripening months (July–August).

---

## 📊 Dashboard & Key Insights
[🔗 Access the Live Interactive Dashboard]([https://lookerstudio.google.com/s/k8x0Unpii1Y])

| **Thermal Stress Analysis** | **Vintage Quality (Parker Scale)** |
| :--- | :--- |
| ![Thermal Chart](images/climate_rioja_trends.png) | ![Quality Pie](images/quality_pie.png) |

**Key Findings:**
1.  **Warming Trend:** A clear upward slope in average annual temperatures since 2005.
2.  **Climatic Volatility:** Despite general warming, specific years like 2022 show record peaks in frost days, proving that "average" warming does not eliminate local cold risks.
3.  **Quality Resilience:** Over 60% of Rioja vintages remain in high-quality categories ("Excellent"/"Very Good") despite increasing heat stress.

---

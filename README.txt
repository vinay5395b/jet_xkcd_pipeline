Project Implementation Guide: The XKCD Analytics Engine

Link to Repository: https://github.com/vinay5395b/jet_xkcd_pipeline.git

1. Environment Preparation

Before launching the infrastructure, ensure the project directory is structured for portability and modularity.

Prerequisites
Docker & Docker Compose: 
- Installed and running.

Directory Structure:
/dags: Contains the Airflow orchestration logic.
/dbt: Contains the dbt project and models.

requirements.txt: Includes required libraries.
docker-compose.yml: Defines the service architecture.

Configuration:
Credentials: Update profiles.yml for dbt. Set the host to the Postgres service name defined in your Docker network (e.g., db) rather than localhost.
Dependencies: Verify the requirements.txt is updated to bridge the gap between ingestion scripts and the database environment.


2. Launching Infrastructure
The system uses Docker to spin up the entire ecosystem—Database, Airflow, and dbt—simultaneously to ensure environment parity.

Build and Deploy: Execute the following command in the project root:

docker-compose up -d --build


3. Command Center Access
Infrastructure management and pipeline monitoring are handled through the Airflow UI.

URL: Navigate to http://localhost:8080.
Authentication: Use the designated administrative credentials.
Connection Management: Under Admin > Connections, verify that the postgres_default connection string is correctly mapped to the Postgres container.


4. Pipeline Execution
The pipeline is designed as an automated "all-in-one" workflow.

Activation: Locate the XKCD Pipeline DAG and toggle the status to On.

Trigger: Initiate the run manually via the Trigger DAG button.

Workflow Monitoring: Monitor the Graph View for the following sequence:

API Sensor: Validates the availability of new data.

Ingestion Logic: Fetches metadata and fills gaps in the dataset.

dbt Transformation: Executes the Refinery layer and Hashing logic.

Quality Check: Performs automated data integrity tests.

Documentation: Generates the lineage graph and data dictionary.


5. Verification & Deliverables
After the workflow completes, verify the output to ensure the data is "Production-Ready."

Warehouse Audit: Connect to the Postgres container (using DBeaver or psql) and query the analytics schema. 
Verify that the views_count and avg_rating columns are populated with deterministic values.

Lineage & Docs: Access the generated dbt documentation to review the transformation flow from raw JSON to the final analytics marts.



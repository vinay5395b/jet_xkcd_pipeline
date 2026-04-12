XKCD Data Pipeline (ELT)

This project implements an automated pipeline to fetch XKCD comic metadata and store it in a PostgreSQL Data Warehouse. It uses Airflow for orchestration and Docker for environment consistency.
Prerequisites

    Docker Desktop installed.

    Git installed.

Setup & Installation

    Clone the repository:
    Bash

    git clone <your-repo-url>
    cd jet_xkcd_pipeline

    Launch the environment:
    This command builds the Airflow image, initializes the database, and creates the default admin user.
    Bash

    docker compose up -d

    Verify Containers:
    Ensure all three containers are running:
    Bash

    docker ps

Usage

    Access Airflow:
    Navigate to http://localhost:8080 in your browser.

        Username: admin

        Password: admin

    Trigger Ingestion:

        Locate the DAG xkcd_master_ingestion.

        Toggle the switch to On.

        Click the Play button to trigger the manual backfill.

    Monitor Progress:

        Click on the running task and select Logs to see the comics being fetched in real-time.
    
    Automation for new data:

        
        Scheduling & Polling:

        The pipeline is scheduled via CRON (0 12 * * 1,3,5) to run every Mon, Wed, and Fri.
        A PythonSensor is implemented to poll the XKCD API. If a new comic is not yet available at the scheduled time, the sensor will retry every 10 minutes for up to 12 hours, ensuring data is captured as soon as it is published.

Database Connection (for pgAdmin/DBeaver)

To view the data externally, connect to the PostgreSQL instance using:

    Host: localhost (or the container IP if using a bridge network)

    Port: 5432

    Database: jet_xkcd_db

    Username: postgres

    Password: postgres

Project Structure

    /dags: Contains the Airflow Python scripts for ingestion.

    /scripts: Utility scripts for data handling.

    docker-compose.yml: Defines the Airflow, Postgres, and Scheduler services.

    Dockerfile: Custom Airflow image with necessary Python libraries (psycopg2, requests).

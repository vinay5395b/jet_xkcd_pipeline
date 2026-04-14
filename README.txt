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



---------------------------------------------------

1. Macro Infrastructure (The "Engine")

    generate_mock_metrics: Implemented deterministic "Synthetic Data" logic.

        generate_views: Uses hashtext to create stable engagement metrics.

        generate_rating: Scales hash values to a 1.0–10.0 decimal range.

    get_alphanumeric_count: Centralized logic for string parsing, used to calculate business costs (Production Cost per char).

    generate_schema_name: Overrode dbt's default behavior to enforce a clean Static Schema Strategy (staging, intermediate, analytics) without environment prefixes.

2. Data Quality & Testing

    Generic Tests: Moved from singular tests to a reusable tests/generic/ framework.

        assert_range: Validates numeric boundaries (used for avg_rating).

        assert_not_in_future: Prevents temporal data anomalies.

        assert_is_positive: Ensures financial metrics like production_cost are valid.

    Referential Integrity: Standardized relationships tests in the Marts layer to ensure comic_id consistency between Facts and Dimensions.

3. Model Refactoring

    int_xkcd__transformed / comic_engagement:

        Namespace Resolution: Removed select * in favor of explicit column selection to prevent column ambiguity and name collisions.

        Normalization: Standardized text fields (trimming) and cast date strings into a proper DATE format (published_at).

        Performance: Optimized CTE structures for Postgres execution.

4. Warehouse Architecture

    Successfully implemented a 3-Tiered Schema Architecture:

        staging: Raw-to-Staging views.

        intermediate: Complex transformations and metric calculations (Ephemeral views).

        analytics: Final, clean Marts optimized for BI consumption.

The "HM" Project Narrative

If you are putting this in a README or explaining it in an interview, here is the narrative:

    "After establishing the base pipeline, I performed a deep refactor to migrate from 'script-style' SQL to an Enterprise Data Framework. I decoupled business logic into reusable macros, implemented a custom schema strategy to maintain a clean warehouse namespace, and built a generic testing suite to enforce data contracts. This transition ensures the project is not just a collection of queries, but a scalable, maintainable data platform."
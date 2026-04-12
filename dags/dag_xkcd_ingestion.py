from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.sensors.python import PythonSensor
from airflow.operators.bash import BashOperator
from airflow.utils.task_group import TaskGroup
from datetime import datetime, timedelta
import requests
import psycopg2
from psycopg2.extras import execute_values

def get_db_connection():

    return psycopg2.connect(
        host="postgres",
        database="jet_xkcd_db",
        user="postgres",
        password="postgres"
    )

def setup_database():
   
    conn = get_db_connection()
    conn.autocommit = True
    cur = conn.cursor()
    
    print("--- Initializing Database Objects ---")
    cur.execute("CREATE SCHEMA IF NOT EXISTS raw;")
    cur.execute("CREATE SCHEMA IF NOT EXISTS analytics;")
    cur.execute("""
        CREATE TABLE IF NOT EXISTS raw.xkcd_comics (
            num INTEGER PRIMARY KEY,
            month TEXT, link TEXT, year TEXT, news TEXT,
            safe_title TEXT, transcript TEXT, alt TEXT,
            img TEXT, title TEXT, day TEXT,
            ingested_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
    """)
    cur.close()
    conn.close()

def backfill_xkcd():

    # 1. Setup Connection with Autocommit
    conn = psycopg2.connect(
        host="postgres",
        database="jet_xkcd_db",
        user="postgres",
        password="postgres"
    )
    conn.autocommit = True 
    cur = conn.cursor()
    
    # 2. Ensure Schema and Fresh Table
    cur.execute("CREATE SCHEMA IF NOT EXISTS raw;")
    cur.execute("CREATE SCHEMA IF NOT EXISTS analytics;")
    # We use the full list of columns expected from the API
    cur.execute("""
        CREATE TABLE IF NOT EXISTS raw.xkcd_comics (
            num INTEGER PRIMARY KEY,
            month TEXT, link TEXT, year TEXT, news TEXT,
            safe_title TEXT, transcript TEXT, alt TEXT,
            img TEXT, title TEXT, day TEXT,
            ingested_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
    """)
    
    # 3. Get the Latest ID from API
    latest_resp = requests.get("https://xkcd.com/info.0.json")
    latest_id = latest_resp.json()['num']
    
    # 4. Find where we left off
    cur.execute("SELECT MAX(num) FROM raw.xkcd_comics;")
    result = cur.fetchone()[0]
    start_id = result if result else 0
    
    print(f"Starting ingestion from #{start_id + 1} to #{latest_id}")

    # 5. Ingesting all historical data once (backfilling)
    for comic_id in range(start_id + 1, latest_id + 1):
        if comic_id == 404: continue # Skip the 404 joke
            
        try:
            r = requests.get(f"https://xkcd.com/{comic_id}/info.0.json", timeout=10)
            if r.status_code == 200:
                data = r.json()
                
                # We extract keys and values dynamically
                columns = data.keys()
                values = [data[col] for col in columns]
                
                placeholders = ",".join(["%s"] * len(columns))
                insert_query = f"INSERT INTO raw.xkcd_comics ({','.join(columns)}) VALUES ({placeholders}) ON CONFLICT (num) DO NOTHING"
                
                cur.execute(insert_query, values)
                if comic_id % 50 == 0: # Print progress every 50
                    print(f"Progress: Ingested up to #{comic_id}")
                    
        except Exception as e:
            print(f"Error on #{comic_id}: {e}")
            continue

    cur.close()
    conn.close()
    print("Full backfill complete!")

# def backfill_xkcd():
    conn = psycopg2.connect(
        host="postgres",
        database="jet_xkcd_db",
        user="postgres",
        password="postgres"
    )
    conn.autocommit = True 
    cur = conn.cursor()
    
    # FORCE THESE FIRST
    print("--- Executing Schema Creation ---")
    cur.execute("CREATE SCHEMA IF NOT EXISTS raw;")
    cur.execute("CREATE SCHEMA IF NOT EXISTS analytics;")
    
    # Try to get the latest ID
    try:
        latest_resp = requests.get("https://xkcd.com/info.0.json", timeout=10)
        latest_id = latest_resp.json()['num']
    except Exception as e:
        print(f"API Error: {e}")
        latest_id = 3000 # Fallback for testing if API is down
    
    # Create Table
    cur.execute("""
        CREATE TABLE IF NOT EXISTS raw.xkcd_comics (
            num INTEGER PRIMARY KEY,
            month TEXT, link TEXT, year TEXT, news TEXT,
            safe_title TEXT, transcript TEXT, alt TEXT,
            img TEXT, title TEXT, day TEXT,
            ingested_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
    """)

    # Check how many we have
    cur.execute("SELECT COUNT(*) FROM raw.xkcd_comics;")
    count = cur.fetchone()[0]
    print(f"Current count in DB: {count}")

    # For testing: just fetch the first 5 comics to prove it works
    for comic_id in range(1, 6):
        r = requests.get(f"https://xkcd.com/{comic_id}/info.0.json")
        if r.status_code == 200:
            data = r.json()
            columns = data.keys()
            values = [data[col] for col in columns]
            placeholders = ",".join(["%s"] * len(columns))
            query = f"INSERT INTO raw.xkcd_comics ({','.join(columns)}) VALUES ({placeholders}) ON CONFLICT DO NOTHING"
            cur.execute(query, values)
            print(f"Manually ingested #{comic_id}")

    cur.close()
    conn.close()

def check_for_new_comic():
    """
    Polling Logic: This function checks if the latest comic on the API
    is greater than the latest comic in our database.
    """
    conn = get_db_connection()
    cur = conn.cursor()
    
    # Get local max
    cur.execute("SELECT MAX(num) FROM raw.xkcd_comics;")
    local_max = cur.fetchone()[0] or 0
    
    # Get API max
    latest_resp = requests.get("https://xkcd.com/info.0.json")
    api_max = latest_resp.json()['num']
    
    cur.close()
    conn.close()
    
    # If API has a higher number, the sensor "succeeds" and the next task runs
    return api_max > local_max

with DAG(
    dag_id='xkcd_master_pipeline',
    start_date=datetime(2026, 1, 1),
    schedule_interval='0 12 * * 1,3,5', 
    catchup=False,
    default_args={
        'retries': 5,
        'retry_delay': timedelta(minutes=30),
    }
) as dag:

    # 1. NEW: Initialize Database (The Foundation)
    # This prevents the Sensor from failing on a missing table.
    init_db = PythonOperator(
        task_id='init_db_objects',
        python_callable=setup_database # Make sure this function is defined above
    )

    # 2. The Poller: Only runs after we know the table exists
    wait_for_comic = PythonSensor(
        task_id='wait_for_new_comic',
        python_callable=check_for_new_comic,
        poke_interval=600, 
        timeout=3600 * 12, 
        mode='reschedule'  
    )

    # 3. The Ingestor: Fetches the data once the sensor finds something new
    ingest_data = PythonOperator(
        task_id='ingest_xkcd_data',
        python_callable=backfill_xkcd 
    )

    # 4. Transformation & Quality Control
    with TaskGroup(group_id='dbt_pipeline') as dbt_pipeline:
        
        dbt_run = BashOperator(
            task_id='dbt_run',
            bash_command='cd /opt/airflow/dbt_xkcd && dbt run --profiles-dir .',
        )

        dbt_test = BashOperator(
            task_id='dbt_test',
            bash_command='cd /opt/airflow/dbt_xkcd && dbt test --profiles-dir .',
        )

        dbt_run >> dbt_test

    # 5. Documentation update
    generate_docs = BashOperator(
        task_id='dbt_generate_docs',
        bash_command='cd /opt/airflow/dbt_xkcd && dbt docs generate --profiles-dir .',
    )

    # FINAL SERIALIZATION:
    # 1. Create Table -> 2. Wait for New Data -> 3. Fetch Data -> 4. Transform & Test -> 5. Docs
    init_db >> wait_for_comic >> ingest_data >> dbt_pipeline >> generate_docs
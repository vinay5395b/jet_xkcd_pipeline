from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.sensors.python import PythonSensor
from airflow.operators.bash import BashOperator
from airflow.utils.task_group import TaskGroup
import json
from datetime import datetime, timedelta
import requests
import time
import psycopg2

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
    # cur.execute("CREATE SCHEMA IF NOT EXISTS analytics;")
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
    conn = get_db_connection()
    conn.autocommit = True 
    cur = conn.cursor()

    # 1. Get the latest ID from the API
    latest_id = requests.get("https://xkcd.com/info.0.json").json()['num']

    # 2. Get EVERY ID currently in the database to find holes
    cur.execute("SELECT num FROM raw.xkcd_comics;")
    existing_ids = {row[0] for row in cur.fetchall()}

    print(f"Latest API ID is {latest_id}. You have {len(existing_ids)} records.")
    print("Checking for missing comics...")

    # 3. Iterate through the full range 1 to Latest
    for comic_id in range(1, latest_id + 1):
        if comic_id == 404: continue 
        
        # SKIP if we already have it
        if comic_id in existing_ids:
            continue
            
        # FETCH if it's missing
        try:
            r = requests.get(f"https://xkcd.com/{comic_id}/info.0.json", timeout=10)
           
            if r.status_code == 200:
                data = r.json()
                
                
                target_columns = [
                    'num', 'month', 'link', 'year', 'news', 'safe_title', 
                    'transcript', 'alt', 'img', 'title', 'day'
                ]
                
               
                values = []
                for col in target_columns:
                    val = data.get(col) 
                    
                    # use our dict fix just in case one of our target fields changes type ()
                    if isinstance(val, (dict, list)):
                        val = json.dumps(val)
                    values.append(val)
                
                # 3. Insert only our targeted columns
                placeholders = ",".join(["%s"] * len(target_columns))
                insert_query = f"""
                    INSERT INTO raw.xkcd_comics ({','.join(target_columns)}) 
                    VALUES ({placeholders}) 
                    ON CONFLICT (num) DO NOTHING
                """
                cur.execute(insert_query, values)
                print(f"Successfully filled gap: #{comic_id}")
                    
        except Exception as e:
            print(f"Error on #{comic_id}: {e}")
            continue

    cur.close()
    conn.close()


def check_for_new_comic():
    """
    Polling Logic: This function checks if the latest comic on the API
    is greater than the latest comic in our database.
    """
    conn = get_db_connection()
    cur = conn.cursor()
    
    # Check actual count instead of just the ceiling
    cur.execute("SELECT COUNT(*) FROM raw.xkcd_comics;")
    local_count = cur.fetchone()[0]
    
    # Get API max
    latest_resp = requests.get("https://xkcd.com/info.0.json")
    api_max = latest_resp.json()['num']
    
    cur.close()
    conn.close()
    
    # If we have fewer records than the API max, something is missing!
    # (Subtracting 1 because of the 404 comic gap)
    return local_count < (api_max - 1)

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
        poke_interval=3600,
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
    # 1. Create Table/Schema -> 2. Wait for New Data -> 3. Fetch Data -> 4. Transform & Test -> 5. Docs
    init_db >> wait_for_comic >> ingest_data >> dbt_pipeline >> generate_docs
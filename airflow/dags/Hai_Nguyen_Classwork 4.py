from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import pandas as pd
import sqlite3
import os

# Define file paths
# NOTE: Assumes Airflow runs as a user with home dir /home/anadir
# If your setup is different, you might need to use /opt/airflow/
DATA_PATH = '/home/vboxuser/airflow/datasets/weather_data.csv'
DB_PATH = '/home/vboxuser/airflow/databases/weather_database.db'


# Default args for the DAG
default_args = {
    'owner': 'Hai',
    'depend_on_past': False,
    'start_date': datetime(2025, 10, 28),
    'retries': 1,
    'retry_delay': timedelta(minutes=2)
}

# --- Task 1: Extract ---
def extract_data(**kwargs):
    """
    Extracts data by pushing the source file path to XCom.
    This task "extracts" the data's location for the next task.
    """
    print(f"Extracting data from: {DATA_PATH}")
    
    # Check if file exists before pushing
    if not os.path.exists(DATA_PATH):
        raise FileNotFoundError(f"Source file not found: {DATA_PATH}")
    
    # Push the file path to XCom for the transform task to pull
    kwargs['ti'].xcom_push(key='source_file_path', value=DATA_PATH)

# --- Task 2: Transform ---
def transform_data(**kwargs):
    """
    Pulls the source file path, transforms the data,
    saves it to a new temp file, and pushes the new temp file's path.
    """
    # Pull the source file path from XCom (pushed by extract_task)
    source_path = kwargs['ti'].xcom_pull(task_ids='extract_task', key='source_file_path')
    
    print(f"Transforming data from: {source_path}")
    df = pd.read_csv(source_path)

    # 1. Handle missing values in 'temperature' with the median
    median_temp = df['temperature'].median()
    df['temperature'].fillna(median_temp, inplace=True)
    print(f"Filled missing temperatures with median: {median_temp}")

    # 2. Add 'feels_like_temperature' column
    # Formula: AT = Ta + 0.33*E - 0.7*WS - 4
    df['feels_like_temperature'] = df['temperature'] + (0.33 * df['humidity']) - (0.7 * df['wind_speed']) - 4
    print("Added 'feels_like_temperature' column.")

    # Define the transformed path locally
    transformed_path = '/tmp/transformed_weather_data.csv'

    # Save transformed data to a new CSV file
    df.to_csv(transformed_path, index=False)
    print(f"Saved transformed data to: {transformed_path}")

    # Push the path of the *new* transformed file to XCom for the load task
    kwargs['ti'].xcom_push(key='transformed_file_path', value=transformed_path)

# --- Task 3: Load ---
def load_data(**kwargs):
    """
    Pulls the transformed file path, reads the data,
    and loads it into the SQLite database.
    """
    # Pull the transformed file path from XCom (pushed by transform_task)
    transformed_path = kwargs['ti'].xcom_pull(task_ids='transform_task', key='transformed_file_path')
    
    print(f"Loading data from: {transformed_path}")
    df = pd.read_csv(transformed_path)

    # Ensure the database directory exists
    db_dir = os.path.dirname(DB_PATH)
    if not os.path.exists(db_dir):
        print(f"Creating database directory: {db_dir}")
        os.makedirs(db_dir, exist_ok=True)

    # Connect to the SQLite database
    conn = sqlite3.connect(DB_PATH)

    # Load data into the 'weather_data' table
    # 'if_exists='append'' will add the data each time the DAG runs
    df.to_sql('weather_data', conn, if_exists='append', index=False)

    # Commit changes and close the connection
    conn.commit()
    conn.close()
    
    print(f"Successfully loaded {len(df)} rows into 'weather_data' table in {DB_PATH}")
    
    # Clean up the temporary file
    if os.path.exists(transformed_path):
        os.remove(transformed_path)
        print(f"Cleaned up temp file: {transformed_path}")


# --- DAG Definition ---
with DAG(
    'weather_etl_pipeline',
    default_args=default_args,
    description='ETL pipeline for weather data using file paths in XCom',
    schedule='@daily',
    catchup=False
) as dag:

    extract_task = PythonOperator(
        task_id='extract_task',
        python_callable=extract_data,
    )

    transform_task = PythonOperator(
        task_id='transform_task',
        python_callable=transform_data,
    )

    load_task = PythonOperator(
        task_id='load_task',
        python_callable=load_data,
    )

    extract_task >> transform_task >> load_task


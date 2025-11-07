from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.trigger_rule import TriggerRule
from datetime import datetime, timedelta
import pandas as pd
import sqlite3
import os

# -------------------- File Paths --------------------
RAW_CSV_PATH = '/home/vboxuser/airflow/datasets/global_power_plant_database.csv'
DB_PATH = '/home/vboxuser/airflow/databases/power_plant.db'

# -------------------- Default Args --------------------
default_args = {
    'owner': 'Hai',
    'depends_on_past': False,
    'start_date': datetime(2025, 11, 4),
    'retries': 1,
    'retry_delay': timedelta(minutes=2)
}

# -------------------- DAG Definition --------------------
dag = DAG(
    'power_plant_etl_pipeline_local',
    default_args=default_args,
    description='ETL pipeline for global power plant data using local CSV file',
    schedule='@daily',
    catchup=False
)

# -------------------- Task 1: Extract --------------------
def extract_data(**kwargs):
    print(f"Using local file: {RAW_CSV_PATH}")
    if not os.path.exists(RAW_CSV_PATH):
        raise FileNotFoundError(f"Source file not found: {RAW_CSV_PATH}")
    kwargs['ti'].xcom_push(key='source_file_path', value=RAW_CSV_PATH)

# -------------------- Task 2: Transform --------------------
def transform_data(**kwargs):
    source_path = kwargs['ti'].xcom_pull(task_ids='extract_task', key='source_file_path')
    print(f"Transforming data from: {source_path}")
    df = pd.read_csv(source_path)

    # Convert numeric columns safely
    df['capacity_mw'] = pd.to_numeric(df['capacity_mw'], errors='coerce')
    df['commissioning_year'] = pd.to_numeric(df['commissioning_year'], errors='coerce')

    # Fill missing values
    df['capacity_mw'].fillna(df['capacity_mw'].median(), inplace=True)
    df['commissioning_year'].fillna(df['commissioning_year'].median(), inplace=True)

    # Add age_of_plant
    current_year = datetime.now().year
    df['age_of_plant'] = current_year - df['commissioning_year']

    # Save transformed data to temp file
    transformed_path = '/tmp/transformed_power_plants.csv'
    df.to_csv(transformed_path, index=False)
    print(f"Saved transformed data to: {transformed_path}")
    kwargs['ti'].xcom_push(key='transformed_file_path', value=transformed_path)

# -------------------- Task 3: Validate --------------------
def validate_data(**kwargs):
    transformed_path = kwargs['ti'].xcom_pull(task_ids='transform_task', key='transformed_file_path')
    print(f"Validating data from: {transformed_path}")
    df = pd.read_csv(transformed_path)

    # Check critical columns
    if df[['capacity_mw', 'commissioning_year']].isnull().any().any():
        raise ValueError("Validation failed: Missing values remain in critical columns.")
    print("✅ Validation passed: No missing values in critical columns.")

# -------------------- Task 4: Load --------------------
def load_data(**kwargs):
    transformed_path = kwargs['ti'].xcom_pull(task_ids='transform_task', key='transformed_file_path')
    print(f"Loading data from: {transformed_path}")
    df = pd.read_csv(transformed_path)

    # Ensure DB directory exists
    db_dir = os.path.dirname(DB_PATH)
    os.makedirs(db_dir, exist_ok=True)

    # Connect to SQLite and create table if not exists
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS power_plant_data (
            country TEXT, country_long TEXT, name TEXT, gppd_idnr TEXT,
            capacity_mw REAL, latitude REAL, longitude REAL, primary_fuel TEXT,
            other_fuel1 TEXT, other_fuel2 TEXT, other_fuel3 TEXT,
            commissioning_year REAL, owner TEXT, source TEXT, url TEXT,
            geolocation_source TEXT, wepp_id TEXT, year_of_capacity_data REAL,
            generation_gwh_2013 REAL, generation_gwh_2014 REAL, generation_gwh_2015 REAL,
            generation_gwh_2016 REAL, generation_gwh_2017 REAL, generation_gwh_2018 REAL,
            generation_gwh_2019 REAL, generation_data_source TEXT,
            estimated_generation_gwh_2013 REAL, estimated_generation_gwh_2014 REAL,
            estimated_generation_gwh_2015 REAL, estimated_generation_gwh_2016 REAL,
            estimated_generation_gwh_2017 REAL,
            estimated_generation_note_2013 TEXT, estimated_generation_note_2014 TEXT,
            estimated_generation_note_2015 TEXT, estimated_generation_note_2016 TEXT,
            estimated_generation_note_2017 TEXT, age_of_plant REAL
        )
    ''')

    # Load data into table
    df.to_sql('power_plant_data', conn, if_exists='append', index=False)
    conn.commit()
    conn.close()
    print(f"✅ Loaded {len(df)} rows into {DB_PATH}")

    # Clean up temp file
    if os.path.exists(transformed_path):
        os.remove(transformed_path)
        print(f"Cleaned up temp file: {transformed_path}")

# -------------------- Operators --------------------
extract_task = PythonOperator(task_id='extract_task', python_callable=extract_data, dag=dag)
transform_task = PythonOperator(task_id='transform_task', python_callable=transform_data, dag=dag)
validate_task = PythonOperator(task_id='validate_task', python_callable=validate_data, dag=dag, trigger_rule=TriggerRule.ALL_SUCCESS)
load_task = PythonOperator(task_id='load_task', python_callable=load_data, dag=dag, trigger_rule=TriggerRule.ALL_SUCCESS)

# -------------------- DAG Flow --------------------
extract_task >> transform_task >> validate_task >> load_task

from airflow import DAG 
from airflow.operators.python import PythonOperator
from datetime import datetime 
import pandas as pd 
import sqlite3

DATA_PATH = '/home/anadir/airflow/datasets/weather_data.csv'
DB_PATH = '/home/anadir/airflow/databases/weather_database.db'

#Default args for our DAG 
default_args=(
    'owner': 'Hai',
    'depend_on_past':False,
    'start_date': datetime(2025,10,28),
    'retries:' 1, 
    'retry_delay': timedelta(minutes=2)
)

dag = DAG(
    'weatherdata_etl',
    default_args=default_args, 
    scchedule_interval='@daily',
    description = 'Very fancy ETL pipeline for weather data',
)

#Task 1: Extract data from csv 
def extract_weather_data(**kwargs):
    #read the weather data from the CSV in path
    df = pd.read_csv(DATA_PATH)
    #cast the dataframe to dictionary and push it to xcom
    kwargs['ti'].xcom_push(key='weather_data', value=df.to_dict())

extract_task = PythonOperator(
    task_id='extract_task',
    python_callable = extract_weather_data,
    dag = dag,
    #provide_context = True
)

#Task 2: Handle missing value, calculate feels_like_tmp 
def transform_data(**kwargs):
    wd_dict = kwargs['ti'].xcom_pull(key='weather_data')
    df = pd.DataFrame.from_dict(wd_dict)

    #fill missing value with median values in temperature column
    df['temperature'].fillna(df['temperature'].median(),inplace=True)

    df['feels_like_temperature'] = df['temperature'] + (0.33 * df['humidity'] - (0.7 * df['wind_speed'])) - 4

    transformed_path = '/tmp/transformed_weather_data.csv'
    df.to_csv(transformed_path, index=False)

    kwargs['ti'].xcom(key='file_path', value=transformed_path)

transform_task = PythonOperator(
    task_id = 'transform_task',
    python_callable=transform_data, 
    dag = dag,
)

def load_data(**kwargs):
    fp = kwargs['ti'].xcom_pull(task_ids='transform_task', key='file_path')
    df = pd.read_csv(fp)

    conn = sqlite3.connect(DB_PATH)

    df.to_sql('weather_data', conn, if_exists = 'append', index = False)

    conn.commit()
    conn.close()

load_task = PythonOperator(
    task_id = 'load_task',
    python_callable=load_data,
    dag = dag, 
)
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.trigger_rule import TriggerRule
import pandas as pd
import zipfile
import os
import sqlite3
import numpy as np
from datetime import datetime, timedelta
import subprocess

# Default args
default_args = {
    'owner': 'Hai',
    'depends_on_past': False,
    'start_date': datetime(2025, 11, 6),
    'retries': 1,
    'retry_delay': timedelta(minutes=2)
}

# Wind categorization helper
def categorize_wind(speed):
    speed_mps = speed / 3.6
    if speed_mps <= 1.5: return 'Calm'
    elif speed_mps <= 3.3: return 'Light Air'
    elif speed_mps <= 5.4: return 'Light Breeze'
    elif speed_mps <= 7.9: return 'Gentle Breeze'
    elif speed_mps <= 10.7: return 'Moderate Breeze'
    elif speed_mps <= 13.8: return 'Fresh Breeze'
    elif speed_mps <= 17.1: return 'Strong Breeze'
    elif speed_mps <= 20.7: return 'Near Gale'
    elif speed_mps <= 24.4: return 'Gale'
    elif speed_mps <= 28.4: return 'Strong Gale'
    elif speed_mps <= 32.6: return 'Storm'
    else: return 'Violent Storm'

# Extraction with Kaggle API
def extract_data(**kwargs):
    dataset = "muthuj7/weather-dataset"  # Correct dataset slug
    extract_dir = "/home/vboxuser/airflow/datasets"
    os.makedirs(extract_dir, exist_ok=True)

    print("Downloading dataset from Kaggle...")
    subprocess.run(["kaggle", "datasets", "download", "-d", dataset, "-p", extract_dir], check=True)

    zip_path = os.path.join(extract_dir, "weather-dataset.zip")
    print(f"Extracting {zip_path}...")
    with zipfile.ZipFile(zip_path, 'r') as zip_ref:
        zip_ref.extractall(extract_dir)

    file_path = os.path.join(extract_dir, "weatherHistory.csv")
    if not os.path.exists(file_path):
        raise FileNotFoundError(f"File not found after extraction: {file_path}")

    kwargs['ti'].xcom_push(key='weather_data_path', value=file_path)

# Transformation
def transform_data(**kwargs):
    file_path = kwargs['ti'].xcom_pull(key='weather_data_path')
    df = pd.read_csv(file_path)

    # Data Cleaning
    df['Formatted Date'] = pd.to_datetime(df['Formatted Date'], utc=True, errors='coerce')
    df.dropna(subset=['Formatted Date', 'Temperature (C)', 'Humidity', 'Wind Speed (km/h)'], inplace=True)
    df.drop_duplicates(inplace=True)
    df.set_index('Formatted Date', inplace=True)
    df.sort_index(inplace=True)

    # Daily averages
    daily_df = df.resample('D').mean(numeric_only=True)
    daily_df['avg_temperature_c'] = daily_df['Temperature (C)']
    daily_df['avg_humidity'] = daily_df['Humidity']
    daily_df['avg_wind_speed_kmh'] = daily_df['Wind Speed (km/h)']
    daily_df['wind_strength'] = daily_df['Wind Speed (km/h)'].apply(categorize_wind)
    daily_df.reset_index(inplace=True)

    # Monthly aggregates
    df_monthly = df.copy()
    df_monthly['month'] = df_monthly.index.to_period('M')
    mode_precip = df_monthly.groupby('month')['Precip Type'].agg(
        lambda x: x.mode()[0] if len(x.mode()) == 1 else np.nan
    )

    monthly_df = df.resample('M').mean(numeric_only=True)
    monthly_df['avg_temperature_c'] = monthly_df['Temperature (C)']
    monthly_df['avg_apparent_temperature_c'] = monthly_df['Apparent Temperature (C)']
    monthly_df['avg_humidity'] = monthly_df['Humidity']
    monthly_df['avg_visibility_km'] = monthly_df['Visibility (km)']
    monthly_df['avg_pressure_millibars'] = monthly_df['Pressure (millibars)']
    monthly_df['mode_precip_type'] = mode_precip.values
    monthly_df.reset_index(inplace=True)

    # Save transformed data
    daily_path = '/home/vboxuser/airflow/datasets/daily_weather.csv'
    monthly_path = '/home/vboxuser/airflow/datasets/monthly_weather.csv'
    daily_df.to_csv(daily_path, index=False)
    monthly_df.to_csv(monthly_path, index=False)

    kwargs['ti'].xcom_push(key='daily_path', value=daily_path)
    kwargs['ti'].xcom_push(key='monthly_path', value=monthly_path)

# Validation
def validate_data(**kwargs):
    daily_path = kwargs['ti'].xcom_pull(key='daily_path')
    monthly_path = kwargs['ti'].xcom_pull(key='monthly_path')
    daily_df = pd.read_csv(daily_path)
    monthly_df = pd.read_csv(monthly_path)

    # Missing values
    for df_check in [daily_df, monthly_df]:
        if df_check[['Temperature (C)', 'Humidity', 'Wind Speed (km/h)']].isnull().any().any():
            raise ValueError("Missing values detected")

    # Range checks
    for df_check in [daily_df, monthly_df]:
        if not df_check['Temperature (C)'].between(-50, 50).all():
            raise ValueError("Temperature out of range")
        if not df_check['Humidity'].between(0, 1).all():
            raise ValueError("Humidity out of range")
        if not (df_check['Wind Speed (km/h)'] >= 0).all():
            raise ValueError("Negative wind speed detected")

    # Outlier detection
    z_scores = (daily_df['Temperature (C)'] - daily_df['Temperature (C)'].mean()) / daily_df['Temperature (C)'].std()
    if (abs(z_scores) > 3).any():
        print("Warning: Outliers detected in daily temperature")

# Load
def load_data(**kwargs):
    daily_path = kwargs['ti'].xcom_pull(key='daily_path')
    monthly_path = kwargs['ti'].xcom_pull(key='monthly_path')
    daily_df = pd.read_csv(daily_path)
    monthly_df = pd.read_csv(monthly_path)

    db_path = '/home/vboxuser/airflow/datasets/weather.db'
    conn = sqlite3.connect(db_path)

    # Add ID column
    daily_df.insert(0, 'id', range(1, len(daily_df) + 1))
    monthly_df.insert(0, 'id', range(1, len(monthly_df) + 1))

    daily_df.to_sql('daily_weather', conn, if_exists='replace', index=False)
    monthly_df.to_sql('monthly_weather', conn, if_exists='replace', index=False)
    conn.close()

# DAG definition
with DAG(
    dag_id='weather_history_pipeline',
    default_args=default_args,
    schedule_interval='@daily',
    catchup=False,
    tags=['etl', 'weather', 'pandas']
) as dag:

    t1 = PythonOperator(task_id='extract_data', python_callable=extract_data)
    t2 = PythonOperator(task_id='transform_data', python_callable=transform_data)
    t3 = PythonOperator(task_id='validate_data', python_callable=validate_data, trigger_rule=TriggerRule.ALL_SUCCESS)
    t4 = PythonOperator(task_id='load_data', python_callable=load_data)

    t1 >> t2 >> t3 >> t4

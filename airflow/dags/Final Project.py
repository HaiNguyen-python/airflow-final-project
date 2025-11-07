from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
from airflow.utils.trigger_rule import TriggerRule
import pandas as pd
import zipfile
import os
import sqlite3
import numpy as np
from datetime import datetime, timedelta

# Default args for the DAG
default_args = {
    'owner': 'Hai',
    'depends_on_past': False,
    'start_date': datetime(2025, 11, 6),
    'retries': 1,
    'retry_delay': timedelta(minutes=2)
}

# --- Helper Function ---
# Moved categorize_wind outside transform_data so it's a clean helper
def categorize_wind(speed):
    """Categorizes wind speed (km/h) into Beaufort scale names."""
    speed_mps = speed / 3.6  # Convert km/h to m/s
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

# --- Task Functions ---

def extract_data(**kwargs):
    zip_path = "/home/vboxuser/airflow/datasets/weatherHistory.csv.zip"
    extract_dir = "/home/vboxuser/airflow/datasets"
    os.makedirs(extract_dir, exist_ok=True)
    
    print(f"Extracting {zip_path} to {extract_dir}...")
    with zipfile.ZipFile(zip_path, 'r') as zip_ref:
        zip_ref.extractall(extract_dir)
        
    file_path = os.path.join(extract_dir, "weatherHistory.csv")
    
    # Check if file exists after extraction
    if not os.path.exists(file_path):
        raise FileNotFoundError(f"File not found after extraction: {file_path}")
        
    print(f"File extracted to: {file_path}")
    kwargs['ti'].xcom_push(key='weather_data_path', value=file_path)

def transform_data(**kwargs):
    file_path = kwargs['ti'].xcom_pull(key='weather_data_path')
    if not file_path:
        raise ValueError("Could not find 'weather_data_path' in XCom. Did extract_data run?")
        
    print(f"Transforming data from: {file_path}")
    df = pd.read_csv(file_path)

    # Data Cleaning:
    df['Formatted Date'] = pd.to_datetime(df['Formatted Date'], utc=True, errors='coerce') # <-- FIX: Best to use utc=True for resampling
    df.dropna(subset=['Formatted Date'], inplace=True)
    df.dropna(subset=['Temperature (C)', 'Humidity', 'Wind Speed (km/h)'], inplace=True)
    df.drop_duplicates(inplace=True) # <-- FIX: Was df.drop.duplicates
    df.set_index('Formatted Date', inplace=True)
    df.sort_index(inplace=True) # Good practice before resampling

    # Daily Averages
    print("Resampling for daily data...")
    daily_df = df.resample('D').mean(numeric_only=True) # <-- FIX: Use numeric_only=True
    daily_df.reset_index(inplace=True)
    
    # <-- FIX: Moved wind categorization here to apply to daily data -->
    if 'Wind Speed (km/h)' in daily_df.columns:
        daily_df['wind_strength'] = daily_df['Wind Speed (km/h)'].apply(categorize_wind)
    else:
        print("Warning: 'Wind Speed (km/h)' not in daily_df, can't categorize wind.")

    # Monthly Mode Precip Type
    print("Resampling for monthly data...")
    # Re-read index for this operation
    df_monthly = df.copy()
    df_monthly['month'] = df_monthly.index.to_period('M')
    
    # Handle potential empty modes
    mode_precip = df_monthly.groupby('month')['Precip Type'].agg(lambda x: x.mode().iloc[0] if not x.mode().empty else np.nan)
    
    monthly_df = df.resample('M').mean(numeric_only=True) # <-- FIX: Use numeric_only=True
    
    # Align mode_precip index (Period) with monthly_df index (Datetime)
    monthly_df['most_precip_type'] = mode_precip.values
    monthly_df.reset_index(inplace=True)

    # Save Transformed Data
    daily_path = '/home/vboxuser/airflow/datasets/daily_weather.csv'
    monthly_path = '/home/vboxuser/airflow/datasets/monthly_weather.csv'
    
    print(f"Saving daily data to: {daily_path}")
    daily_df.to_csv(daily_path, index=False)
    
    print(f"Saving monthly data to: {monthly_path}")
    monthly_df.to_csv(monthly_path, index=False)

    kwargs['ti'].xcom_push(key='daily_path', value=daily_path)
    kwargs['ti'].xcom_push(key='monthly_path', value=monthly_path) # <-- FIX: Key is 'monthly_path'

def validate_data(**kwargs):
    daily_path = kwargs['ti'].xcom_pull(key='daily_path')
    monthly_path = kwargs['ti'].xcom_pull(key='monthly_path') # <-- FIX: Was 'montly_path'

    if not daily_path or not os.path.exists(daily_path):
        raise FileNotFoundError("Daily data file not found.")
    if not monthly_path or not os.path.exists(monthly_path):
        raise FileNotFoundError("Monthly data file not found.")

    print(f"Validating {daily_path} and {monthly_path}...")
    daily_df = pd.read_csv(daily_path)
    monthly_df = pd.read_csv(monthly_path)

    # Missing Values Check
    cols_to_check = ['Temperature (C)', 'Humidity', 'Wind Speed (km/h)']
    if daily_df[cols_to_check].isnull().any().any():
        raise ValueError("Missing values found in daily data")
    if monthly_df[cols_to_check].isnull().any().any():
        raise ValueError("Missing values found in monthly data")

    # Range Check
    if not daily_df['Temperature (C)'].between(-50, 50).all():
        raise ValueError("Daily temperature out of range [-50, 50]")
    if not daily_df['Humidity'].between(0, 1).all():
        raise ValueError("Daily humidity out of range [0, 1]")
    if not (daily_df['Wind Speed (km/h)'] >= 0).all():
        raise ValueError("Daily data has negative wind speed")
    
    # Outlier Detection (using the correct column name)
    z_scores = (daily_df['Temperature (C)'] - daily_df['Temperature (C)'].mean()) / daily_df['Temperature (C)'].std() # <-- FIX: Both used 'Temperature (C)'
    if (abs(z_scores) > 3).any():
        print("Warning: Outliers (z-score > 3) detected in daily temperature")
        
    print("Validation successful.")

def load_data(**kwargs):
    daily_path = kwargs['ti'].xcom_pull(key='daily_path')
    monthly_path = kwargs['ti'].xcom_pull(key='monthly_path') # <-- FIX: Was 'montly_path'

    if not daily_path or not os.path.exists(daily_path):
        raise FileNotFoundError("Daily data file not found for loading.")
    if not monthly_path or not os.path.exists(monthly_path):
        raise FileNotFoundError("Monthly data file not found for loading.")

    daily_df = pd.read_csv(daily_path)
    monthly_df = pd.read_csv(monthly_path)

    db_path = '/home/vboxuser/airflow/datasets/weather.db' # <-- FIX: Use an absolute path
    print(f"Loading data into database: {db_path}")
    conn = sqlite3.connect(db_path)
    
    daily_df.to_sql('daily_weather', conn, if_exists='replace', index=False)
    monthly_df.to_sql('monthly_weather', conn, if_exists='replace', index=False) # <-- FIX: Was 'montly_weather'
    
    conn.close()
    print("Data loaded successfully.")

# --- DAG Definition ---

with DAG(
    dag_id='weather_history_pipeline',
    default_args=default_args,
    schedule='@daily',
    catchup=False,
    tags=['etl', 'weather', 'pandas']
) as dag:

    # <-- FIX: All task definitions moved here, to the main DAG context -->
    
    t1 = PythonOperator(
        task_id='extract_data',
        python_callable=extract_data
    )

    t2 = PythonOperator(
        task_id='transform_data',
        python_callable=transform_data
    )

    t3 = PythonOperator(
        task_id='validate_data',
        python_callable=validate_data,
        trigger_rule=TriggerRule.ALL_SUCCESS 
    )

    t4 = PythonOperator(
        task_id='load_data',
        python_callable=load_data
    )

    # <-- FIX: Dependency chain also moved here -->
    t1 >> t2 >> t3 >> t4
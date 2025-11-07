from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from datetime import datetime
import random

#Default arguments
default_args = {
    'owner': 'Hai',
    'start_date': datetime(2025, 10,28),
}

#Define the DAG
dag = DAG(
    dag_id='random_number_check',
    default_args=default_args,
    schedule='@daily',
    catchup=False,
)

#Task 1: Generate random number 
def generate_random_number(**kwargs):
    number = random.randint(1, 100)
    print(f"Generated number: {number}")
    kwargs['ti'].xcom_push(key='random_number', value=number)

#Task 2: Check divisibility
def check_divisibility(**kwargs):
    number = kwargs['ti'].xcom_pull(key='random_number', task_ids='generate_number')
    if number % 3 == 0 and number % 5 == 0: 
        result = "multiple of both 3 and 5"
    elif number % 3 == 0: 
        result = "multiple of 3"
    elif number % 5 == 0: 
        result = "multiple of 5"
    else: 
        result = "not a multiple of 3 or 5"
    print(f"Result: {result}")
    kwargs['ti'].xcom_push(key='check_result', value=result)

#Define tasks
generate_task = PythonOperator(
    task_id='generate_number',
    python_callable=generate_random_number,
    dag=dag,
)

check_number = PythonOperator(
    task_id='check_number',
    python_callable=check_divisibility,
    dag=dag,
)

print_result = BashOperator(
    task_id='print_result',
    bash_command='echo "{{ti.xcom_pull(key=\'check_result\', task_ids=\'check_number\')}}"',
    dag=dag,
)

#Set task dependencies
generate_task >> check_number >> print_result
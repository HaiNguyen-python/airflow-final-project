from airflow import DAG 
from airflow.operators.bash_operator import BashOperator
from datetime import datetime, timedelta

default_arg = {
	'owner' = 'Hai',
	'start-date': datatime(2025, 10, 09),
	'depends_on_past': False,
	'retries': 1,
	'retry_delay' = timedelta(minutes=5),
}

dag = DAG (
	'example1_dag',
	default_args=default_arg,
	schedule_intercal=timedelta(days=1),
}

task1 = BashOperator(
	task_id='task01',
	bash_command='echo "Hello from Airflow!"',
	dag = dag,
}

task2 = BashOperator(
	task_id='task02',
	bash_command='echo "Goodbye from Airflow!",
	dag = dag,
}

task1 >> task2

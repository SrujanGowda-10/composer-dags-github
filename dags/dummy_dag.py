from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta

# Define a simple Python function
def print_hello():
    print("Hello from Airflow!")

# Default arguments for the DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 2, 10),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# Define the DAG
with DAG(
    dag_id='dummy_test_dag',
    default_args=default_args,
    schedule_interval='@daily',  # Runs once a day
    catchup=False,
) as dag:

    # Dummy start task
    start = DummyOperator(
        task_id='start'
    )

    # Python task to print a message
    hello_task = PythonOperator(
        task_id='print_hello',
        python_callable=print_hello
    )

    # Dummy end task
    end = DummyOperator(
        task_id='end'
    )

    # Define task dependencies
    start >> hello_task >> end

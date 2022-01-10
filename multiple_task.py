from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime


def task1():
    print("Hello task1")

def task2():
    print("Hello task2")

def task3():
    print("Hello task3")

def task4():
    print("Hello task4")


with DAG(dag_id = 'hello_world_dag1',
    start_date=datetime(2021,8,19),
    schedule_interval='@hourly',
    catchup=False) as dag:

    python_task1 = PythonOperator(
        task_id="python_task",
        python_callable=task1
    )

    python_task2 = PythonOperator(
        task_id='python_task2',
        python_callable=task2
    )

    python_task3 = PythonOperator(
        task_id='python_task3',
        python_callable=task3
    )

    python_task4 = PythonOperator(
        task_id='python_task4',
        python_callable=task4
    )

python_task1 >> python_task2 >> python_task3 >> python_task4
from airflow.models import DAG
from datetime import datetime, timedelta

from script_python import py_func

from typing import Union

from airflow.operators.python_operator import PythonOperator
from airflow.operators.bash_operator import BashOperator


default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

task1_value ='python'
task2_value ='java'
task3_value ='python'
task4_value ='java'

def build_python_task(dag: DAG, py_task_id_value, script) -> PythonOperator:
    python_task = PythonOperator(
        task_id=py_task_id_value,
        python_callable=script,
        dag=dag,
    )
    return python_task


def build_java_task(dag: DAG, java_task_id_value) -> BashOperator:
    java_task = BashOperator(
        task_id=java_task_id_value,
        bash_command='java -cp /usr/local/custom/HelloWorld.jar HelloWorld',
        dag=dag,
    )
    return java_task

def build_dag_task(dag: DAG,task_value, script, task_id_value) -> Union[PythonOperator, BashOperator]:

    if task_value == 'python':
        python_task = build_python_task(dag=dag, py_task_id_value=task_id_value, script=script)
        return python_task
    elif task_value == 'java':
        java_task= build_java_task(dag=dag, java_task_id_value=task_id_value)
        return java_task

    else:
        pass


with DAG(
        dag_id="modularized_dag",
        schedule_interval="@hourly",
        start_date=datetime(2021, 1, 1),
        default_args=default_args,
        catchup=False
) as dag:
    # logical chunk of tasks

    task1 = build_dag_task(dag=dag, task_value=task1_value, script=py_func, task_id_value="py_task1")
    task2 = build_dag_task(dag=dag, task_value=task2_value, script=None, task_id_value="java_task2")
    task3 = build_dag_task(dag=dag, task_value=task3_value, script=py_func, task_id_value="py_task3")
    task4 = build_dag_task(dag=dag, task_value=task4_value, script=None, task_id_value="java_task4")

    task1 >> task2 >> task3 >> task4




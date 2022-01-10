from airflow.models import DAG
from datetime import datetime, timedelta
from build_task import build_dag_task
from script_python import py_func


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




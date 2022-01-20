from airflow.models import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.operators.bash_operator import BashOperator
from datetime import datetime, timedelta

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

task1_value = 'python'
task2_value = 'java'
task3_value = 'python'
task4_value = 'java'

def py_func():
    print("Hello world!")

def build_python_task(dag: DAG, task_id_value, script) -> PythonOperator:
    python_task = PythonOperator(
        task_id=task_id_value,
        python_callable=script,
        dag=dag,
    )
    return python_task


def build_java_task(dag: DAG, task_id_value) -> BashOperator:
    java_task = BashOperator(
        task_id=task_id_value,
        bash_command='java -cp HelloWorld.jar HelloWorld',
        dag=dag,
    )
    return java_task


with DAG(
        dag_id="java_dag",
        schedule_interval="@hourly",
        start_date=datetime(2021, 1, 1),
        default_args=default_args,
        catchup=False
) as dag:
    # logical chunk of tasks
    if task1_value == 'python':
        task1 = build_python_task(dag=dag, task_id_value="py_task1", script=py_func)
    elif task1_value == 'java':
        task1 = build_java_task(dag=dag, task_id_value="java_task1")
    else:
        pass

    if task2_value == 'python':
        task2 = build_python_task(dag=dag, task_id_value="py_task2", script=py_func)
    elif task2_value == 'java':
        task2 = build_java_task(dag=dag, task_id_value="java_task2")
    else:
        pass

    if task3_value == 'python':
        task3 = build_python_task(dag=dag, task_id_value="py_task3", script=py_func)
    elif task3_value == 'java':
        task3 = build_java_task(dag=dag, task_id_value="java_task3")
    else:
        pass

    if task4_value == 'python':
        task4 = build_python_task(dag=dag, task_id_value="py_task1", script=py_func)
    elif task4_value == 'java':
        task4 = build_java_task(dag=dag, task_id_value="java_task1")
    else:
        pass

    task1 >> task2 >> task3 >> task4

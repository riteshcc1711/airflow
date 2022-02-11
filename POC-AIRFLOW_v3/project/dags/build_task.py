from typing import Union
from airflow.models import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.operators.bash_operator import BashOperator



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






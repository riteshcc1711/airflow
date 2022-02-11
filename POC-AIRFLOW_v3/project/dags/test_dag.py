from airflow.models import DAG
from datetime import datetime, timedelta
from executor_python import func
from typing import Union
from airflow.operators.python_operator import PythonOperator
from airflow.operators.bash_operator import BashOperator
from airflow.models import Variable
from airflow.operators.subdag_operator import SubDagOperator
from script_python import py_func
import os
from importlib.util import spec_from_file_location, module_from_spec
from mongo_load import mongofunc
#DatabricksSubmitRunOperator


def load_module(root, file_name):
    """Load a file_name from root folder
    Args:
        root (str): absolute path to the filder where the file is located
        file_name (str): name of file in root folder which
            needs to be loaded. Exclude .py file extension.
    Returns:
        (module) file loaded as a module
    """
    module_path = os.path.join(root, f"{file_name}.py")
    spec = spec_from_file_location(file_name, module_path)
    module = module_from_spec(spec)
    spec.loader.exec_module(module)
    return module

UTIL_PATH = os.path.abspath(os.path.join(__file__, "..", "..", "cloud"))
FileConfigParser = load_module(UTIL_PATH, "config_parser1").FileConfigParser

#from config_parser1 import FileConfigParser

DAG_CONFIG = FileConfigParser()

# default_args = {
#     'owner': DAG_CONFIG.get('default_args.owner'),
#     'depends_on_past': DAG_CONFIG.get('default_args.depends_on_past'),
#     'email_on_retry': DAG_CONFIG.get('default_args.email_on_retry'),
#     'retries': DAG_CONFIG.get('default_args.retries'),
#     'retry_delay': DAG_CONFIG.get('default_args.retry_delay'),
# }

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

params = Variable.get("params", deserialize_json=True)
task1_value = params['task1']
task2_value = params['task2']
task3_value = params['task3']
task4_value = params['task4']
delay = 'sleep'
paths = Variable.get("paths", deserialize_json=True)
bash_command_java = paths['bash_command_java']
bash_command_delay = paths['bash_command_delay']


def build_python_task(dag: DAG, task_id_value, script) -> PythonOperator:
    python_task = PythonOperator(
        task_id=task_id_value,
        python_callable=script,
        dag=dag,
    )
    return python_task


def build_java_task(dag: DAG, task_id_value, script) -> BashOperator:
    java_task = BashOperator(
        task_id=task_id_value,
        bash_command=script,
        dag=dag,
    )
    return java_task


def build_dag_task(dag: DAG,task_value, script, task_id_value) -> Union[PythonOperator, BashOperator]:

    if task_value == 'python':
        python_task = build_python_task(dag=dag, task_id_value=task_id_value, script=script)
        return python_task
    elif task_value == 'java':
        java_task= build_java_task(dag=dag, task_id_value=task_id_value,script=script)
        return java_task

    elif task_value == 'sleep':
        delay_task= build_java_task(dag=dag, task_id_value=task_id_value,script=script)
        return delay_task
    else:
        pass


def subdag(parent_dag_name, child_dag_name, args,task_id_value, task_value, script,no_of_subtasks):
    dag_subdag = DAG(
        dag_id='{0}.{1}'.format(parent_dag_name, child_dag_name),
        default_args=args,
        schedule_interval="@daily",
        start_date=datetime(2021, 1, 1),
        catchup=False
    )
    with dag_subdag:
        for i in range(no_of_subtasks):
            sub_task_id = '{1}_{0}'.format(i,task_id_value)
            t = build_dag_task(dag=dag_subdag, task_value=task_value, script=script, task_id_value=sub_task_id)

    return dag_subdag


def build_sub_dag_task(dag: DAG,task_value, script, task_id_value,parent_dag_name,child_dag_name,default_args,no_of_subtasks) -> SubDagOperator:

    sub_dag_task = SubDagOperator(
        task_id=task_id_value,
        subdag=subdag(
            parent_dag_name=parent_dag_name,
            child_dag_name=child_dag_name,
            args=default_args,
            task_id_value=task_id_value,
            task_value=task_value,
            script=script,
            no_of_subtasks=no_of_subtasks
        ),
        default_args=default_args,
        dag=dag,
    )
    return sub_dag_task


with DAG(
        dag_id="mongo_dag",
        schedule_interval="@hourly",
        start_date=datetime(2021, 1, 1),
        default_args=default_args,
        catchup=False
) as dag:
    # logical chunk of tasks
    task1 = build_dag_task(dag=dag, task_value=task1_value, script=func, task_id_value="py_task1")
    task2 = build_dag_task(dag=dag, task_value=task2_value, script=bash_command_java, task_id_value="java_task2")
    task3 = build_dag_task(dag=dag, task_value=task3_value, script=bash_command_java, task_id_value="java_task3")
    task4 = build_dag_task(dag=dag, task_value=task4_value, script=mongofunc, task_id_value="py_task4")
    task1 >> task2 >> task3 >> task4




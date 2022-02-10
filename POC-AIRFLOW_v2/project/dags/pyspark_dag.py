import airflow
import time
from datetime import datetime, timedelta
from airflow import DAG
# from airflow.contrib.operators.databricks_operator import DatabricksSubmitRunOperator
from airflow.providers.databricks.operators.databricks import DatabricksSubmitRunOperator
from airflow.models import Variable

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    'start_date': airflow.utils.dates.days_ago(2)
}

with DAG(dag_id='Airflow_Databricks_Integration', default_args=default_args, schedule_interval='@daily') as dag:
    notebook_task_params = {
            # 'new_cluster': new_cluster,
            'existing_cluster_id': '0201-113152-gh78wyan',
            'notebook_task': {
                'notebook_path': '/Users/ritesh.d.jain@accenture.com/test',
            }
        }

    notebook_task = DatabricksSubmitRunOperator(
            task_id='Databricks_task1',
            databricks_conn_id='databricks_default',
            dag=dag,
            json=notebook_task_params)


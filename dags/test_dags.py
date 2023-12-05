from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.utils.task_group import TaskGroup

from datetime import datetime

from utils.ingestion import *
from utils.notification import Notification


# DAG definition
default_args = {
    'owner': 'kel11',
    'depends_on_past':False,
    'on_failure_callback': Notification.push,
    'on_retry_callback': Notification.push,
    'on_success_callback': Notification.push,
    'start_date': datetime(2023, 11, 27)
}

dag = DAG(
    'testing_dags',
    description='DAG for ingesting data files into PostgreSQL',
    default_args=default_args,
    catchup=False,
    schedule_interval='@once',
    tags=['testing']
)

with dag:
    # Task to test the connection
    test_conn_task = PythonOperator(
        task_id='test_connection',
        python_callable=test_conn,
        dag=dag,
    )

    # End of DAG
    end = DummyOperator(
        task_id='end',
        trigger_rule='none_failed' # If none failed then end
    )


# Setting dependencies
test_conn_task >> end
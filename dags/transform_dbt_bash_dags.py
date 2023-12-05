from pendulum import datetime

from airflow import DAG
from airflow.operators.bash_operator import BashOperator

from utils.notification import Notification


DBT_PROJECT_DIR = "/opt/airflow/dags/dbt/dbt_fp"

default_args = {
    'owner': 'kel11',
    'depends_on_past':False,
    'on_failure_callback': Notification.push,
    'on_retry_callback': Notification.push,
    'on_success_callback': Notification.push,
    'start_date': datetime(2023, 11, 27)
}

with DAG(
    "transform_dbt_bash_dags",
    default_args=default_args,
    description="A sample Airflow DAG to invoke dbt runs using a BashOperator",
    schedule_interval='@once',
    catchup=False,
    tags=["transform", "dbt", "bash"]
) as dag:

    dbt_run = BashOperator(
        task_id="dbt_run",
        bash_command=f"dbt run --profiles-dir {DBT_PROJECT_DIR} --project-dir {DBT_PROJECT_DIR}"
    )

    dbt_test = BashOperator(
        task_id="dbt_test",
        bash_command=f"dbt test --profiles-dir {DBT_PROJECT_DIR} --project-dir {DBT_PROJECT_DIR}"
    )

    dbt_run >> dbt_test
from pendulum import datetime

from airflow import DAG
from airflow.operators.dummy import DummyOperator

from utils.dbt_dag_parser import DbtDagParser

# We're hardcoding these values here for the purpose of the demo, but in a production environment these
# would probably come from a config file and/or environment variables!
DBT_PROJECT_DIR = "/opt/airflow/dags/dbt/dbt_fp"
DBT_GLOBAL_CLI_FLAGS = ""
DBT_TARGET = "dev"

with DAG(
    "dbt_advanced_dag_utility",
    start_date=datetime(2020, 12, 23),
    description="A dbt wrapper for Airflow using a utility class to map the dbt DAG to Airflow tasks",
    schedule_interval=None,
    catchup=False,
    doc_md=__doc__
) as dag:

    start_dummy = DummyOperator(task_id="start")
    end_dummy = DummyOperator(task_id="end")

    # The parser parses out a dbt manifest.json file and dynamically creates tasks for "dbt run" and "dbt test"
    # commands for each individual model. It groups them into task groups which we can retrieve and use in the DAG.
    dag_parser = DbtDagParser(
        dbt_global_cli_flags=DBT_GLOBAL_CLI_FLAGS,
        dbt_project_dir=DBT_PROJECT_DIR,
        dbt_profiles_dir=DBT_PROJECT_DIR,
        dbt_target=DBT_TARGET
    )
    dbt_run_group = dag_parser.get_dbt_run_group()
    dbt_test_group = dag_parser.get_dbt_test_group()

    start_dummy >> dbt_run_group >> dbt_test_group >> end_dummy
from datetime import datetime
from cosmos import DbtTaskGroup, ExecutionConfig, ProjectConfig
from cosmos.config import ProfileConfig

from airflow import DAG
from airflow.operators.empty import EmptyOperator


profile_config = ProfileConfig(
    profile_name="dbt_fp",
    target_name="dev",
    profiles_yml_filepath="/opt/airflow/dags/dbt/dbt_fp/profiles.yml",
)

with DAG(
    dag_id="my_cosmos_task_group_dag",
    start_date=datetime(2023, 1, 1),
    schedule=None,
):
    e1 = EmptyOperator(task_id="pre_dbt")

    dbt_tg = DbtTaskGroup(
        project_config=ProjectConfig(
            "/opt/airflow/dags/dbt/dbt_fp",
        ),
        profile_config=profile_config,
    )

    e2 = EmptyOperator(task_id="post_dbt")

    e1 >> dbt_tg >> e2
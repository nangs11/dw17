from airflow import DAG
from cosmos import DbtDag, ExecutionConfig, ProjectConfig
from cosmos.config import ProfileConfig, ExecutionMode 

from datetime import datetime

from utils.notification import Notification


# Define the profile configuration for dbt
profile_config = ProfileConfig(
    profile_name="dbt_fp",
    target_name="dev",
    profiles_yml_filepath="/opt/airflow/dags/dbt/dbt_fp/profiles.yml",
)

example_virtualenv = DbtDag(
    # dbt/cosmos-specific parameters
    project_config=ProjectConfig(
        "/opt/airflow/dags/dbt/dbt_fp"
    ),
    profile_config=profile_config,
    execution_config=ExecutionConfig(
        execution_mode=ExecutionMode.VIRTUALENV,
    ),
    operator_args={
        "py_system_site_packages": False,
        "py_requirements": ["dbt-postgres==1.7.2"],
        "install_deps": True,
    },
    # normal dag parameters
    dag_id="transform_dbt_cosmos_dags",
    tags=["transform", "dbt", "cosmos"],
    schedule_interval="@once",
    default_args={"owner": "kel11",
                  "start_date": datetime(2023, 12, 3),
                  "catchup": False,
                  "schedule_interval": "@once",
                  "on_failure_callback": Notification.push,
                  "on_retry_callback": Notification.push,
                  "on_success_callback": Notification.push
    }
)

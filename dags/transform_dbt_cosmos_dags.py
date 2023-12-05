from datetime import datetime
from cosmos import DbtDag, ExecutionConfig, ProjectConfig
from cosmos.config import ProfileConfig, ExecutionMode 


profile_config = ProfileConfig(
    profile_name="dbt_fp",
    target_name="dev",
    profiles_yml_filepath="/opt/airflow/dags/dbt/dbt_fp/profiles.yml",
)

example_virtualenv = DbtDag(
    # dbt/cosmos-specific parameters
    project_config=ProjectConfig(
        "/opt/airflow/dags/dbt/dbt_test"
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
    schedule_interval=None,
    start_date=datetime(2023, 1, 1),
    catchup=False,
    dag_id="example_virtualenv_test",
    default_args={"retries": 2},
)

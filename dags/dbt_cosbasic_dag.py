from datetime import datetime
from cosmos import DbtDag, ProjectConfig, ExecutionConfig, RenderConfig, LoadMode
from pathlib import Path
from cosmos.config import ProfileConfig, ProjectConfig

dbt_executable = Path("/home/airflow/.local/bin/dbt")

venv_execution_config = ExecutionConfig(
    dbt_executable_path=str(dbt_executable)
)

profile_config = ProfileConfig(
    profile_name="dbt_fp",
    target_name="dev",
    profiles_yml_filepath="/opt/airflow/dags/dbt/dbt_fp/profiles.yml"
)

config = ProjectConfig(
    dbt_project_path="/opt/airflow/dags/dbt/dbt_fp",
    models_relative_path="/opt/airflow/dags/dbt/dbt_fp/models",
    seeds_relative_path="/opt/airflow/dags/dbt/dbt_fp/data",
    snapshots_relative_path="/opt/airflow/dags/dbt/dbt_fp/snapshots"
)


simple_dag = DbtDag(
    # dbt/cosmos-specific parameters
    project_config=config,
    profile_config=profile_config,
    # render_config=RenderConfig(load_method=LoadMode.DBT_MANIFEST),
    execution_config=venv_execution_config,
    # normal dag parameters
    schedule_interval="@daily",
    start_date=datetime(2023, 1, 1),
    catchup=False,
    dag_id="simple_dag_cosbasic",
    tags=["simple"]
)
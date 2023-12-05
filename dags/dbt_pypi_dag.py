import os 
from datetime import datetime 
from pathlib import Path 

from cosmos import DbtDag, ProjectConfig
from cosmos.config import ProfileConfig



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

# [START local_example] 
basic_cosmos_dag = DbtDag( 
    # dbt/cosmos-specific parameters 
    project_config=config,
    profile_config=profile_config, 
    operator_args={ 
        "install_deps": True,  # install any necessary dependencies before running any dbt command 
        "full_refresh": True,  # used only in dbt commands that support this flag 
    }, 
    # normal dag parameters 
    schedule_interval=None, 
    start_date=datetime(2023, 1, 1), 
    catchup=False, 
    dag_id="DAGTAIII", 
    default_args={"retries": 2}
) 
# [END local_example] 
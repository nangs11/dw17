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
    'ingest_data_dags',
    default_args=default_args,
    description='DAG for ingesting data files into PostgreSQL',
    catchup=False,
    schedule_interval='@once',
    tags=['ingesting']
)

data_folder_path = 'data/'

with dag:
    # Task to test the connection
    test_conn_task = PythonOperator(
        task_id='test_connection',
        python_callable=test_conn,
        dag=dag,
    )

    # CSV task
    ingest_csv_task = PythonOperator(
        task_id='ingest_csv',
        python_callable=ingest_csv_files,
        op_kwargs={'folder_path': data_folder_path, 'table_name': 'customers'},
        dag=dag,
    )

    # JSON task
    with TaskGroup("ingest_json_tasks") as json_task_group:
        # Task to ingest JSON files for login attempts
        ingest_json_files_login_attempts_task = PythonOperator(
            task_id='ingest_json_login_attempts',
            python_callable=ingest_json_files_login_attempts,
            op_kwargs={'folder_path': data_folder_path, 'table_name': 'login_attempt_history'},
            dag=dag,
        )

        # Task to ingest JSON files for coupons
        ingest_json_files_coupons_task = PythonOperator(
            task_id='ingest_json_coupons',
            python_callable=ingest_json_files_coupons,
            op_kwargs={'folder_path': data_folder_path, 'table_name': 'coupons'},
            dag=dag,
        )

    # XLS task group
    with TaskGroup("ingest_xls_tasks") as xls_task_group:
        # Task to ingest XLS files for suppliers
        ingest_xls_files_supplier_task = PythonOperator(
            task_id='ingest_xls_supplier',
            python_callable=ingest_xls_files_supplier,
            op_kwargs={'folder_path': data_folder_path, 'table_name': 'suppliers'},  # Update table name as needed
            dag=dag,
        )

        # Task to ingest XLS files for products
        ingest_xls_files_product_task = PythonOperator(
            task_id='ingest_xls_product',
            python_callable=ingest_xls_files_product,
            op_kwargs={'folder_path': data_folder_path, 'table_name': 'product'},  # Update table name as needed
            dag=dag,
        )

        # Task to ingest XLS files for product categories
        ingest_xls_files_product_category_task = PythonOperator(
            task_id='ingest_xls_product_category',
            python_callable=ingest_xls_files_product_category,
            op_kwargs={'folder_path': data_folder_path, 'table_name': 'product_category'},  # Update table name as needed
            dag=dag,
        )


    # PARQUET task
    ingest_parquet_task = PythonOperator(
        task_id='ingest_parquet',
        python_callable=ingest_parquet_file,
        op_kwargs={'file_path': f'{data_folder_path}/order.parquet', 'table_name': 'orders'},
        dag=dag,
    )

    # AVRO task
    ingest_avro_task = PythonOperator(
        task_id='ingest_avro',
        python_callable=ingest_avro_file,
        op_kwargs={'file_path': f'{data_folder_path}/order_item.avro', 'table_name': 'order_items'},
        dag=dag,
    )

    # End of DAG
    end = DummyOperator(
        task_id='end',
        trigger_rule='none_failed' # If none failed then end
    )


# Setting dependencies
test_conn_task >> ingest_csv_task >> end
test_conn_task >> xls_task_group >> end
test_conn_task >> ingest_parquet_task >> end
test_conn_task >> ingest_avro_task >> end
test_conn_task >> json_task_group >> end
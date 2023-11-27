# data_ingestion_dag.py
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta
import pandas as pd
from sqlalchemy import create_engine
import os
import glob
from avro.datafile import DataFileReader
from avro.io import DatumReader
import pyarrow.parquet as pq

# SQLAlchemy connection function
def get_sqlalchemy_conn():
    engine = create_engine('postgresql://user:password@dibimbing-dataeng-dw-postgres:5432/data_warehouse')
    return engine.connect()

def test_conn():
    conn = get_sqlalchemy_conn()
    result = conn.execute("SELECT version();")
    record = result.fetchone()
    print(f"You are connected to - {record}")
    conn.close()

# CSV ingestion
def ingest_csv_files(folder_path, table_name):
    conn = get_sqlalchemy_conn()
    for file_path in glob.glob(f"{folder_path}/customer_*.csv"):
        df = pd.read_csv(file_path, index_col=0)
        df.to_sql(table_name, conn, if_exists='append', index=False)
    conn.close()

# JSON ingestion
def ingest_json_files_login_attempts(folder_path, table_name):
    conn = get_sqlalchemy_conn()
    for file_path in glob.glob(f"{folder_path}/login_attempts_*.json"):
        df = pd.read_json(file_path)
        df.to_sql(table_name, conn, if_exists='append', index=False)
    conn.close()

def ingest_json_files_coupons(folder_path, table_name):
    conn = get_sqlalchemy_conn()
    for file_path in glob.glob(f"{folder_path}/coupons.json"):
        df = pd.read_json(file_path)
        df.to_sql(table_name, conn, if_exists='append', index=False)
    conn.close()

# XLS ingestion
def ingest_xls_files_supplier(folder_path, table_name):
    conn = get_sqlalchemy_conn()
    for file_path in glob.glob(f"{folder_path}/supplier.xls"):
        df = pd.read_excel(file_path, index_col=0)
        df.to_sql(table_name, conn, if_exists='append', index=False)
    conn.close()

def ingest_xls_files_product_category(folder_path, table_name):
    conn = get_sqlalchemy_conn()
    for file_path in glob.glob(f"{folder_path}/product_category.xls"):
        df = pd.read_excel(file_path, index_col=0)
        df.to_sql(table_name, conn, if_exists='append', index=False)
    conn.close()

def ingest_xls_files_product(folder_path, table_name):
    conn = get_sqlalchemy_conn()
    for file_path in glob.glob(f"{folder_path}/product.xls"):
        df = pd.read_excel(file_path, index_col=0)
        df.to_sql(table_name, conn, if_exists='append', index=False)
    conn.close()

# PARQUET ingestion
def ingest_parquet_file(file_path, table_name):
    conn = get_sqlalchemy_conn()
    df = pd.read_parquet(file_path)
    df.to_sql(table_name, conn, if_exists='append', index=False)
    conn.close()

# AVRO ingestion
def ingest_avro_file(file_path, table_name):
    conn = get_sqlalchemy_conn()

    # Create an empty list to store the dictionaries
    data_list = []

    # Read data and append to the list
    with DataFileReader(open(file_path, "rb"), DatumReader()) as reader:
        for user in reader:
            data_list.append(user)

    # Convert the list of dictionaries to a DataFrame
    df = pd.DataFrame(data_list)
    df.to_sql(table_name, conn, if_exists='append', index=False)
    conn.close()

# DAG definition
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2023, 11, 26),
    'email_on_failure': False,
    'email_on_retry': False,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'data_ingestion',
    default_args=default_args,
    description='DAG for ingesting data files into PostgreSQL',
    schedule_interval=timedelta(days=1),
)

data_folder_path = 'data/'

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
ingest_json_files_login_attempts_task = PythonOperator(
    task_id='ingest_json_login_attempts',
    python_callable=ingest_json_files_login_attempts,
    op_kwargs={'folder_path': data_folder_path, 'table_name': 'login_attempt_history'},
    dag=dag,
)

ingest_json_files_coupons_task = PythonOperator(
    task_id='ingest_json_coupons',
    python_callable=ingest_json_files_coupons,
    op_kwargs={'folder_path': data_folder_path, 'table_name': 'coupons'},
    dag=dag,
)


# XLS task
ingest_xls_files_supplier_task = PythonOperator(
    task_id='ingest_xls_supplier',
    python_callable=ingest_xls_files_supplier,
    op_kwargs={'folder_path': data_folder_path, 'table_name': 'suppliers'},  # Update table name as needed
    dag=dag,
)

ingest_xls_files_product_task = PythonOperator(
    task_id='ingest_xls_product',
    python_callable=ingest_xls_files_product,
    op_kwargs={'folder_path': data_folder_path, 'table_name': 'product'},  # Update table name as needed
    dag=dag,
)

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

# Setting dependencies
test_conn_task >> [ingest_csv_task, ingest_json_files_login_attempts_task, ingest_json_files_coupons_task, ingest_xls_files_supplier_task, ingest_xls_files_product_task, ingest_xls_files_product_category_task, ingest_parquet_task, ingest_avro_task]
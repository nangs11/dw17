import pandas as pd
from sqlalchemy import create_engine
import os
import glob
from avro.datafile import DataFileReader
from avro.io import DatumReader
import pyarrow.parquet as pq
from sqlalchemy import create_engine

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


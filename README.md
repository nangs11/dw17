# Final Project Data Engineering - Dibimbing - Online Retail ELT

[![Presentasi-Kel11-FP-Dibimbing.jpg](https://i.postimg.cc/vZKwLfT7/Presentasi-Kel11-FP-Dibimbing.jpg)](https://postimg.cc/w1Dr9th3)

## Problem Statement
Congratulations on your first role as Data Engineer!. You are just hired at a US online retail company that sells general customer products directly to customers from multiple suppliers around the world. Your challenge is to build-up the data infrastructure using generated data crafted to mirror real-world data from leading tech companies. 
* ETL/ELT Job Creation using Airflow
* Data Modeling in Postgres
* Dashboard Creation with Data Visualization
* Craft a Presentation Based on Your Work

## Overview
### Full Pipeline
[![data-diagram.jpg](https://i.postimg.cc/HL2Y0VGT/data-diagram.jpg)](https://postimg.cc/zLyY8DXQ)

<details>
    
### Ingest Data DAG
[![Screen-Shot-2023-12-06-at-16-32-21.png](https://i.postimg.cc/RhnxY0Y6/Screen-Shot-2023-12-06-at-16-32-21.png)](https://postimg.cc/SYqP2mkk)
### Data Transformation with DBT - Bash
[![Screen-Shot-2023-12-06-at-16-36-06.png](https://i.postimg.cc/MHbJwLWq/Screen-Shot-2023-12-06-at-16-36-06.png)](https://postimg.cc/FkRBgDdB)
### Data Transformation with DBT - Cosmos by Astronomer
[![Screen-Shot-2023-12-06-at-16-37-23.png](https://i.postimg.cc/vTX23wYg/Screen-Shot-2023-12-06-at-16-37-23.png)](https://postimg.cc/LgYV87Z2)

</details>

### Data Lineage
[![dbt-dag.png](https://i.postimg.cc/Ss1zrZ0Q/dbt-dag.png)](https://postimg.cc/DJsZfPyR)

### Technologies and Tools
* Containerization - Docker, Docker Compose
* Workflow Orchestration - Airflow
* Data Transformation - dbt
* Data Warehouse - Postgresql
* Data Visualization - Metabase
* Metrics Monitoring - Grafana Prometheus
* Language - Python

### Project structure

```
.
├── README.md
├── dags                                  # for airflow dags
│   ├── dbt                               # dbt models used for data warehouse transformations
│   ├── ingest_data_dags.py
│   ├── test_dags.py
│   ├── transform_dbt_bash_dags.py
│   ├── transform_dbt_cosmos_dags.py     
│   └── utils                             # utils for main files
├── data                                  # data source, generated data
├── docker                                # for containerizations
├── final_deliverables                    # ppt, docs, png, etc here
├── grafana 
├── makefile
├── prometheus
├── requirements.txt                      # library for python
└── scripts
└── .env                                  # secret keys, environment variables
```

## Dashboard with Metabase
* Customer Lifetime Value (CLV), Identify high-value customers and understand spending patterns. This helps in tailoring marketing strategies and improving customer retention.
* Product Performance Analysis, Highlight top-performing products and categories. Use this data to manage inventory effectively and plan product development strategies.
* Burning Rate, It measures the rate at which a company is spending its capital.

[![Clean-Shot-2023-12-06-at-14-13-54.png](https://i.postimg.cc/dVmMTpdC/Clean-Shot-2023-12-06-at-14-13-54.png)](https://postimg.cc/4Yd2D8W4)

## Setup
- Customize the .env files, change the TELEGRAM_TOKEN and TELEGRAM_CHAT_ID
- In order to spin up the containers, first you have to build all the Docker images needed using 
    ```sh
    make build
    ```
- Once all the images have been build up, you can try to spin up the containers using
    ```sh
    make spinup
    ```
- Once all the containers ready, you can try to
    - Access the Airflow on port `8081`
    - Access the Metabase on port `3001`, for the username and password, you can try to access the [.env](/.env) file
    - Access the Grafana Dashboards on port `3000`, for the username and password, you can try `admin` and `password`
    - Access the Prometheus on port `9090`
    - Since we have 2 Postgres containers, you can use `dataeng-warehouse-postgres` container as your data warehouse and ignore the `dataeng-ops-postgres` container since it is only being used for the opetrational purposes
- Run the DAGs, from Ingest Data DAG, and then Data Transformation with DBT
- Customize your visualization on Metabase
- Done!

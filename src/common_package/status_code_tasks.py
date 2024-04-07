import airflow # type: ignore
from airflow import DAG # type: ignore
from airflow.operators.bash import BashOperator # type: ignore
from airflow.operators.python import PythonOperator # type: ignore
from airflow.providers.postgres.operators.postgres import PostgresOperator # type: ignore
from common_package.db_conn import get_db_connection
import os
import logging


extract_unique_status_code_query = '''
    DROP TABLE IF EXISTS staging_status_code;
    
    CREATE TABLE staging_status_code (
        status_code_id SERIAL PRIMARY KEY,
        status_code INT
    );
    
    INSERT INTO staging_status_code (status_code)
    SELECT DISTINCT CAST(status_code AS INT) from staging_log_data;
'''
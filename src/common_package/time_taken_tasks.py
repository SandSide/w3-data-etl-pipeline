import http
import airflow # type: ignore
from airflow import DAG # type: ignore
from airflow.operators.bash import BashOperator # type: ignore
from airflow.operators.python import PythonOperator # type: ignore
from airflow.providers.postgres.operators.postgres import PostgresOperator # type: ignore
from common_package.db_conn import get_db_connection
import os
import logging

extract_unique_time_taken_query = '''
    DROP TABLE IF EXISTS staging_time_taken;
    
    CREATE TABLE staging_time_taken(
        time_taken_id SERIAL PRIMARY KEY,
        time_taken int  
    );
    
    INSERT INTO staging_time_taken (time_taken)
    SELECT DISTINCT time_taken from staging_log_data;
'''
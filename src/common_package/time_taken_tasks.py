import http
import airflow # type: ignore
from airflow import DAG # type: ignore
from airflow.operators.bash import BashOperator # type: ignore
from airflow.operators.python import PythonOperator # type: ignore
from airflow.providers.postgres.operators.postgres import PostgresOperator # type: ignore
from common_package.db_conn import get_db_connection
import os
import logging


time_ranges = [
        ('1min+', 100000, None),
        ('30s+', 30000, 99999),
        ('10s-30s', 10000, 30000),
        ('5s-10s', 5000, 10000),
        ('2s-5s', 2000, 5000),
        ('1s-2s', 1000, 2000),
        ('500ms-1s', 500, 999),
        ('200ms-500ms', 200, 499),
        ('100ms-200ms', 100, 199),
        ('<100ms', 0, 99)
    ]


extract_unique_time_taken_query = '''
    DROP TABLE IF EXISTS staging_time_taken;
    
    CREATE TABLE staging_time_taken(
        time_taken_id SERIAL PRIMARY KEY,
        time_taken int  
    );
    
    INSERT INTO staging_time_taken (time_taken)
    SELECT DISTINCT time_taken from staging_log_data;
'''

add_category_column_query ='''
    ALTER TABLE staging_time_taken
    ADD COLUMN IF NOT EXISTS time_category VARCHAR,
    ADD COLUMN IF NOT EXISTS min_category_time INT,
    ADD COLUMN IF NOT EXISTS max_category_time INT
'''

update_time_category_details_query = '''
    UPDATE staging_time_taken
    SET 
        time_category = %s, 
        min_category_time = %s, 
        max_category_time = %s
    WHERE  
        time_taken_id = %s;
'''

def categorize_time_taken():
    
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        
        # Add columns to file table
        cursor.execute(add_category_column_query)
        
        sql_query = 'SELECT time_taken_id, time_taken from staging_time_taken;';
        cursor.execute(sql_query)
        
        result = cursor.fetchall()
        
        for row in result:
            time_taken_id, time_taken = row
            
            values = determine_category(time_taken)
            
            cursor.execute(update_time_category_details_query, (*values, time_taken_id))
            
        conn.commit()
            
    except Exception as e:
        conn.rollback()
        logging.exception(f'Error: {e}')
        raise
        
    finally:
        cursor.close()
        conn.close()
        

def determine_category(time_taken):
    
    for r in time_ranges:
        
        if r[2]:
            if time_taken >= r[1] and time_taken <= r[2]:
                return r
        else:
            if time_taken >= r[1]:
                return r
            
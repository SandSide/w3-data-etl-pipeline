import http
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

def determine_status_code_details():
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        
        # Add columns to file table
        sql_query = '''
            ALTER TABLE staging_status_code
            ADD COLUMN IF NOT EXISTS code_phrase VARCHAR,
            ADD COLUMN IF NOT EXISTS code_type VARCHAR,
            ADD COLUMN IF NOT EXISTS code_description VARCHAR
            '''
        cursor.execute(sql_query)
        
        cursor.execute('SELECT status_code_id, status_code FROM staging_status_code;')
        result = cursor.fetchall()
        
        for row in result:
            code_id, status_code = row
            
            values = get_status_code_details(status_code)
            
            if values:

                # Insert file details
                cursor.execute('''
                        UPDATE staging_status_code
                        SET code_phrase = %s, code_type = %s, code_description = %s
                        WHERE  status_code_id = %s;
                        ''', (*values, code_id))
            
        conn.commit()
            
    except Exception as e:
        conn.rollback()
        logging.exception(f'Error: {e}')
        raise
    
    finally:
        cursor.close()
        conn.close()
      
        
def get_status_code_details(status_code):
    
    try:
        status_info = http.HTTPStatus(status_code)
        status_type = get_status_code_type(status_code)
        
        return (status_info.phrase, status_type, status_info.description)

    except ValueError:
        return None
    
def get_status_code_type(status_code):
    
    if status_code > 500:
        return 'server error'
    elif status_code > 400:
        return 'client error'
    elif status_code > 300:
        return 'redirection'
    elif status_code > 200:
        return 'success'
    elif status_code > 100:
        return 'informational'
    
    
build_dim_status_code_table_query = '''
    DROP TABLE IF EXISTS 
        dim_status_code;
    
    CREATE TABLE 
        dim_status_code 
    AS
        SELECT * FROM staging_status_code;
'''
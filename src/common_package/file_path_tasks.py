import airflow # type: ignore
from airflow import DAG # type: ignore
from airflow.operators.bash import BashOperator # type: ignore
from airflow.operators.python import PythonOperator # type: ignore
from airflow.providers.postgres.operators.postgres import PostgresOperator # type: ignore
from common_package.db_conn import get_db_connection
import os
import re
import logging


def extract_file_details():
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        
        # Add columns to file table
        sql_query = '''
            ALTER TABLE staging_file
            ADD COLUMN IF NOT EXISTS file_path VARCHAR,
            ADD COLUMN IF NOT EXISTS file_name VARCHAR,
            ADD COLUMN IF NOT EXISTS file_extension VARCHAR,
            ADD COLUMN IF NOT EXISTS file_directory VARCHAR;
            '''
        cursor.execute(sql_query)
        
        cursor.execute('SELECT file_id, raw_file_path FROM staging_file;')
        result = cursor.fetchall()
        
        for row in result:
            file_id, file_path = row
            
            values = process_file_path(file_path)
            
            # Insert file details
            cursor.execute('''
                    UPDATE staging_file
                    SET file_path = %s, file_name = %s, file_extension = %s, file_directory = %s
                    WHERE  file_id = %s;
                    ''', (*values, file_id))
            
        conn.commit()
            
    except Exception as e:
        conn.rollback()
        logging.exception(f'Error: {e}')
        raise
    
    finally:
        cursor.close()
        conn.close()
        

def process_file_path(raw_file_path):

    file_directory, file_name = os.path.split(raw_file_path)
    
    # Remove anything after +++
    if '+++' in file_name:
        i = file_name.find('+++')
        file_name = file_name[:i]
       
    
    # Remove anything after " 
    if '"' in file_name:
        i = file_name.find('"')
        file_name = file_name[:i]
    
    
    # Remove anything after ?
    if '?' in file_name:
        i = file_name.find('?')
        file_name = file_name[:i]
    
    
    pattern = r'[^a-zA-Z0-9./\-\'+_]'
    file_name = re.sub(pattern, '', file_name)

    a, file_extension = os.path.splitext(file_name)    
    
    if '+' in file_extension:
        file_extension = ''
    

    if file_directory.endswith('/'):
        file_path = f'{file_directory}{file_name}' 
    else:
        file_path = f'{file_directory}/{file_name}' 
          
    
    # out = (raw_file_path, file_path, file_name, file_extension, file_directory)   
        
    return (file_path, file_name, file_extension, file_directory) 

extract_unique_file_path_query = '''
            DROP TABLE IF EXISTS staging_file;
            
            CREATE TABLE staging_file (
                file_id SERIAL PRIMARY KEY,
                raw_file_path VARCHAR
            );
            
            INSERT INTO staging_file (raw_file_path)
            SELECT DISTINCT raw_file_path from staging_log_data;
        '''
        
build_dim_file_query = '''
            DROP TABLE IF EXISTS dim_file;
            
            CREATE TABLE dim_file AS
            SELECT * FROM staging_file;
        '''
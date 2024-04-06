
import airflow # type: ignore
from airflow import DAG # type: ignore
from airflow.operators.bash import BashOperator # type: ignore
from airflow.operators.python import PythonOperator # type: ignore
from airflow.providers.postgres.operators.postgres import PostgresOperator # type: ignore
from common_package.db_conn import get_db_connection
import os
import logging

# Global variables
BASE_DIR = '/opt/airflow/data'
RAW_DATA = BASE_DIR + '/W3SVC1/'
STAGING = BASE_DIR + '/staging/'
STAR_SCHEMA = BASE_DIR + '/star-schema/'


def process_raw_data():
   
    arr = os.listdir(RAW_DATA)
   
    if not arr:
        print('Raw data folder is empty')

    logging.debug('Raw file list:' + ','.join(str(element) for element in arr)) 
    
    clear_files()
    
    for f in arr:
        process_log_file(f)
       

def process_log_file(filename):
    
    logging.debug('Processing ' + filename)
    
    type = filename[-3:len(filename)]
    
    if (type == 'log'):
        
        in_file = open(RAW_DATA + filename, 'r')
        out_file = open(STAGING + 'merged-data.txt', 'a')
        
        lines = in_file.readlines()
        
        for line in lines:
            
            if (line[0] != '#'):
            
                result = process_log_line(line)

                if result:
                    if not result.endswith('\n'):
                        result += '\n'
                    out_file.write(result)
                
        in_file.close()
        out_file.close()
                 
  
def process_log_line(line):    
    
    split = line.split(' ')
    response_time = ''
    status_code = ''
    cs_bytes = '-'
    sc_bytes = '-'
    
    if (len(split) == 14):
        status_code = split[-4]
        response_time = split[13]
        
    elif (len(split) == 18):  
        status_code = split[-6]
        response_time = split[16]
        sc_bytes = split[-3]
        cs_bytes = split[-2]

    else:
        return 
    
    browser = split[9].replace(',','')
    file_path = split[4].replace(',','')

    out = f'{split[0]},{split[1]},{split[3]},{file_path},{browser},{split[8]},{status_code},{sc_bytes},{cs_bytes},{response_time}'
    
    return out
            

def clear_files():
    out_file_long = open(STAGING + 'merged-data.txt', 'w')
    
    
def insert_staging_log_data():
    
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        
        with open(STAGING + 'merged-data.txt', 'r') as file:
            for line in file:
                values = line.strip().split(',')
                
                if values[-3] == '-' or values[-2] == '-':
                    values[-3] = None
                    values[-2] = None
 
                cursor.execute('INSERT INTO staging_log_data (date, time, http_method, raw_file_path, browser_string, ip, status_code, sc_bytes, cs_bytes, response_time) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)', values)
                
        conn.commit()
            
    except Exception as e:
        conn.rollback()
        logging.exception(f'Error: {e}')
        raise
        
    finally:
        cursor.close()
        conn.close()
        
def define_process_raw_data_tasks(dag):
    
    extract_raw_data_task = PythonOperator(
        task_id = 'extract_log_data',
        python_callable = process_raw_data, 
        dag = dag
    )
    
    
    create_staging_log_data_table_task = PostgresOperator(
        task_id = 'create_staging_log_data_table',
        sql = f"""
        DROP TABLE IF EXISTS staging_log_data;
        
        CREATE TABLE staging_log_data(
            log_id SERIAL PRIMARY KEY,
            date VARCHAR,
            time VARCHAR,
            http_method VARCHAR,
            raw_file_path VARCHAR,
            browser_string VARCHAR,
            ip VARCHAR,
            status_code VARCHAR,
            sc_bytes INT,
            cs_bytes INT,
            response_time int
        );
        """,
        dag = dag
    )
    
    insert_staging_log_data_task = PythonOperator(
        task_id = 'insert_log_data',
        python_callable = insert_staging_log_data,
        dag = dag
    )
    
    return extract_raw_data_task, create_staging_log_data_table_task, insert_staging_log_data_task
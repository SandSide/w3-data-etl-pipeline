import airflow # type: ignore
from airflow import DAG # type: ignore
from airflow.operators.bash import BashOperator # type: ignore
from airflow.operators.python import PythonOperator # type: ignore
from airflow.providers.postgres.operators.postgres import PostgresOperator # type: ignore
from db_conn import get_db_connection
import os
import logging
from user_agents import parse

def determine_if_bot():
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        
        
        sql_query = '''
            ALTER TABLE staging_log_data
            ADD COLUMN IF NOT EXISTS is_bot BOOLEAN
            '''
        cursor.execute(sql_query)
        

        cursor.execute('SELECT log_id, raw_file_path, browser_string FROM staging_log_data;')
        result = cursor.fetchall()
        
        for row in result:
            
            log_id, file_path, browser = row
            
            is_bot_result = is_bot(browser, file_path)
            
            cursor.execute('''
                UPDATE staging_log_data
                SET is_bot = %s
                WHERE log_id = %s;
                ''', (is_bot_result, log_id))
            
        conn.commit()
            
    except Exception as e:
        conn.rollback()
        logging.exception(f'Error: {e}')
        raise
        
    finally:
        cursor.close()
        conn.close()
        

def is_bot(browser, file_path):
    parsed_ua = parse(browser)
    return parsed_ua.is_bot or file_path == '/robots.txt'


def define_bot_tasks(dag):
    determine_if_bot_task = PythonOperator(
        task_id = 'determine_if_bot',
        python_callable = determine_if_bot,
        dag = dag
    )
    
    return determine_if_bot_task
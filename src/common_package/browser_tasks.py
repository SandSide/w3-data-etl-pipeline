import airflow # type: ignore
from airflow import DAG # type: ignore
from airflow.operators.bash import BashOperator # type: ignore
from airflow.operators.python import PythonOperator # type: ignore
from airflow.providers.postgres.operators.postgres import PostgresOperator # type: ignore
from common_package.db_conn import get_db_connection
from user_agents import parse

def determine_browser():
    conn = get_db_connection()
    cursor = conn.cursor()
    
    # Add browser column
    sql_query = '''
        ALTER TABLE staging_log_data
        ADD COLUMN IF NOT EXISTS browser VARCHAR;
    '''
    cursor.execute(sql_query)
    
    
    cursor.execute('SELECT log_id, browser_string FROM staging_log_data;')
    result = cursor.fetchall()
    
    for log in result:
        
        log_id, browser = log
        
        # Get details from browser string
        parsed_ua = parse(browser)
        browser = parsed_ua.browser.family

        # Update table
        cursor.execute('''
            UPDATE staging_log_data 
            SET browser = %s
            WHERE log_id = %s;
            ''', (browser, log_id))
        
    conn.commit()
    cursor.close()
    conn.close() 


extract_unique_browser_query = '''
            DROP TABLE IF EXISTS staging_browser;
            
            CREATE TABLE staging_browser (
                browser_id SERIAL PRIMARY KEY,
                browser VARCHAR
            );
            
            INSERT INTO staging_browser (browser)
            SELECT DISTINCT browser from staging_log_data;
        '''

build_dim_browser_table_query = '''
            DROP TABLE IF EXISTS dim_browser;
            
            CREATE TABLE dim_browser AS
            SELECT * FROM staging_browser;
        '''
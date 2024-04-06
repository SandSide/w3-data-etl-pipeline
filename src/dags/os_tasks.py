import airflow # type: ignore
from airflow import DAG # type: ignore
from airflow.operators.bash import BashOperator # type: ignore
from airflow.operators.python import PythonOperator # type: ignore
from airflow.providers.postgres.operators.postgres import PostgresOperator # type: ignore
from db_conn import get_db_connection
from user_agents import parse

def determine_os():
    conn = get_db_connection()
    cursor = conn.cursor()
    
    # Add os column
    sql_query = '''
        ALTER TABLE staging_log_data
        ADD COLUMN IF NOT EXISTS os VARCHAR;
    '''
    cursor.execute(sql_query)
    
    
    cursor.execute('SELECT log_id, browser_string FROM staging_log_data;')
    result = cursor.fetchall()
    
    for log in result:
        
        log_id, browser = log
        
        # Get details from browser string
        parsed_ua = parse(browser)
        os = parsed_ua.os.family

        # Update table
        cursor.execute('''
            UPDATE staging_log_data 
            SET os = %s
            WHERE log_id = %s;
            ''', (os, log_id))
        
    conn.commit()
    cursor.close()
    conn.close()    
    

def define_os_tasks(dag):
    determine_os_task = PythonOperator(
        task_id = 'determine_os',
        python_callable = determine_os,
        dag = dag
    )
    
    extract_unique_os_task = PostgresOperator(
        task_id = 'extract_unique_os',
        sql = 
        '''
            DROP TABLE IF EXISTS staging_os;
            
            CREATE TABLE staging_os (
                os_id SERIAL PRIMARY KEY,
                os VARCHAR
            );
            
            INSERT INTO staging_os (os)
            SELECT DISTINCT os from staging_log_data;
        ''',
        dag = dag
    )
    
    build_dim_os_table_task = PostgresOperator(
        task_id = 'build_dim_os_table',
        sql = 
        '''
            DROP TABLE IF EXISTS dim_os;
            
            CREATE TABLE dim_os AS
            SELECT * FROM staging_os;
        ''',
        dag = dag
    )
    
    return determine_os_task, extract_unique_os_task, build_dim_os_table_task
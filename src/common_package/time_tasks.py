import airflow # type: ignore
from airflow import DAG # type: ignore
from airflow.operators.bash import BashOperator # type: ignore
from airflow.operators.python import PythonOperator # type: ignore
from airflow.providers.postgres.operators.postgres import PostgresOperator # type: ignore
from common_package.db_conn import get_db_connection


def define_time_tasks(dag):
    extract_unique_time_task = PostgresOperator(
        task_id = 'extract_unique_time',
        sql = 
        '''
            DROP TABLE IF EXISTS staging_time;
            
            CREATE TABLE staging_time (
                time_id SERIAL PRIMARY KEY,
                time TIME
            );
            
            INSERT INTO staging_time (time)
            SELECT DISTINCT CAST(time AS TIME) from staging_log_data;
        ''',
        dag = dag
    )
        
    determine_time_details_task = PostgresOperator(
        task_id = 'determine_time_details',
        sql = 
        '''
            ALTER TABLE staging_time
            ADD COLUMN IF NOT EXISTS hour INT,
            ADD COLUMN IF NOT EXISTS minute INT,
            ADD COLUMN IF NOT EXISTS second INT;
            
            UPDATE staging_time
            SET hour = EXTRACT(HOUR FROM time),
                minute = EXTRACT(MINUTE FROM time),
                second = EXTRACT(SECOND FROM time);
        ''',
        dag = dag
    )
    
    
    build_dim_time_table_task = PostgresOperator(
        task_id = 'build_dim_time_table',
        sql = 
        '''
            DROP TABLE IF EXISTS dim_time;
            
            CREATE TABLE dim_time AS
            SELECT * FROM staging_time;
        ''',
        dag = dag
    )
    
    return extract_unique_time_task, determine_time_details_task, build_dim_time_table_task
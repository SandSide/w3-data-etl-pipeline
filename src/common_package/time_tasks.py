import airflow # type: ignore
from airflow import DAG # type: ignore
from airflow.operators.bash import BashOperator # type: ignore
from airflow.operators.python import PythonOperator # type: ignore
from airflow.providers.postgres.operators.postgres import PostgresOperator # type: ignore
from common_package.db_conn import get_db_connection


extract_unique_time_task_query = '''
            DROP TABLE IF EXISTS staging_time;
            
            CREATE TABLE staging_time (
                time_id SERIAL PRIMARY KEY,
                time TIME
            );
            
            INSERT INTO staging_time (time)
            SELECT DISTINCT CAST(time AS TIME) from staging_log_data;
        '''
        
determine_time_details_query = '''
            ALTER TABLE staging_time
            ADD COLUMN IF NOT EXISTS hour INT,
            ADD COLUMN IF NOT EXISTS minute INT,
            ADD COLUMN IF NOT EXISTS second INT;
            
            UPDATE staging_time
            SET hour = EXTRACT(HOUR FROM time),
                minute = EXTRACT(MINUTE FROM time),
                second = EXTRACT(SECOND FROM time);
        '''
build_dim_time_table_query = '''
            DROP TABLE IF EXISTS dim_time;
            
            CREATE TABLE dim_time AS
            SELECT * FROM staging_time;
        '''
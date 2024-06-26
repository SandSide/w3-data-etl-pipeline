import airflow # type: ignore
from airflow import DAG # type: ignore
from airflow.operators.bash import BashOperator # type: ignore
from airflow.operators.python import PythonOperator # type: ignore
from airflow.providers.postgres.operators.postgres import PostgresOperator # type: ignore
from common_package.db_conn import get_db_connection
from user_agents import parse
import logging

add_device_column_query = '''
    ALTER TABLE staging_log_data
    ADD COLUMN IF NOT EXISTS device_type VARCHAR;
'''

update_with_device_details_query = '''
    UPDATE staging_log_data
    SET 
        device_type = %s
    WHERE  
        log_id = %s;
'''


def determine_device_details():
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        
        cursor.execute(add_device_column_query)
        
        sql_query = 'SELECT log_id, browser_string FROM staging_log_data'
        cursor.execute(sql_query)
        result = cursor.fetchall()
        
        for row in result:
            
            log_id, browser_string = row
            
            values = determine_details(browser_string)

            cursor.execute(update_with_device_details_query, (values, log_id))              
    
        conn.commit()
               
    except Exception as e:
        conn.rollback()
        logging.exception(f'Error: {e}')
        raise
        
    finally:
        cursor.close()
        conn.close()

def determine_details(browser_string):
    
    parsed_ua = parse(browser_string)
    
    # device_family = parsed_ua.device.family or 'Unknown'
    # device_model = parsed_ua.device.model or 'Unknown'
    device_type = determine_device_type(parsed_ua) or 'Unknown'

    # return device_family, device_model, device_type
    
    return device_type


def determine_device_type(ua):
    
    if ua.is_mobile:
        return 'Mobile'
    elif ua.is_tablet:
        return 'Tablet'
    elif ua.is_pc:
        return 'PC'
    
extract_unique_device_query = '''
    DROP TABLE IF EXISTS staging_device;
    
    CREATE TABLE staging_device (
        device_id SERIAL PRIMARY KEY,
        device_type VARCHAR
    );
    
    INSERT INTO staging_device (device_type)
    SELECT DISTINCT device_type from staging_log_data;
'''

build_dim_device_query =  '''
    DROP TABLE IF EXISTS dim_device;
    
    CREATE TABLE dim_device AS
    SELECT * FROM staging_device;
'''  
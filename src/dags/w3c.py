import airflow # type: ignore
from airflow import DAG # type: ignore
from airflow.operators.bash import BashOperator # type: ignore
from airflow.operators.python import PythonOperator # type: ignore
from airflow.providers.postgres.operators.postgres import PostgresOperator # type: ignore

import os
import datetime as dt
from datetime import datetime
import re

# DODGY CODE
import subprocess
subprocess.call(['pip', 'install', 'user-agents'])
from user_agents import parse


import logging
import json
import requests # type: ignore


from db_conn import get_db_connection
from process_raw_data import define_process_raw_data_tasks


def determine_date_details():
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        
        cursor.execute('SELECT date FROM staging_date;')
        dates = cursor.fetchall()
        
        dates = [x[0] for x in dates]
        
        cursor.execute('''
            ALTER TABLE staging_date
            ADD COLUMN IF NOT EXISTS year INT,
            ADD COLUMN IF NOT EXISTS month INT,
            ADD COLUMN IF NOT EXISTS day INT,
            ADD COLUMN IF NOT EXISTS week_day VARCHAR,
            ADD COLUMN IF NOT EXISTS quarter int;
        ''')
        
        for date in dates:
            result = extract_date_details(date)
            
            cursor.execute('''
                UPDATE staging_date 
                SET year  = %s, month = %s, day = %s, week_day = %s, quarter = %s
                WHERE date = %s;
                ''', (*result, date))

        conn.commit()
        
    except Exception as e:
        conn.rollback()
        logging.exception(f'Error: {e}')
        raise
        
    finally:
        cursor.close()
        conn.close()
        
        
def extract_date_details(date):
        
    logging.debug('Extracting date details: ' + date)
        
    DAYS = ['Monday','Tuesday','Wednesday','Thursday','Friday','Saturday','Sunday']
    
    try:
        date = datetime.strptime(date,'%Y-%m-%d').date()
        weekday = DAYS[date.weekday()]
        
        quarter = 1
        
        if date.month >= 10:
            quarter = 4
        elif date.month >= 7:
            quarter = 3
        elif date.month >= 4:
            quarter = 2
        else:
            quarter = 1
        
        #out = str(date) + ',' + str(date.year) + ',' + str(date.month) + ',' + str(date.day) + ',' + weekday + '\n'  
        return (date.year, date.month, date.day, weekday, quarter)

    except:
        logging.error('Error with extracting date details ' + date)
        

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


def determine_ip_location():
    try: 
        conn = get_db_connection()
        cursor = conn.cursor()
        
        sql_query = '''
            ALTER TABLE staging_ip
            ADD COLUMN IF NOT EXISTS country_code VARCHAR,
            ADD COLUMN IF NOT EXISTS country_name VARCHAR,
            ADD COLUMN IF NOT EXISTS latitude FLOAT,
            ADD COLUMN IF NOT EXISTS longitude FLOAT;
        '''
        cursor.execute(sql_query)
        
        
        sql_query = """
            SELECT ip
            FROM staging_ip
            WHERE country_code IS NULL OR country_name IS NULL OR latitude IS NULL OR longitude IS NULL;
        """
        cursor.execute(sql_query)
        ips = cursor.fetchall()

        ips = [x[0] for x in ips]
        
        for ip in ips:
            
            logging.debug(f"Finding location IP: {ip}")
            result = get_ip_location(ip)
            
            if result is not None and result[0] != 'Not found':
            
                cursor.execute('''
                    UPDATE staging_ip
                    SET country_code = %s, country_name = %s, latitude = %s, longitude = %s
                    WHERE ip = %s;
                    ''', (*result, ip))
            else:
                logging.debug(f"Location information not found for IP: {ip}")

        conn.commit()
        
    except Exception as e:
        conn.rollback()
        logging.exception(f'Error: {e}')
        raise
        
    finally:
        cursor.close()
        conn.close()

        
        
def get_ip_location(ip):
    
    # URL to send the request to
    request_url = 'https://geolocation-db.com/jsonp/' + ip

    # Send request and decode the result
    try:
        response = requests.get(request_url)
        result = response.content.decode()
    except:
        logging.exception('error response ' + result)
        return
        
        
    try:
        # Clean the returned string so it just contains the dictionary data for the IP address
        result = result.split('(')[1].strip(')')
        
        # Convert this data into a dictionary
        result  = json.loads(result)
        
        return (result['country_code'], result['country_name'], result['latitude'], result['longitude'])
    
    except:
        logging.exception('error getting location')


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
        

with DAG(
    dag_id = 'Process_W3_Data',                          
    schedule_interval = '@weekly',                                     
    start_date = dt.datetime(2023, 2, 24), 
    catchup = False,
) as dag:


    extract_raw_data_task, create_staging_log_data_table_task, insert_staging_log_data_task = define_process_raw_data_tasks(dag)
    
    
    ###
    
    determine_if_bot_task = PythonOperator(
        task_id = 'determine_if_bot',
        python_callable = determine_if_bot,
    )


    ###### IP TASKS ######
    extract_unique_ip_task = PostgresOperator(
        task_id = 'extract_unique_ip',
        sql = 
        ''' 
            CREATE TABLE IF NOT EXISTS staging_ip(
                ip_id SERIAL PRIMARY KEY,
                ip VARCHAR
            );
            
            INSERT INTO staging_ip (ip)
            SELECT DISTINCT ip 
            FROM staging_log_data
            WHERE NOT EXISTS (
                SELECT * 
                FROM staging_ip 
                WHERE staging_ip.ip = staging_log_data.ip
            ); 
        '''
    )
    
    determine_ip_location_task = PythonOperator(
        task_id = 'determine_ip_location',
        python_callable = determine_ip_location, 
    )
    

    build_dim_ip_table_task = PostgresOperator(
        task_id = 'build_dim_ip_table',
        sql = 
        '''
            DROP TABLE IF EXISTS dim_ip;
            
            CREATE TABLE dim_ip AS
            SELECT * FROM staging_ip;
        '''
    )
    
    
    ###### DATE TASKS ######
    extract_unique_date_task = PostgresOperator(
        task_id = 'extract_unique_date',
        sql = 
        '''
            DROP TABLE IF EXISTS staging_date;
            
            CREATE TABLE staging_date (
                date_id SERIAL PRIMARY KEY,
                date VARCHAR
            );
            
            INSERT INTO staging_date (date)
            SELECT DISTINCT date from staging_log_data;
        '''
    )
    
    determine_date_details_task = PythonOperator(
        task_id = 'determine_date_details',
        python_callable = determine_date_details, 
    )
    
    build_dim_date_table_task = PostgresOperator(
        task_id = 'build_dim_date_table',
        sql = 
        '''
            DROP TABLE IF EXISTS dim_date;
            
            CREATE TABLE dim_date AS
            SELECT * FROM staging_date;
        '''
    )
        

    ##### BROWSER TASKS ######
    
    determine_browser_task = PythonOperator(
        task_id = 'determine_browser',
        python_callable = determine_browser,
    )
    
    extract_unique_browser_task = PostgresOperator(
        task_id = 'extract_unique_browser',
        sql = 
        '''
            DROP TABLE IF EXISTS staging_browser;
            
            CREATE TABLE staging_browser (
                browser_id SERIAL PRIMARY KEY,
                browser VARCHAR
            );
            
            INSERT INTO staging_browser (browser)
            SELECT DISTINCT browser from staging_log_data;
        '''
    )
    
    build_dim_browser_table_task = PostgresOperator(
        task_id = 'build_dim_browser_table',
        sql = 
        '''
            DROP TABLE IF EXISTS dim_browser;
            
            CREATE TABLE dim_browser AS
            SELECT * FROM staging_browser;
        '''
    )
    
    ##### OS TASKS ######  
    
    determine_os_task = PythonOperator(
        task_id = 'determine_os',
        python_callable = determine_os,
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
        '''
    )
    
    build_dim_os_table_task = PostgresOperator(
        task_id = 'build_dim_os_table',
        sql = 
        '''
            DROP TABLE IF EXISTS dim_os;
            
            CREATE TABLE dim_os AS
            SELECT * FROM staging_os;
        '''
    )
    
    ## FILE TASKS
    
    extract_unique_file_path_task = PostgresOperator(
        task_id = 'extract_unique_file_path',
        sql = 
        '''
            DROP TABLE IF EXISTS staging_file;
            
            CREATE TABLE staging_file (
                file_id SERIAL PRIMARY KEY,
                raw_file_path VARCHAR
            );
            
            INSERT INTO staging_file (raw_file_path)
            SELECT DISTINCT raw_file_path from staging_log_data;
        '''
    )
    
    extract_file_details_task = PythonOperator(
        task_id = 'extract_file_details',
        python_callable = extract_file_details,
    )
    
    
    build_dim_file_table_task = PostgresOperator(
        task_id = 'build_dim_file_table',
        sql = 
        '''
            DROP TABLE IF EXISTS dim_file;
            
            CREATE TABLE dim_file AS
            SELECT * FROM staging_file;
        '''
    )
    
    ## TIME TASKS
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
        '''
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
        '''
    )
    
    
    build_dim_time_table_task = PostgresOperator(
        task_id = 'build_dim_time_table',
        sql = 
        '''
            DROP TABLE IF EXISTS dim_time;
            
            CREATE TABLE dim_time AS
            SELECT * FROM staging_time;
        '''
    )
    
    ##### FACT TASKS ######
    build_fact_table_task = PostgresOperator(
        task_id = 'build_fact_table',
        sql = 
        '''
            DROP TABLE IF EXISTS log_fact_table;
            
            CREATE TABLE log_fact_table AS
            SELECT log_id, date, time, raw_file_path, ip, browser, os, response_time, is_bot FROM staging_log_data;
            
            
            UPDATE log_fact_table AS f
            SET ip = dim.ip_id
            FROM dim_ip AS dim
            WHERE f.ip = dim.ip;

            ALTER TABLE log_fact_table
            RENAME COLUMN ip TO ip_id;
                
                  
            UPDATE log_fact_table AS f
            SET date = dim.date_id
            FROM dim_date AS dim
            WHERE f.date = dim.date;
            
            ALTER TABLE log_fact_table
            RENAME COLUMN date TO date_id;
            
 
            
            UPDATE log_fact_table AS f
            SET time = dim.time_id
            FROM dim_time AS dim
            WHERE f.time::TIME = dim.time;
            
            ALTER TABLE log_fact_table
            RENAME COLUMN time TO time_id;
            
            
            
            UPDATE log_fact_table AS f
            SET browser = dim.browser_id
            FROM dim_browser AS dim
            WHERE f.browser = dim.browser;
            
            ALTER TABLE log_fact_table
            RENAME COLUMN browser TO browser_id;
            
            
            
            UPDATE log_fact_table AS f
            SET os = dim.os_id
            FROM dim_os AS dim
            WHERE f.os = dim.os;
            
            ALTER TABLE log_fact_table
            RENAME COLUMN os TO os_id;
            
            
            UPDATE log_fact_table AS f
            SET raw_file_path = dim.file_id
            FROM dim_file AS dim
            WHERE f.raw_file_path = dim.raw_file_path;
            
            ALTER TABLE log_fact_table
            RENAME COLUMN raw_file_path TO file_id;
            
            

            ALTER TABLE log_fact_table
            ALTER COLUMN date_id TYPE INT USING date_id::INT,
            ALTER COLUMN time_id TYPE INT USING time_id::INT,
            ALTER COLUMN file_id TYPE INT USING file_id::INT,
            ALTER COLUMN ip_id TYPE INT USING ip_id::INT,
            ALTER COLUMN browser_id TYPE INT USING browser_id::INT,
            ALTER COLUMN os_id TYPE INT USING os_id::INT;
        '''
    )

    # START
    extract_raw_data_task >> create_staging_log_data_table_task >> insert_staging_log_data_task >> determine_if_bot_task
    
    
    # IP
    insert_staging_log_data_task >> extract_unique_ip_task >>  determine_ip_location_task >> build_dim_ip_table_task


    # DATE
    insert_staging_log_data_task >> extract_unique_date_task >> determine_date_details_task >> build_dim_date_table_task


    # BROWSER
    insert_staging_log_data_task >> determine_browser_task >> extract_unique_browser_task >> build_dim_browser_table_task
    
    
    # OS
    insert_staging_log_data_task >> determine_os_task >> extract_unique_os_task >> build_dim_os_table_task
    
    
    # FILE
    insert_staging_log_data_task >> extract_unique_file_path_task >> extract_file_details_task >> build_dim_file_table_task
    
    
    # TIME
    insert_staging_log_data_task >> extract_unique_time_task >> determine_time_details_task >> build_dim_time_table_task
    
    
    # FACT TABLE
    build_fact_table_task.set_upstream(task_or_task_list = [determine_if_bot_task, build_dim_ip_table_task, build_dim_date_table_task, build_dim_browser_table_task, build_dim_os_table_task, build_dim_file_table_task, build_dim_time_table_task])
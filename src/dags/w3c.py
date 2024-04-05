import airflow
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from sqlalchemy import create_engine
from airflow.models import Connection

import os
import csv
import datetime as dt
from datetime import datetime
import re

# DODGY CODE
import subprocess
subprocess.call(['pip', 'install', 'user-agents'])
from user_agents import parse


import psycopg2


import logging
import json
import requests
import requests.exceptions as requests_exceptions



# Global variables
BASE_DIR = '/opt/airflow/data'
RAW_DATA = BASE_DIR + '/W3SVC1/'
STAGING = BASE_DIR + '/staging/'
STAR_SCHEMA = BASE_DIR + '/star-schema/'



def get_db_connection():
    conn = psycopg2.connect(
        dbname='airflow',
        user='airflow',
        password='airflow',
        host='postgres',
        port='5432'
    )
    return conn


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
            

def sanitize_string(string):
    clean = re.sub(r'[^\w/.]', '', string)
    return clean
   
     
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



def update_date_with_details():
    conn = get_db_connection()
    cursor = conn.cursor()
    
    cursor.execute('SELECT date FROM staging_date;')
    dates = cursor.fetchall()
    
    dates = [x[0] for x in dates]
    
    cursor.execute('''
        ALTER TABLE staging_date
        ADD COLUMN year INT,
        ADD COLUMN month INT,
        ADD COLUMN day INT,
        ADD COLUMN week_day VARCHAR;
    ''')
    
    for date in dates:
        result = extract_date_details(date)
        
        cursor.execute('''
            UPDATE staging_date 
            SET year  = %s, month = %s, day = %s, week_day = %s
            WHERE date = %s;
            ''', (*result, date))

    conn.commit()
    cursor.close()
    conn.close()
    

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



def extract_date_details(date):
        
    logging.debug('Extracting date details: ' + date)
        
    DAYS = ['Monday','Tuesday','Wednesday','Thursday','Friday','Saturday','Sunday']
    
    try:
        date = datetime.strptime(date,'%Y-%m-%d').date()
        weekday = DAYS[date.weekday()]
        
        #out = str(date) + ',' + str(date.year) + ',' + str(date.month) + ',' + str(date.day) + ',' + weekday + '\n'  
        return (date.year, date.month, date.day, weekday)

    except:
        logging.error('Error with extracting date details ' + date)
           
            
def update_ip_with_location():
        
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
    
    print(ips)
    
    for ip in ips:
        
        logging.debug(f"Finding location IP: {ip}")
        print(f"Finding location IP: {ip}")
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
        
        #out = line + ',' + str(result['country_code'] ) + ',' + str(result['country_name']) + ',' + str(result['city'] ) +',' + str(result['latitude']) + ',' + str(result['longitude']) + '\n'
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
    schedule_interval = '@daily',                                     
    start_date = dt.datetime(2023, 2, 24), 
    catchup = False,
) as dag:


    extract_raw_data_task = PythonOperator(
        task_id = 'extract_log_data',
        python_callable = process_raw_data, 
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
        """
    )
    
    insert_staging_log_data_task = PythonOperator(
        task_id = 'insert_log_data',
        python_callable = insert_staging_log_data,
    )
    
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
    
    update_ip_with_location_task = PythonOperator(
        task_id = 'update_ip_with_location',
        python_callable = update_ip_with_location, 
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
    
    update_date_with_details_task = PythonOperator(
        task_id = 'update_date_with_details',
        python_callable = update_date_with_details, 
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
        
    # update_staging_log_with_date_dim_task = PostgresOperator(
    #     task_id = 'update_staging_log_with_date_id',
    #     sql = 
    #     '''
    #         UPDATE staging_log_data AS f
    #         SET date = dim.date_id
    #         FROM dim_date AS dim
    #         WHERE f.date = dim.date;
    #     '''
    # )

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
            
            UPDATE log_fact_table AS f
            SET date = dim.date_id
            FROM dim_date AS dim
            WHERE f.date = dim.date;
            
            UPDATE log_fact_table AS f
            SET browser = dim.browser_id
            FROM dim_browser AS dim
            WHERE f.browser = dim.browser;
            
            UPDATE log_fact_table AS f
            SET os = dim.os_id
            FROM dim_os AS dim
            WHERE f.os = dim.os;
            
            UPDATE log_fact_table AS f
            SET raw_file_path = dim.file_id
            FROM dim_file AS dim
            WHERE f.raw_file_path = dim.raw_file_path;
            
            ALTER TABLE log_fact_table
            ALTER COLUMN date TYPE INT USING date::INT,
            ALTER COLUMN raw_file_path TYPE INT USING raw_file_path::INT,
            ALTER COLUMN ip TYPE INT USING ip::INT,
            ALTER COLUMN browser TYPE INT USING browser::INT,
            ALTER COLUMN os TYPE INT USING os::INT;
        '''
    )

    # START
    extract_raw_data_task >> create_staging_log_data_table_task >> insert_staging_log_data_task >> determine_if_bot_task
    
    
    # IP
    determine_if_bot_task >> extract_unique_ip_task >>  update_ip_with_location_task >> build_dim_ip_table_task


    # DATE
    determine_if_bot_task >> extract_unique_date_task >> update_date_with_details_task >> build_dim_date_table_task


    # BROWSER
    determine_if_bot_task >> determine_browser_task >> extract_unique_browser_task >> build_dim_browser_table_task
    
    
    # OS
    determine_if_bot_task >> determine_os_task >> extract_unique_os_task >> build_dim_os_table_task
    
    
    # FILE
    determine_if_bot_task >> extract_unique_file_path_task >> extract_file_details_task >> build_dim_file_table_task
    
    # TIME
    determine_if_bot_task >> extract_unique_time_task
    
    
    # FACT TABLE
    build_fact_table_task.set_upstream(task_or_task_list = [build_dim_ip_table_task, build_dim_date_table_task, build_dim_browser_table_task, build_dim_os_table_task, build_dim_file_table_task])
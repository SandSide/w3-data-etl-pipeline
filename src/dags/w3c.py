import airflow
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from sqlalchemy import create_engine

from airflow.models import Connection
import psycopg2
import datetime as dt

import os
import csv
from datetime import datetime
import logging

import json
import requests
import requests.exceptions as requests_exceptions


DB = ENV_ID = os.environ.get("AIRFLOW__DATABASE__SQL_ALCHEMY_CONN")


# Global variables
BASE_DIR = '/opt/airflow/data'
RAW_DATA = BASE_DIR + '/W3SVC1/'
STAGING = BASE_DIR + '/staging/'
STAR_SCHEMA = BASE_DIR + '/star-schema/'

ROBOTS = [
    'YandexBot', 
    'Googlebot', 
    'Baiduspider', 
    'DotBot',
    'Slurp', 
    'msnbot',
    'MLBot',
    'yacybot',
    '/robots.txt', 
    'robot',
    'Robot',
    'Bot',
    'bot'
]


def get_db_connection():
    conn = psycopg2.connect(
        dbname='airflow',
        user='airflow',
        password='airflow',
        host='postgres',
        port='5432'
    )
    return conn


# def create_directory():
    
#     print('Creating directories')
        
#     try: 
#         os.mkdir(STAGING)
#     except FileExistsError:
#         print('Cant make staging dir') 
    
#     try:
#         os.mkdir(STAR_SCHEMA)
#     except FileExistsError:
#         print('Cant make star schema dir') 
        
#     print('Finished creating directories')


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
        out_file_robot = open(STAGING + 'data-robot.txt', 'a')
        out_file = open(STAGING + 'merged-data.txt', 'a')
        
        lines = in_file.readlines()
        
        for line in lines:
            
            if (line[0] != '#'):
            
                result = process_log_line(line)

                if result:
                    if not result.endswith('\n'):
                        result += '\n'
                    out_file.write(result)
                else:
                    out_file_robot.write(line)
                
        in_file.close()
        out_file_robot.close()
        out_file.close()
                 
    
def process_log_line(line):    
    
    if not any(robot in line for robot in ROBOTS):
    
        split = line.split(' ')
        
        logging.debug('Processing ', len(split))
        
        if (len(split) == 14):
            browser = split[9].replace(',','')
            out = split[0] + ',' + split[1] + ',' + browser + ',' + split[8] + ',' + split[13]
            return out
            
        elif (len(split) == 18):  
            browser = split[9].replace(',','')
            out = split[0] + ',' + split[1] + ',' + browser + ',' + split[8] + ',' + split[16]
            return out

        else:
            logging.debug('Fault line ' + str(len(split)))
            return None
            
    else:
        logging.debug('Robot')
        return None

    
def clear_files():
    out_file_long = open(STAGING + 'merged-data.txt', 'w')
    
    
def insert_staging_log_data():
    
    conn = get_db_connection()
    cursor = conn.cursor()
    
    with open(STAGING + 'merged-data.txt', 'r') as file:
        for line in file:
            values = line.strip().split(',')
            cursor.execute('INSERT INTO staging_log_data (date, time, browser, ip, response_time) VALUES (%s, %s, %s, %s, %s)', values)
            
    conn.commit()
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
            date VARCHAR,
            time VARCHAR,
            browser VARCHAR,
            ip VARCHAR,
            response_time int
        );
        """
    )
    
    insert_staging_log_data_task = PythonOperator(
        task_id = 'insert_log_data',
        python_callable = insert_staging_log_data,
    )


    create_staging_ip_table_task = PostgresOperator(
        task_id = 'create_staging_ip_table',
        sql = 
        '''
            CREATE TABLE IF NOT EXISTS staging_ip(
                ip_id SERIAL PRIMARY KEY,
                ip VARCHAR
            )
        '''
    )

    extract_unique_ip_task = PostgresOperator(
        task_id = 'extract_unique_ip',
        sql = 
        '''        
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
    
    build_dim_date_table_task = PostgresOperator(
        task_id = 'build_dim_date_table',
        sql = 
        '''
            DROP TABLE IF EXISTS dim_date;
            
            CREATE TABLE dim_date AS
            SELECT * FROM staging_date;
        '''
    )
    
    update_staging_log_with_ip_dim_task = PostgresOperator(
        task_id = 'update_staging_log_with_ip_id',
        sql = 
        '''
            UPDATE staging_log_data AS f
            SET ip = dim.ip_id
            FROM dim_ip AS dim
            WHERE f.ip = dim.ip;
        '''
    )
    
    update_staging_log_with_date_dim_task = PostgresOperator(
        task_id = 'update_staging_log_with_date_id',
        sql = 
        '''
            UPDATE staging_log_data AS f
            SET date = dim.date_id
            FROM dim_date AS dim
            WHERE f.date = dim.date;
        '''
    )

    build_fact_table_task = PostgresOperator(
        task_id = 'build_fact_table',
        sql = 
        '''
            DROP TABLE IF EXISTS log_fact_table;
            
            CREATE TABLE log_fact_table AS
            SELECT * FROM staging_log_data;
        '''
    )

    # copy_fact_table_task = BashOperator(
    #     task_id = 'copy_fact_table',
    #     bash_command = 'cp ' + STAGING + 'merged-data.txt ' + STAR_SCHEMA + 'fact_table.txt ',
    # )

    extract_raw_data_task >> create_staging_log_data_table_task >> insert_staging_log_data_task >> create_staging_ip_table_task
    
    # IP
    create_staging_ip_table_task >> extract_unique_ip_task >>  update_ip_with_location_task >> build_dim_ip_table_task >> update_staging_log_with_ip_dim_task
    # extract_unique_ip_task.set_upstream(task_or_task_list = create_staging_ip_table_task)
    # update_ip_with_location_task.set_upstream(task_or_task_list = extract_unique_ip_task)
    # build_dim_ip_table_task.set_upstream(task_or_task_list = update_ip_with_location_task)
    # update_staging_log_with_ip_dim_task.set_upstream(task_or_task_list = build_dim_ip_table_task)
    
    # DATE
    insert_staging_log_data_task >> extract_unique_date_task >> update_date_with_details_task >> build_dim_date_table_task >> update_staging_log_with_date_dim_task
    # extract_unique_date_task.set_upstream(task_or_task_list = insert_staging_log_data_task)
    # update_date_with_details_task.set_upstream(task_or_task_list = extract_unique_date_task)
    # build_dim_date_table_task.set_upstream(task_or_task_list = update_date_with_details_task)
    # update_staging_date_with_date_dim_task.set_upstream(task_or_task_list = buildbuild_dim_date_table_task_dim_ip_table_task)
    
    # FACT TABLE
    build_fact_table_task.set_upstream(task_or_task_list = [update_staging_log_with_ip_dim_task, update_staging_log_with_date_dim_task])
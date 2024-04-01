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


def create_directory():
    
    print('Creating directories')
        
    try: 
        os.mkdir(STAGING)
    except FileExistsError:
        print('Cant make staging dir') 
    
    try:
        os.mkdir(STAR_SCHEMA)
    except FileExistsError:
        print('Cant make star schema dir') 
        
    print('Finished creating directories')


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

def extract_date():
    
    in_file = open(STAGING + 'merged-data.txt', 'r')
    out_file = open(STAGING + 'dim-date.txt', 'w')

    lines = in_file.readlines()
    
    for line in lines:
        split = line.split(',')
        out = split[0] + '\n'
        out_file.write(out)
 

DAYS = ['Monday','Tuesday','Wednesday','Thursday','Friday','Saturday','Sunday']

 
def build_dim_date_table():
    
    in_file = open(STAGING + 'dim-date-uniq.txt', 'r')   
    out_file = open(STAR_SCHEMA + 'dim-date-table.txt', 'w')
    
    with out_file as file:
       file.write('Date,Year,Month,Day,DayofWeek\n')
       
    lines = in_file.readlines()
    
    for line in lines:
        
        line = line.replace('\n','')
        print(line)
        
        try:
            date = datetime.strptime(line,'%Y-%m-%d').date()
            weekday = DAYS[date.weekday()]
            
            out = str(date) + ',' + str(date.year) + ',' + str(date.month) + ',' + str(date.day) + ',' + weekday + '\n'
            
            with open(STAR_SCHEMA + 'dim-date-table.txt', 'a') as file:
               file.write(out)
        except:
            logging.error('Error with creating Date table')
           
            
def build_dim_ip_loc_table():
    
    table_name = STAR_SCHEMA + 'dim-ip-loc-table.txt'
    
    # Dont call api if we dont have to
    try:
        file_stats = os.stat(table_name)
    
        if (file_stats.st_size >2):
           logging.info('Dim IP Loc Table already exists')
           return
    except:
        logging.exception('Dim IP Loc Table does not exist, creating one')



    in_file = open(STAGING + 'dim-ip-uniq.txt', 'r')
     
    lines = in_file.readlines()
    
    for line in lines:
        
        line = line.replace('\n','')
        
        # URL to send the request to
        request_url = 'https://geolocation-db.com/jsonp/' + line

        # Send request and decode the result
        try:
            response = requests.get(request_url)
            result = response.content.decode()
        except:
            logging.exception('error response ' + result)
            
        try:
            # Clean the returned string so it just contains the dictionary data for the IP address
            result = result.split('(')[1].strip(')')
            
            # Convert this data into a dictionary
            result  = json.loads(result)
            
            out = line + ',' + str(result['country_code'] ) + ',' + str(result['country_name']) + ',' + str(result['city'] ) +',' + str(result['latitude']) + ',' + str(result['longitude']) + '\n'

            with open(STAR_SCHEMA + 'dim-ip-loc-table.txt', 'a') as file:
               file.write(out)
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

    extract_unique_ip_task = PostgresOperator(
        task_id = 'extract_unique_ip',
        sql = 
        '''
            DROP TABLE IF EXISTS staging_ip;
            
            CREATE TABLE staging_ip (
                ip_id SERIAL PRIMARY KEY,
                ip VARCHAR
            );
            
            INSERT INTO staging_ip (ip)
            SELECT DISTINCT ip from staging_log_data;
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

    # extract_date_task = PythonOperator(
    #     task_id = 'extract_date',
    #     python_callable = extract_date,   
    # )

    # unique_ip_task = BashOperator(
    #     task_id = 'unique_ip',
    #     bash_command = 'sort -u ' + STAGING + 'dim-ip.txt > ' + STAGING + 'dim-ip-uniq.txt',     
    # )

    # unique_date_task = BashOperator(
    #     task_id = 'unique_date',
    #     bash_command = 'sort -u ' + STAGING + 'dim-date.txt > ' + STAGING + 'dim-date-uniq.txt',    
    # )

    # build_dim_date_table_task = PythonOperator(
    #     task_id = 'build_dim_date_table',
    #     python_callable = build_dim_date_table, 
    # )

    # build_dim_ip_loc_table_task = PythonOperator(
    #     task_id='build_dim_ip_table',
    #     python_callable = build_dim_ip_loc_table,
    # )

    # copy_fact_table_task = BashOperator(
    #     task_id = 'copy_fact_table',
    #     bash_command = 'cp ' + STAGING + 'merged-data.txt ' + STAR_SCHEMA + 'fact_table.txt ',
    # )

    extract_raw_data_task >> create_staging_log_data_table_task >> insert_staging_log_data_task
    
    extract_unique_ip_task.set_upstream(task_or_task_list = insert_staging_log_data_task)
    extract_unique_date_task.set_upstream(task_or_task_list = insert_staging_log_data_task)
    
    #>> #merge_data_sources_task >> [extract_date_task, extract_ip_task, copy_fact_table_task]

    # unique_ip_task.set_upstream(task_or_task_list = extract_ip_task)
    # unique_date_task.set_upstream(task_or_task_list = extract_date_task)

    # build_dim_date_table_task.set_upstream(task_or_task_list = unique_date_task)
    # build_dim_ip_loc_table_task.set_upstream(task_or_task_list = unique_ip_task)
    
    # test_db = PostgresOperator(
    #     task_id = 'test',
    #     sql = """
    #         CREATE TABLE test (
    #             name VARCHAR NOT NULL
    #         )
    #     """,
    # )
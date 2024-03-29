import datetime as dt
import csv
import airflow
import requests
import os
from datetime import datetime
import requests.exceptions as requests_exceptions
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
import requests
import json
import mysql.connector
import logging


# Global variables
BASE_DIR = '/opt/airflow/data'
RAW_DATA = BASE_DIR + '/W3SVC1/'
STAGING = BASE_DIR + '/staging/'
STAR_SCHEMA = BASE_DIR + '/star-schema/'

   
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
    
        out_file_short = open(STAGING + 'data-short.txt', 'a')
        out_file_long = open(STAGING + 'data-long.txt', 'a')

        in_file = open(RAW_DATA + filename, 'r')
    
        lines= in_file.readlines()
        
        for line in lines:
            if (line[0] != '#'):
                
                split = line.split(' ')
                
                if (len(split) == 14):
                    out_file_short.write(line)
                    logging.debug('Short ', filename, len(split))
                elif (len(split) == 18):
                    out_file_long.write(line)
                    logging.debug('Long ', filename, len(split))
                else:
                    logging.debug('Fault ' + str(len(split)))
    
    
def clear_files():
    out_file_short = open(STAGING + 'data-short.txt', 'w')
    out_file_long = open(STAGING + 'data-long.txt', 'w')
    
    
def merge_data_sources():
    with open(STAGING + 'merged-data.txt', 'w') as file:
        file.write('Date,Time,Browser,IP,ResponseTime\n')
        
    merge_short()
    merge_long()
 
       
def merge_short():
    in_file = open(STAGING + 'data-short.txt','r')
    out_file = open(STAGING + 'merged-data.txt', 'a')

    lines = in_file.readlines()
    
    for line in lines:
        
        split = line.split(' ')
        
        browser = split[9].replace(',','')
        out = split[0] + ',' + split[1] + ',' + browser + ',' + split[8] + ',' + split[13]

        out_file.write(out)


def merge_long():
    in_file = open(STAGING + 'data-long.txt', 'r')
    out_file = open(STAGING + 'merged-data.txt', 'a')

    lines = in_file.readlines()
    
    for line in lines:
        
        split = line.split(' ')
        browser = split[9].replace(',','')
        
        out = split[0] + ',' + split[1] + ',' + browser + ',' + split[8] + ',' + split[16]
        
        out_file.write(out)

 
def extract_ip():
    
    in_file = open(STAGING + 'merged-data.txt', 'r')
    out_file = open(STAGING + 'dim-ip.txt', 'w')
    
    lines= in_file.readlines()
    
    for line in lines:
        
        split = line.split(',')
        out = split[3] + '\n'
        out_file.write(out)

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


dag = DAG(                                                     
   dag_id = 'Process_W3_Data',                          
   schedule_interval = '@daily',                                     
   start_date = dt.datetime(2023, 2, 24), 
   catchup = False,
)


create_directory_task = PythonOperator(
    task_id = 'create_directories',
    python_callable = create_directory,
    dag = dag,
)


process_raw_data_task = PythonOperator(
   task_id = 'process_raw_data',
   python_callable = process_raw_data, 
   dag = dag,
)

merge_data_sources_task = PythonOperator(
    task_id = 'merge_data_sources',
    python_callable = merge_data_sources,
    dag = dag, 
)

extract_ip_task = PythonOperator(
    task_id = 'extract_ip',
    python_callable = extract_ip,
    dag = dag,
)

extract_date_task = PythonOperator(
    task_id = 'extract_date',
    python_callable = extract_date,
    dag = dag,
)

unique_ip_task = BashOperator(
    task_id = 'unique_ip',
    bash_command = 'sort -u ' + STAGING + 'dim-ip.txt > ' + STAGING + 'dim-ip-uniq.txt',
    dag = dag,
)

unique_date_task = BashOperator(
    task_id = 'unique_date',
    bash_command = 'sort -u ' + STAGING + 'dim-date.txt > ' + STAGING + 'dim-date-uniq.txt',
    dag = dag,
)

build_dim_date_table_task = PythonOperator(
   task_id = 'build_dim_date_table',
   python_callable = build_dim_date_table, 
   dag = dag,
)

build_dim_ip_loc_table_task = PythonOperator(
    task_id='build_dim_ip_table',
    python_callable = build_dim_ip_loc_table,
    dag = dag,
)

copy_fact_table_task = BashOperator(
    task_id = 'copy_fact_table',
    bash_command = 'cp ' + STAGING + 'merged-data.txt ' + STAR_SCHEMA + 'fact_table.txt ',
    dag = dag,
)

create_directory_task >> process_raw_data_task >> merge_data_sources_task >> [extract_date_task, extract_ip_task, copy_fact_table_task]

unique_ip_task.set_upstream(task_or_task_list = extract_ip_task)
unique_date_task.set_upstream(task_or_task_list = extract_date_task)

build_dim_date_table_task.set_upstream(task_or_task_list = unique_date_task)
build_dim_ip_loc_table_task.set_upstream(task_or_task_list = unique_ip_task)
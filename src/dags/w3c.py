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


def clean_raw_data():
   
   arr=os.listdir(RAW_DATA)
   
   if not arr:
      print('Raw data folder is empty')

   logging.debug('Raw file list:' + ','.join(str(element) for element in arr)) 
   
   clear_files()
   
   for f in arr:
       clean_hash(f)


def clean_hash(filename):
    
    logging.debug('Cleaning ' + filename)
    
    type = filename[-3:len(filename)]
    
    if (type == 'log'):
    
        OutputFileShort = open(STAGING + 'output-short.txt', 'a')
        OutputFileLong = open(STAGING + 'output-long.txt', 'a')

        InFile = open(RAW_DATA + filename, 'r')
    
        Lines= InFile.readlines()
        
        for line in Lines:
            if (line[0] != '#'):
                
                Split = line.split(' ')
                
                if (len(Split) == 14):
                    OutputFileShort.write(line)
                    logging.debug('Short ', filename, len(Split))
                else:
                    if (len(Split) == 18):
                        OutputFileLong.write(line)
                        logging.debug('Long ', filename, len(Split))
                    else:
                        logging.debug('Fault ' + str(len(Split)))
    
    
def clear_files():
    OutputFileShort = open(STAGING + 'output-short.txt', 'w')
    OutputFileLong = open(STAGING + 'output-long.txt', 'w')
   
    
def build_fact_1():
    with open(STAGING + 'out-fact-1.txt', 'w') as file:
        file.write('Date,Time,Browser,IP,ResponseTime\n')
        
    build_fact_short()
    build_fact_long()
 
       
def build_fact_short():
    InFile = open(STAGING + 'output-short.txt','r')
    OutFact1 = open(STAGING + 'out-fact-1.txt', 'a')

    Lines = InFile.readlines()
    
    for line in Lines:
        Split = line.split(' ')
        Browser = Split[9].replace(',','')
        Out = Split[0] + ',' + Split[1] + ',' + Browser + ',' + Split[8] + ',' + Split[13]

        OutFact1.write(Out)


def build_fact_long():
    InFile = open(STAGING + 'output-long.txt', 'r')
    OutFact1 = open(STAGING + 'out-fact-1.txt', 'a')

    Lines = InFile.readlines()
    
    for line in Lines:
        Split=line.split(' ')
        Browser=Split[9].replace(',','')
        Out = Split[0] + ',' + Split[1] + ',' + Browser + ',' + Split[8] + ',' + Split[16]
        
        OutFact1.write(Out)
 
def build_dim_ip():
    
    in_file = open(STAGING + 'out-fact-1.txt', 'r')
    output_file = open(STAGING + 'dim-ip.txt', 'w')
    
    lines= in_file.readlines()
    
    for line in lines:
        
        split = line.split(',')
        out = split[3] + '\n'
        output_file.write(out)

def build_dim_date():
    
    in_file = open(STAGING + 'out-fact-1.txt', 'r')
    out_file = open(STAGING + 'dim-date.txt', 'w')

    lines = in_file.readlines()
    
    for line in lines:
        split = line.split(',')
        out = split[0] + '\n'
        out_file.write(out)
 


#Days=['Monday','Tuesday','Wednesday','Thursday','Friday','Saturday','Sunday']

 
# def getDates():
#     InDateFile = open(Staging+'DimDateUniq.txt', 'r')   
#     OutputDateFile=open(StarSchema+'DimDateTable.txt', 'w')
#     with OutputDateFile as file:
#        file.write('Date,Year,Month,Day,DayofWeek\n')
#     Lines= InDateFile.readlines()
    
#     for line in Lines:
#         line=line.replace('\n','')
#         print(line)
#         try:
#             date=datetime.strptime(line,'%Y-%m-%d').date()
#             weekday=Days[date.weekday()]
#             out=str(date)+','+str(date.year)+','+str(date.month)+','+str(date.day)+','+weekday+'\n'
            
#             with open(StarSchema+'DimDateTable.txt', 'a') as file:
#                file.write(out)
#         except:
#             print('Error with Date')
            
# def GetLocations():
#     DimTablename=StarSchema+'DimIPLoc.txt'
#     try:
#         file_stats = os.stat(DimTablename)
    
#         if (file_stats.st_size >2):
#            print('Dim IP Table Exists')
#            return
#     except:
#         print('Dim Table IP does not exist, creating one')
#     InFile=open(Staging+'DimIPUniq.txt', 'r')
#     OutFile=open(StarSchema+'DimIPLoc.txt', 'w')
    
    
#     Lines= InFile.readlines()
#     for line in Lines:
#         line=line.replace('\n','')
#         # URL to send the request to
#         request_url = 'https://geolocation-db.com/jsonp/' + line
# #         print (request_url)
#         # Send request and decode the result
#         try:
#             response = requests.get(request_url)
#             result = response.content.decode()
#         except:
#             print ('error reponse'+result)
#         try:
#         # Clean the returned string so it just contains the dictionary data for the IP address
#             result = result.split('(')[1].strip(')')
#         # Convert this data into a dictionary
#             result  = json.loads(result)
#             out=line+','+str(result['country_code'])+','+str(result['country_name'])+','+str(result['city'])+','+str(result['latitude'])+','+str(result['longitude'])+'\n'
# #            print(out)
#             with open(StarSchema+'DimIPLoc.txt', 'a') as file:
#                file.write(out)
#         except:
#             print ('error getting location')

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


clean_raw_data_task = PythonOperator(
   task_id = 'clean_raw_data',
   python_callable = clean_raw_data, 
   dag = dag,
)

build_fact_1_task = PythonOperator(
    task_id = 'build_fact_1',
    python_callable = build_fact_1,
    dag = dag, 
)

build_dim_ip_task = PythonOperator(
    task_id = 'build_dim_ip',
    python_callable = build_dim_ip,
    dag = dag,
)

build_dim_date_task = PythonOperator(
    task_id = 'build_dim_date',
    python_callable = build_dim_date,
    dag = dag,
)

# IPTable = PythonOperator(
#     task_id='IPTable',
#     python_callable=GetLocations,
#     dag=dag,
# )

# BuildFact1 = PythonOperator(
#    task_id='BuildFact1',
#    python_callable= Fact1,
#    dag=dag,
# )

# BuildDimDate = PythonOperator(
#    task_id='BuildDimDate',
#    python_callable=getDates, 
#    dag=dag,
# )

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

# copyfact = BashOperator(
#     task_id='copyfact',
# #    bash_command=uniqDateCommand,
#      bash_command='cp /home/airflow/gcs/data/Staging/OutFact1.txt /home/airflow/gcs/data/StarSchema/OutFact1.txt',

#     dag=dag,
# )
 
  
# download_data >> BuildFact1 >>DimIp>>DateTable>>uniq>>uniq2>>BuildDimDate>>IPTable
# BuildFact1.set_upstream(task_or_task_list=[download_data])
# DimIp.set_upstream(task_or_task_list=[BuildFact1])
# DateTable.set_upstream(task_or_task_list=[BuildFact1])
# uniq2.set_upstream(task_or_task_list=[DateTable])
# uniq.set_upstream(task_or_task_list=[DimIp])
#clean_raw_data.set_upstream(task_or_task_list=[create_dir])

create_directory_task >> clean_raw_data_task >> build_fact_1_task >> [build_dim_ip_task, build_dim_date_task]

unique_ip_task.set_upstream(task_or_task_list=build_dim_ip_task)
unique_date_task.set_upstream(task_or_task_list=build_dim_date_task)


# BuildDimDate.set_upstream(task_or_task_list=[uniq2])
# IPTable.set_upstream(task_or_task_list=[uniq])
# copyfact.set_upstream(task_or_task_list=[IPTable,BuildDimDate])

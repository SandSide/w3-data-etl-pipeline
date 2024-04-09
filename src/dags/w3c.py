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


# from common_package import db_conn
import sys
sys.path.append('/opt/airflow') # Or os.getcwd() for this directory

import logging
import json
import requests # type: ignore


from common_package.process_raw_data import *
from common_package.bot_tasks import *
from common_package.date_tasks import *
from common_package.time_tasks import *
from common_package.ip_tasks import *
from common_package.browser_tasks import *
from common_package.os_tasks import *
from common_package.file_path_tasks import *
from common_package.status_code_tasks import *
from common_package.http_method_tasks import *
from common_package.time_taken_tasks import (
    extract_unique_time_taken_query,
    categorize_time_taken,
    build_dim_time_taken_query
)

from common_package.device_tasks import (
    determine_device_details,
    extract_unique_device_query
)

with DAG(
    dag_id = 'Process_W3_Data',                          
    schedule_interval = '@weekly',                                     
    start_date = dt.datetime(2023, 2, 24), 
    catchup = False,
) as dag:

    
    extract_raw_data_task = PythonOperator(
        task_id = 'extract_log_data',
        python_callable = process_raw_data, 
        dag = dag
    )
    
    create_staging_log_data_table_task = PostgresOperator(
        task_id = 'create_staging_log_data_table',
        sql = create_staging_log_data_table_query,
        dag = dag
    )
    
    insert_staging_log_data_task = PythonOperator(
        task_id = 'insert_log_data',
        python_callable = insert_staging_log_data,
        dag = dag
    )
    
    determine_if_bot_task = PythonOperator(
        task_id = 'determine_if_bot',
        python_callable = determine_if_bot,
        dag = dag
    )

    extract_unique_ip_task = PostgresOperator(
        task_id = 'extract_unique_ip',
        sql = extract_unique_ip_query,
        dag = dag
    )
    
    determine_ip_location_task = PythonOperator(
        task_id = 'determine_ip_location',
        python_callable = determine_ip_location, 
        dag = dag
    )
    
    build_dim_ip_table_task = PostgresOperator(
        task_id = 'build_dim_ip_table',
        sql = build_dim_ip_table_query,
        dag = dag
    )
    
    extract_unique_date_task = PostgresOperator(
        task_id = 'extract_unique_date',
        sql = extract_unique_date_query,
        dag = dag
    )
    
    determine_date_details_task = PythonOperator(
        task_id = 'determine_date_details',
        python_callable = determine_date_details, 
        dag = dag
    )
    
    build_dim_date_table_task = PostgresOperator(
        task_id = 'build_dim_date_table',
        sql = create_dim_date_table_query,
        dag = dag
    )

    determine_browser_task = PythonOperator(
        task_id = 'determine_browser',
        python_callable = determine_browser,
        dag = dag
    )
    
    extract_unique_browser_task = PostgresOperator(
        task_id = 'extract_unique_browser',
        sql = extract_unique_browser_query,
        dag = dag
    )
    
    build_dim_browser_table_task = PostgresOperator(
        task_id = 'build_dim_browser_table',
        sql = build_dim_browser_table_query,
        dag = dag
    )

    determine_os_task = PythonOperator(
        task_id = 'determine_os',
        python_callable = determine_os,
        dag = dag
    )
    
    extract_unique_os_task = PostgresOperator(
        task_id = 'extract_unique_os',
        sql = extract_unique_os_query,
        dag = dag
    )
    
    build_dim_os_table_task = PostgresOperator(
        task_id = 'build_dim_os_table',
        sql = build_dim_os_query,
        dag = dag
    )
 
    extract_unique_file_path_task = PostgresOperator(
        task_id = 'extract_unique_file_path',
        sql = extract_unique_file_path_query,
        dag = dag
    )
    
    extract_file_details_task = PythonOperator(
        task_id = 'extract_file_details',
        python_callable = extract_file_details,
        dag = dag
    )
    
    
    build_dim_file_table_task = PostgresOperator(
        task_id = 'build_dim_file_table',
        sql = build_dim_file_query,
        dag = dag
    )

    extract_unique_time_task = PostgresOperator(
        task_id = 'extract_unique_time',
        sql = extract_unique_time_task_query,
        dag = dag
    )
        
    determine_time_details_task = PostgresOperator(
        task_id = 'determine_time_details',
        sql = determine_time_details_query,
        dag = dag
    )
    
    
    build_dim_time_table_task = PostgresOperator(
        task_id = 'build_dim_time_table',
        sql = build_dim_time_table_query,
        dag = dag
    )
    
    extract_unique_status_code_task = PostgresOperator(
        task_id = 'extract_unique_status_code',
        sql = extract_unique_status_code_query,
        dag = dag
    )
    
    determine_status_code_details_task = PythonOperator(
        task_id = 'determine_status_code_details',
        python_callable = determine_status_code_details
    )

    build_dim_status_code_table_task = PostgresOperator(
        task_id = 'build_dim_status_code_table',
        sql = build_dim_status_code_table_query,
        dag = dag
    )
    
    extract_unique_http_method_task = PostgresOperator(
        task_id = 'extract_unique_http_method',
        sql = extract_unique_http_method_query
    )
    
    build_dim_http_method_table_task = PostgresOperator(
        task_id = 'build_dim_http_method_table',
        sql = build_dim_http_method_table_query
    )

    extract_unique_time_taken_task = PostgresOperator(
        task_id = 'extract_unique_time_taken',
        sql = extract_unique_time_taken_query
    )
    
    categorize_time_taken_task = PythonOperator(
        task_id = 'categorize_time_taken',
        python_callable = categorize_time_taken
    )

    build_dim_time_taken_table_task = PostgresOperator(
        task_id = 'build_dim_time_taken_table',
        sql = build_dim_time_taken_query
    )
    
    determine_device_details_task = PythonOperator(
        task_id = 'determine_device_details',
        python_callable = determine_device_details
    )
    
    extract_unique_device_task = PostgresOperator(
        task_id = 'extract_unique_device',
        sql = extract_unique_device_query
    )

    ##### FACT TASKS ######
    build_fact_table_task = PostgresOperator(
        task_id = 'build_fact_table',
        sql = 
        '''
            DROP TABLE IF EXISTS 
                log_fact_table;
            
            CREATE TABLE 
                log_fact_table 
            AS
                SELECT 
                    log_id, date, time, http_method, raw_file_path, ip, browser, os, status_code, time_taken, is_bot 
                FROM 
                    staging_log_data
            ORDER BY
                log_id;
            
            
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
            
            
            UPDATE 
                log_fact_table AS f
            SET 
                status_code = dim.status_code_id
            FROM 
                dim_status_code AS dim
            WHERE 
                f.status_code::INT = dim.status_code;
            
            ALTER TABLE 
                log_fact_table
            RENAME COLUMN 
                status_code TO status_code_id;
                
                
            UPDATE 
                log_fact_table AS f
            SET 
                http_method = dim.http_method_id
            FROM 
                dim_http_method AS dim
            WHERE 
                f.http_method = dim.http_method;
            
            ALTER TABLE 
                log_fact_table
            RENAME COLUMN 
                http_method TO http_method_id;         



            UPDATE 
                log_fact_table AS f
            SET 
                time_taken = dim.time_taken_id
            FROM 
                dim_time_taken AS dim
            WHERE 
                f.time_taken = dim.time_taken;
            
            ALTER TABLE 
                log_fact_table
            RENAME COLUMN 
                time_taken TO time_taken_id; 
 

            ALTER TABLE log_fact_table
            ALTER COLUMN date_id TYPE INT USING date_id::INT,
            ALTER COLUMN time_id TYPE INT USING time_id::INT,
            ALTER COLUMN file_id TYPE INT USING file_id::INT,
            ALTER COLUMN ip_id TYPE INT USING ip_id::INT,
            ALTER COLUMN browser_id TYPE INT USING browser_id::INT,
            ALTER COLUMN os_id TYPE INT USING os_id::INT,
            ALTER COLUMN status_code_id TYPE INT USING status_code_id::INT,
            ALTER COLUMN http_method_id TYPE INT USING http_method_id::INT;
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
    
    
    # STATUS CODE
    insert_staging_log_data_task >> extract_unique_status_code_task >> determine_status_code_details_task >> build_dim_status_code_table_task
    
    
    # HTTP METHOD
    insert_staging_log_data_task >> extract_unique_http_method_task >> build_dim_http_method_table_task
    
    
    # TIME TAKEN
    insert_staging_log_data_task >> extract_unique_time_taken_task >> categorize_time_taken_task >> build_dim_time_taken_table_task
    
    
    # DEVICE
    
    insert_staging_log_data_task >> determine_device_details_task >> extract_unique_device_task
    
    
    # FACT TABLE
    fact_table_dependencies = [
        determine_if_bot_task, 
        build_dim_ip_table_task, 
        build_dim_date_table_task, 
        build_dim_browser_table_task, 
        build_dim_os_table_task, 
        build_dim_file_table_task, 
        build_dim_time_table_task, 
        build_dim_status_code_table_task,
        build_dim_http_method_table_task,
        build_dim_time_taken_table_task
    ]
    
    
    build_fact_table_task.set_upstream(task_or_task_list = fact_table_dependencies)
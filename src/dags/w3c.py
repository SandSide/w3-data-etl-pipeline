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
from bot_tasks import define_bot_tasks
from date_tasks import define_date_tasks
from time_tasks import define_time_tasks
from ip_tasks import define_ip_tasks
from browser_tasks import define_browser_tasks
from os_tasks import define_os_tasks
from file_path_tasks import define_file_path_tasks


with DAG(
    dag_id = 'Process_W3_Data',                          
    schedule_interval = '@weekly',                                     
    start_date = dt.datetime(2023, 2, 24), 
    catchup = False,
) as dag:


    extract_raw_data_task, create_staging_log_data_table_task, insert_staging_log_data_task = define_process_raw_data_tasks(dag)
    
    determine_if_bot_task = define_bot_tasks(dag)

    extract_unique_ip_task, determine_ip_location_task, build_dim_ip_table_task = define_ip_tasks(dag)

    extract_unique_date_task, determine_date_details_task, build_dim_date_table_task = define_date_tasks(dag)

    determine_browser_task, extract_unique_browser_task, build_dim_browser_table_task = define_browser_tasks(dag)

    determine_os_task, extract_unique_os_task, build_dim_os_table_task = define_os_tasks(dag)
 
    extract_unique_file_path_task, extract_file_details_task, build_dim_file_table_task = define_file_path_tasks(dag)

    extract_unique_time_task, determine_time_details_task, build_dim_time_table_task = define_time_tasks(dag)
    ## TIME TASKS

    
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
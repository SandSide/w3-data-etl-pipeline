import airflow # type: ignore
from airflow import DAG # type: ignore
from airflow.operators.bash import BashOperator # type: ignore
from airflow.operators.python import PythonOperator # type: ignore
from airflow.providers.postgres.operators.postgres import PostgresOperator # type: ignore
from common_package.db_conn import get_db_connection
from datetime import datetime
import logging

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
        

extract_unique_date_query = '''
            DROP TABLE IF EXISTS staging_date;
            
            CREATE TABLE staging_date (
                date_id SERIAL PRIMARY KEY,
                date VARCHAR
            );
            
            INSERT INTO staging_date (date)
            SELECT DISTINCT date from staging_log_data;
        '''

create_dim_date_table_query =  '''
            DROP TABLE IF EXISTS dim_date;
            
            CREATE TABLE dim_date AS
            SELECT * FROM staging_date;
        '''
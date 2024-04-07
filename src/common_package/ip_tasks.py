import airflow # type: ignore
from airflow import DAG # type: ignore
from airflow.operators.bash import BashOperator # type: ignore
from airflow.operators.python import PythonOperator # type: ignore
from airflow.providers.postgres.operators.postgres import PostgresOperator # type: ignore
from common_package.db_conn import get_db_connection
import os
import logging
import json
import requests # type: ignore

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


extract_unique_ip_query = ''' 
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

build_dim_ip_table_query = '''
            DROP TABLE IF EXISTS dim_ip;
            
            CREATE TABLE dim_ip AS
            SELECT * FROM staging_ip;
        '''
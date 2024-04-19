import psycopg2 # type: ignore

def get_db_connection():
    conn = psycopg2.connect(
        dbname='airflow',
        user='airflow',
        password='airflow',
        host='postgres',
        port='5432'
    )
    
    return conn
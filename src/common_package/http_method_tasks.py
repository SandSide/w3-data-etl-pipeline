extract_unique_http_method_query = '''
    DROP TABLE IF EXISTS staging_http_method;
    
    CREATE TABLE 
        staging_http_method(
            http_method_id SERIAL PRIMARY KEY,
            http_method VARCHAR
        );
    
    INSERT INTO 
        staging_http_method (http_method)
    SELECT 
        DISTINCT http_method from staging_log_data;
'''

build_dim_http_method_table_query = '''
    DROP TABLE IF EXISTS
        dim_http_method;
        
    CREATE TABLE
        dim_http_method
    AS
        SELECT * FROM staging_http_method;
'''
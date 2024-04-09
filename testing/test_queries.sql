SELECT * 
FROM log_fact_table as f
INNER JOIN dim_ip as d ON f.ip_id = d.ip_id
WHERE d.country_code = 'GB';

-- 
SELECT d.year, d.quarter, COUNT(*) 
FROM log_fact_table as f
INNER JOIN dim_date as d ON f.date_id = d.date_id
WHERE is_bot = 'f'
GROUP BY d.year, d.quarter
ORDER BY d.year ASC, d.quarter ASC;


-- 
SELECT d.hour, COUNT(*) 
FROM log_fact_table as f
INNER JOIN dim_time as d ON f.time_id = d.time_id
WHERE is_bot = 'f'
GROUP BY d.hour
ORDER BY d.hour ASC;

SELECT d.week_day, COUNT(*) 
FROM log_fact_table as f
INNER JOIN dim_date as d ON f.date_id = d.date_id
WHERE is_bot = 'f'
GROUP BY d.week_day;


SELECT
    time_category,
    min_category_time, 
    max_category_time,
    COUNT(*) AS count
FROM
    staging_time_taken
WHERE
    min_category_time < 5000
GROUP BY
    time_category, min_category_time, max_category_time
ORDER BY 
    min_category_time;



SELECT 
    d.time_category, COUNT(*) 
FROM 
    log_fact_table as f
INNER JOIN 
    dim_time_taken as d 
ON 
    f.time_taken_id = d.time_taken_id
WHERE
    min_category_time < 5000
GROUP BY 
    d.time_category, d.min_category_time
ORDER BY 
    d.min_category_time DESC;



SELECT 
    d.device_type, COUNT(*)
FROM 
    log_fact_table as f
INNER JOIN 
    dim_device as d 
ON 
    f.device_id = d.device_id
GROUP BY
    d.device_type
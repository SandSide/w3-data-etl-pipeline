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
INNER JOIN dim_time as d ON f.time_id = d.time_id
WHERE is_bot = 'f'
GROUP BY d.week_day

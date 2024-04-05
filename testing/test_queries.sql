SELECT * 
FROM log_fact_table as f
INNER JOIN dim_ip as d ON f.ip_id = d.ip_id
WHERE d.country_code = 'GB';
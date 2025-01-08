CREATE OR REPLACE TABLE `final-dwh.chinook_cluster.City_Segment` AS
SELECT
    l.dim_location_id AS city_id,
    l.city AS city,
    COUNT(DISTINCT f.fact_sale_id) / 
        DATE_DIFF(
            MAX(SAFE.PARSE_DATE('%Y-%m-%d', CONCAT(t.Year, '-', t.Month, '-', t.Day))), 
            MIN(SAFE.PARSE_DATE('%Y-%m-%d', CONCAT(t.Year, '-', t.Month, '-', t.Day))), 
            DAY
        ) AS frequency,
    SUM(f.unit_price * f.Quantity) AS total_price,
    NULL AS result_cluster
FROM 
    chinook_olap.fact_sales f
JOIN 
    chinook_olap.DIM_Time t ON f.dim_date_id = t.dim_time_id
JOIN 
    chinook_olap.DIM_Location l ON f.dim_location_id = l.dim_location_id
WHERE 
    f.is_current = True AND
    SAFE.PARSE_DATE('%Y-%m-%d', CONCAT(t.Year, '-', t.Month, '-', t.Day)) IS NOT NULL
GROUP BY 
    l.dim_location_id,
    l.city
ORDER BY 
    l.city;
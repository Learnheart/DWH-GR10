CREATE OR REPLACE TABLE `final-dwh.chinook_olap.fact_sales` AS
SELECT 
    ROW_NUMBER() OVER() AS fact_change_id,  
    iv.InvoiceLineId AS fact_sale_id,
    i.InvoiceId AS dim_date_id,
    c.CustomerId AS dim_location_id,
    t.TrackId AS dim_track_id,
    iv.UnitPrice AS unit_price,
    iv.Quantity AS quantity,
    CURRENT_DATE() AS start_date, 
    CAST(NULL AS DATE) AS end_date,                       
    TRUE AS is_current                  
FROM 
    `final-dwh.chinook_stagging.customer` c
JOIN 
    `final-dwh.chinook_stagging.invoice` i 
    ON c.CustomerId = i.CustomerId
JOIN 
    `final-dwh.chinook_stagging.invoiceLine` iv 
    ON iv.InvoiceId = i.InvoiceId
JOIN 
    `final-dwh.chinook_stagging.track` t 
    ON t.TrackId = iv.TrackId;



-- CREATE OR REPLACE TABLE `final-dwh.chinook_olap.fact_sales` (
--     dim_change_id INT64,
--     dim_sale_id INT64,
--     dim_date_id INT64,
--     dim_location_id INT64,
--     dim_track_id INT64,
--     unit_price FLOAT64,
--     quantity INT64,
--     start_date DATE,
--     end_date DATE,
--     is_current BOOLEAN
-- );
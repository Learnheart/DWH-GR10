CREATE OR REPLACE TABLE `final-dwh.chinook_olap.DIM_Location` AS
  SELECT 
    c.CustomerId AS dim_location_id, 
    c.city AS city,
    c.state AS state, 
    c.country AS country,
    c.postalcode AS postalcode
  FROM `final-dwh.chinook_stagging.customer` c;

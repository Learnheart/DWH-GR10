CREATE OR REPLACE TABLE `final-dwh.chinook_olap.DIM_Time` AS
  SELECT 
    i.InvoiceId AS dim_time_id, 
    i.InvoiceDate AS date_value,
    EXTRACT(DAY FROM i.InvoiceDate) AS day, 
    EXTRACT(MONTH FROM i.InvoiceDate) AS month,
    EXTRACT(YEAR FROM i.InvoiceDate) AS year,
    CONCAT(
      'Q', CAST(EXTRACT(QUARTER FROM i.InvoiceDate) AS STRING), 
      '(', CAST(EXTRACT(YEAR FROM i.InvoiceDate) AS STRING), ')'
    ) AS quarter
  FROM `final-dwh.chinook_stagging.invoice` i;

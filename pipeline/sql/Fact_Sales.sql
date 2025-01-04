CREATE OR REPLACE TABLE `final-dwh.chinook_olap.fact_sales` AS
  SELECT 
    iv.InvoiceLineId AS dim_sale_id,
    i.InvoiceId AS dim_date_id,
    c.CustomerId AS dim_location_id,
    t.TrackId AS dim_track_id,
    iv.UnitPrice AS unit_price,
    iv.Quantity 
  FROM `final-dwh.chinook_stagging.customer` c
  JOIN `final-dwh.chinook_stagging.invoice` i ON c.CustomerId = i.CustomerId
  JOIN `final-dwh.chinook_stagging.invoiceLine` iv ON iv.InvoiceId = i.InvoiceId
  JOIN `final-dwh.chinook_stagging.track` t ON t.TrackId = iv.TrackId;

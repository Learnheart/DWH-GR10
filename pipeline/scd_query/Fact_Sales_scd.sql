-- scd type 2

-- insert
MERGE `final-dwh.chinook_olap.fact_sales` T
USING (
    SELECT 
        iv.InvoiceLineId AS dim_sale_id,
        i.InvoiceId AS dim_date_id,
        c.CustomerId AS dim_location_id,
        t.TrackId AS dim_track_id,
        iv.UnitPrice AS unit_price,
        iv.Quantity,
        CURRENT_DATE() AS start_date
    FROM `final-dwh.chinook_stagging.customer` c
    JOIN `final-dwh.chinook_stagging.invoice` i ON c.CustomerId = i.CustomerId
    JOIN `final-dwh.chinook_stagging.invoiceLine` iv ON iv.InvoiceId = i.InvoiceId
    JOIN `final-dwh.chinook_stagging.track` t ON t.TrackId = iv.TrackId
) S
ON T.dim_sale_id = S.dim_sale_id AND T.is_current = TRUE

-- change to false if current is updated
WHEN MATCHED AND T.unit_price != S.unit_price THEN
    UPDATE SET 
        end_date = CURRENT_DATE(),
        is_current = FALSE;
-- update data
MERGE `final-dwh.chinook_olap.fact_sales` T
USING (
    SELECT 
        iv.InvoiceLineId AS dim_sale_id,
        i.InvoiceId AS dim_date_id,
        c.CustomerId AS dim_location_id,
        t.TrackId AS dim_track_id,
        iv.UnitPrice AS unit_price,
        iv.Quantity,
        CURRENT_DATE() AS start_date
    FROM `final-dwh.chinook_stagging.customer` c
    JOIN `final-dwh.chinook_stagging.invoice` i ON c.CustomerId = i.CustomerId
    JOIN `final-dwh.chinook_stagging.invoiceLine` iv ON iv.InvoiceId = i.InvoiceId
    JOIN `final-dwh.chinook_stagging.track` t ON t.TrackId = iv.TrackId
) S
ON T.dim_sale_id = S.dim_sale_id AND T.is_current = TRUE

-- Insert a new version
WHEN NOT MATCHED THEN
    INSERT (dim_change_id, dim_sale_id, dim_date_id, dim_location_id, dim_track_id, unit_price, quantity, start_date, end_date, is_current)
    VALUES (
        (SELECT IFNULL(MAX(dim_change_id), 0) + 1 FROM `final-dwh.chinook_olap.fact_sales`), 
        S.dim_sale_id, 
        S.dim_date_id, 
        S.dim_location_id, 
        S.dim_track_id, 
        S.unit_price, 
        S.quantity, 
        S.start_date, 
        NULL, 
        TRUE
    );
-- delete data
MERGE `final-dwh.chinook_olap.fact_sales` T
USING (
    SELECT 
        iv.InvoiceLineId AS dim_sale_id
    FROM `final-dwh.chinook_stagging.customer` c
    JOIN `final-dwh.chinook_stagging.invoice` i ON c.CustomerId = i.CustomerId
    JOIN `final-dwh.chinook_stagging.invoiceLine` iv ON iv.InvoiceId = i.InvoiceId
    JOIN `final-dwh.chinook_stagging.track` t ON t.TrackId = iv.TrackId
) S
ON T.dim_sale_id = S.dim_sale_id AND T.is_current = TRUE

-- If the record is missing in the source, mark it as deleted
WHEN NOT MATCHED BY SOURCE THEN
    UPDATE SET 
        end_date = CURRENT_DATE(),
        is_current = FALSE;

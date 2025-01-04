-- scd type 1

MERGE `final-dwh.chinook_olap.DIM_Time` T
USING (
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
    FROM `final-dwh.chinook_stagging.invoice` i
) S
ON T.dim_time_id = S.dim_time_id

-- Update records if any column has changed
WHEN MATCHED AND (
    T.date_value != S.date_value OR
    T.day != S.day OR
    T.month != S.month OR
    T.year != S.year OR
    T.quarter != S.quarter
) THEN
    UPDATE SET 
        date_value = S.date_value,
        day = S.day,
        month = S.month,
        year = S.year,
        quarter = S.quarter

-- Insert new records if not matched
WHEN NOT MATCHED THEN
    INSERT (dim_time_id, date_value, day, month, year, quarter)
    VALUES (S.dim_time_id, S.date_value, S.day, S.month, S.year, S.quarter)

-- delete
WHEN NOT MATCHED BY SOURCE THEN
    DELETE;
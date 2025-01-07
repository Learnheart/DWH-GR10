-- scd type 1

MERGE `final-dwh.chinook_olap.DIM_Location` T
USING (
    SELECT 
        CustomerId AS dim_location_id, 
        city, 
        state, 
        country, 
        postalcode
    FROM `final-dwh.chinook_stagging.customer`
) S
ON T.dim_location_id = S.dim_location_id

-- Update if any value has changed
WHEN MATCHED AND (
    T.city != S.city OR
    T.state != S.state OR
    T.country != S.country OR
    T.postalcode != S.postalcode
) THEN
    UPDATE SET 
        city = S.city,
        state = S.state,
        country = S.country,
        postalcode = S.postalcode

-- Insert new records
WHEN NOT MATCHED THEN
    INSERT (dim_location_id, city, state, country, postalcode)
    VALUES (S.dim_location_id, S.city, S.state, S.country, S.postalcode)

-- delete
WHEN NOT MATCHED BY SOURCE THEN
    DELETE;
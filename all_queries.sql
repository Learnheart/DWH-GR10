SELECT *
FROM `final-dwh.chinook_olap.fact_sales`
LIMIT 10;

#Yearly Revenue
SELECT 
    dt.year AS Year,
    SUM(fs.unit_price * fs.quantity) AS TotalRevenue
FROM 
    `final-dwh.chinook_olap.fact_sales` fs
JOIN 
    `final-dwh.chinook_olap.DIM_Time` dt 
ON 
    fs.dim_date_id = dt.dim_time_id
GROUP BY 
    Year
ORDER BY 
    Year;
#Monthly Revenue
SELECT 
    dt.year AS Year,
    dt.month AS Month,
    SUM(fs.unit_price * fs.quantity) AS TotalRevenue
FROM 
    `final-dwh.chinook_olap.fact_sales` fs
JOIN 
    `final-dwh.chinook_olap.DIM_Time` dt 
ON 
    fs.dim_date_id = dt.dim_time_id
GROUP BY 
    Year, Month
ORDER BY 
    Year, Month;
#Quarterly Revenue
SELECT 
    dt.year AS Year,
    dt.quarter AS Quarter,
    SUM(fs.unit_price * fs.quantity) AS TotalRevenue
FROM 
    `final-dwh.chinook_olap.fact_sales` fs
JOIN 
    `final-dwh.chinook_olap.DIM_Time` dt 
ON 
    fs.dim_date_id = dt.dim_time_id
GROUP BY 
    Year, Quarter
ORDER BY 
    Year, Quarter;
#Artist Revenue Contribution by Location
SELECT 
    dl.country AS Country,
    dl.city AS City,
    dtrack.artist_name AS ArtistName,
    SUM(fs.unit_price * fs.quantity) AS TotalRevenue
FROM 
    `final-dwh.chinook_olap.fact_sales` fs
JOIN 
    `final-dwh.chinook_olap.DIM_Location` dl 
ON 
    fs.dim_location_id = dl.dim_location_id
JOIN 
    `final-dwh.chinook_olap.DIM_Track` dtrack 
ON 
    fs.dim_track_id = dtrack.dim_track_id
GROUP BY 
    Country, City, ArtistName
ORDER BY 
    TotalRevenue DESC
LIMIT 10;
#Best-Selling Track
SELECT 
    dt.track_name AS TrackName,
    SUM(fs.quantity) AS TotalSold
FROM 
    `final-dwh.chinook_olap.fact_sales` fs
JOIN 
    `final-dwh.chinook_olap.DIM_Track` dt 
ON 
    fs.dim_track_id = dt.dim_track_id
GROUP BY 
    TrackName
ORDER BY 
    TotalSold DESC
LIMIT 1;

#Location with Lowest Sales
SELECT 
    dl.country AS Country,
    dl.city AS City,
    SUM(fs.quantity) AS TotalQuantity
FROM 
    `final-dwh.chinook_olap.fact_sales` fs
JOIN 
    `final-dwh.chinook_olap.DIM_Location` dl 
ON 
    fs.dim_location_id = dl.dim_location_id
GROUP BY 
    Country, City
ORDER BY 
    TotalQuantity ASC
LIMIT 1;
#Most Popular Genre
SELECT 
    dt.genre AS Genre,
    SUM(fs.unit_price * fs.quantity) AS TotalRevenue
FROM 
    `final-dwh.chinook_olap.fact_sales` fs
JOIN 
    `final-dwh.chinook_olap.DIM_Track` dt 
ON 
    fs.dim_track_id = dt.dim_track_id
GROUP BY 
    Genre
ORDER BY 
    TotalRevenue DESC
LIMIT 1;
#Most Preferred Media Type
SELECT 
    dt.mediatype AS MediaType,
    SUM(fs.quantity) AS TotalSold
FROM 
    `final-dwh.chinook_olap.fact_sales` fs
JOIN 
    `final-dwh.chinook_olap.DIM_Track` dt 
ON 
    fs.dim_track_id = dt.dim_track_id
GROUP BY 
    MediaType
ORDER BY 
    TotalSold DESC
LIMIT 1;
#Period with Lowest Sales
SELECT 
    dt.year AS Year,
    dt.month AS Month,
    SUM(fs.quantity) AS TotalQuantity
FROM 
    `final-dwh.chinook_olap.fact_sales` fs
JOIN 
    `final-dwh.chinook_olap.DIM_Time` dt 
ON 
    fs.dim_date_id = dt.dim_time_id
GROUP BY 
    Year, Month
ORDER BY 
    TotalQuantity ASC
LIMIT 1;
#Price and Quantity Analysis by Location
SELECT 
    dl.country AS Country,
    dl.city AS City,
    fs.unit_price AS UnitPrice,
    SUM(fs.quantity) AS TotalQuantity,
    SUM(fs.unit_price * fs.quantity) AS TotalRevenue
FROM 
    `final-dwh.chinook_olap.fact_sales` fs
JOIN 
    `final-dwh.chinook_olap.DIM_Location` dl 
ON 
    fs.dim_location_id = dl.dim_location_id
JOIN 
    `final-dwh.chinook_olap.DIM_Track` dt 
ON 
    fs.dim_track_id = dt.dim_track_id
GROUP BY 
    Country, City, UnitPrice
ORDER BY 
    TotalRevenue DESC, TotalQuantity DESC
LIMIT 10;
#Quarter with Highest Revenue
SELECT 
    dt.year AS Year,
    dt.quarter AS Quarter,
    SUM(fs.unit_price * fs.quantity) AS TotalRevenue
FROM 
    `final-dwh.chinook_olap.fact_sales` fs
JOIN 
    `final-dwh.chinook_olap.DIM_Time` dt 
ON 
    fs.dim_date_id = dt.dim_time_id
GROUP BY 
    Year, Quarter
ORDER BY 
    TotalRevenue DESC
LIMIT 1;
#Quarterly Revenue by Genre and Location
SELECT 
    dt.quarter AS Quarter,
    dl.country AS Country,
    dl.city AS City,
    dtrack.genre AS Genre,
    SUM(fs.unit_price * fs.quantity) AS TotalRevenue
FROM 
    `final-dwh.chinook_olap.fact_sales` fs
JOIN 
    `final-dwh.chinook_olap.DIM_Location` dl 
ON 
    fs.dim_location_id = dl.dim_location_id
JOIN 
    `final-dwh.chinook_olap.DIM_Track` dtrack 
ON 
    fs.dim_track_id = dtrack.dim_track_id
JOIN 
    `final-dwh.chinook_olap.DIM_Time` dt 
ON 
    fs.dim_date_id = dt.dim_time_id
GROUP BY 
    Quarter, Country, City, Genre
ORDER BY 
    TotalRevenue DESC
LIMIT 10;
#Relationship Between Unit Price and Quantity
SELECT 
    fs.unit_price AS UnitPrice,
    SUM(fs.quantity) AS TotalQuantity
FROM 
    `final-dwh.chinook_olap.fact_sales` fs
GROUP BY 
    UnitPrice
ORDER BY 
    UnitPrice;
#Revenue Trends Over Time 
SELECT 
    dt.year AS Year,
    dt.quarter AS Quarter,
    dt.month AS Month,
    SUM(fs.unit_price * fs.quantity) AS TotalRevenue
FROM 
    `final-dwh.chinook_olap.fact_sales` fs
JOIN 
    `final-dwh.chinook_olap.DIM_Time` dt 
ON 
    fs.dim_date_id = dt.dim_time_id
GROUP BY 
    Year, Quarter, Month
ORDER BY 
    Year, Quarter, Month;
#Top Artist
SELECT 
    dt.artist_name AS ArtistName,
    SUM(fs.unit_price * fs.quantity) AS TotalRevenue
FROM 
    `final-dwh.chinook_olap.fact_sales` fs
JOIN 
    `final-dwh.chinook_olap.DIM_Track` dt 
ON 
    fs.dim_track_id = dt.dim_track_id
GROUP BY 
    ArtistName
ORDER BY 
    TotalRevenue DESC
LIMIT 1;
#Top Location Contributing to Revenue
SELECT 
    dl.country AS Country,
    dl.city AS City,
    SUM(fs.unit_price * fs.quantity) AS TotalRevenue
FROM 
    `final-dwh.chinook_olap.fact_sales` fs
JOIN 
    `final-dwh.chinook_olap.DIM_Location` dl 
ON 
    fs.dim_location_id = dl.dim_location_id
GROUP BY 
    Country, City
ORDER BY 
    TotalRevenue DESC
LIMIT 1;
#Top Revenue-Generating Tracks by Location
SELECT 
    dl.country AS Country,
    dl.city AS City,
    dt.track_name AS TrackName,
    SUM(fs.unit_price * fs.quantity) AS TotalRevenue
FROM 
    `final-dwh.chinook_olap.fact_sales` fs
JOIN 
    `final-dwh.chinook_olap.DIM_Location` dl 
ON 
    fs.dim_location_id = dl.dim_location_id
JOIN 
    `final-dwh.chinook_olap.DIM_Track` dt 
ON 
    fs.dim_track_id = dt.dim_track_id
GROUP BY 
    Country, City, TrackName
ORDER BY 
    TotalRevenue DESC
LIMIT 10;

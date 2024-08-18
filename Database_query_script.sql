-- 1Number of stores by country

SELECT country_code AS country, COUNT(*) AS total_no_stores
FROM dim_store_details
GROUP BY country_code
ORDER BY total_no_stores DESC;

-- 2Location with most stores

SELECT dsd.locality, count(*) as total_no_stores
FROM dim_store_details dsd
GROUP BY locality
ORDER BY count(*) desc
limit 10;

--3Months produce the most sales overall time of records
SELECT dt.month,ROUND(SUM(o.product_quantity * REGEXP_REPLACE(p.product_price, '[^0-9\.]', '', 'g')::numeric), 2) AS total_revenue
FROM orders_table o
JOIN dim_date_times dt ON o.date_uuid = dt.date_uuid
JOIN dim_products p ON o.product_code = p.product_code
GROUP BY dt.month
ORDER BY total_revenue DESC;

--4Number of sales coming online
SELECT COUNT(dp.product_code) AS number_of_sales, 
SUM(ot.product_quantity) AS product_quantity_count, 
    CASE
        WHEN dsd.store_type IN ('Super Store', 'Local', 'Outlet', 'Mall Kiosk') THEN 'Offline'
        ELSE 'Web'
    END AS location 
FROM dim_products dp
JOIN orders_table ot ON ot.product_code = dp.product_code
JOIN dim_store_details dsd ON dsd.store_code = ot.store_code
GROUP BY location
ORDER BY number_of_sales ASC;

--5 % of sales comes through each type of store (check erroe of GBP symbol later)

WITH CleanedSales AS (
    SELECT 
        ot.store_code,
        dp.product_price,
        ot.product_quantity * 
        CAST(REPLACE(dp.product_price, '£', '') AS NUMERIC) AS sale_amount
    FROM orders_table ot
    JOIN dim_products dp ON ot.product_code = dp.product_code
), TotalSales AS (
    SELECT 
        ds.store_type,
        ROUND(SUM(cs.sale_amount), 2) AS total_sales
    FROM CleanedSales cs
    JOIN dim_store_details ds ON cs.store_code = ds.store_code
    GROUP BY ds.store_type
), OverallSales AS (
    SELECT 
        ROUND(SUM(sale_amount), 2) AS total
    FROM CleanedSales
)

SELECT 
    ts.store_type,
    ts.total_sales,
    ROUND((ts.total_sales / os.total * 100), 2) AS percentage_total
FROM TotalSales ts, OverallSales os
ORDER BY ts.total_sales DESC;


--6 Month in each year produced highest sales 
SELECT 
    ddt.year,
    ddt.month,
    ROUND(SUM(ot.product_quantity * CAST(REPLACE(dp.product_price, '£', '') AS NUMERIC)), 2) AS total_sales
FROM orders_table ot
JOIN dim_date_times ddt ON ot.date_uuid = ddt.date_uuid
JOIN dim_products dp ON ot.product_code = dp.product_code
GROUP BY ddt.year, ddt.month
ORDER BY total_sales DESC;

--7 Staff headcount

SELECT country_code,
    SUM(staff_numbers) AS total_staff_numbers
FROM dim_store_details
GROUP BY country_code
ORDER BY total_staff_numbers DESC;

--8 German store type is selling the most

SELECT 
    SUM(ot.product_quantity * CAST(REPLACE(dp.product_price, '£', '') AS NUMERIC)) AS total_sales,
    ds.country_code,
    ds.store_type
FROM orders_table ot
JOIN dim_products dp ON ot.product_code = dp.product_code
JOIN dim_store_details ds ON ot.store_code = ds.store_code
WHERE ds.country_code = 'DE'
GROUP BY ds.store_type, ds.country_code
ORDER BY total_sales;

--9 timing of the sales in terms of 'Quickness'

WITH purchase_time_gap AS (
    SELECT 
        timestamp::time AS timestamp,
        LEAD(timestamp::time) OVER (ORDER BY timestamp::time) AS next_purchase_time,
        year
    FROM dim_date_times
)

SELECT 
    year,
    AVG(EXTRACT(EPOCH FROM (next_purchase_time - timestamp))) AS average_time_taken_seconds
FROM purchase_time_gap
WHERE next_purchase_time IS NOT NULL
GROUP BY year
ORDER BY average_time_taken_seconds DESC
LIMIT 5;





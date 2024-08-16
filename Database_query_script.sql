-- Number of stores by country

SELECT country_code AS country, COUNT(*) AS total_no_stores
FROM dim_store_details
GROUP BY country_code
ORDER BY total_no_stores DESC;

-- Location with most stores

SELECT dsd.locality, count(*) as total_no_stores
FROM dim_store_details dsd
GROUP BY locality
ORDER BY count(*) desc
limit 10;


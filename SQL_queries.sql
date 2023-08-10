-- 1. no. of stores by country
SELECT country_code AS country, COUNT(store_code) AS total_no_stores
FROM dim_store_details
GROUP BY country_code
ORDER BY total_no_stores DESC;

-- 2. no. of stores by location
SELECT locality, COUNT(store_code) AS total_no_stores
FROM dim_store_details
GROUP BY locality
ORDER BY total_no_stores DESC;

-- 3. months with most sales
SELECT DATE_PART('month', date) AS month, COUNT(date) AS total_sales
FROM dim_date_times
GROUP BY month
ORDER BY total_sales DESC;

-- 4. online vs offline sales
SELECT COUNT(store_code) AS numbers_of_sales, SUM(product_quantity) as product_quantity_count, 
CASE
    WHEN store_code = 'WEB-1388012W' THEN 'Web'
ELSE 'Offline'
END AS location
FROM orders_table
GROUP BY location;

-- 5. % by store type
SELECT  store_type, 
        COUNT(store_type) AS total_sales, 
        COUNT(store_type) * 100 / 
            (SELECT COUNT(*) FROM orders_table) AS "percentage_total(%)" 
            -- sub-query above executes before the GROUP BY
FROM orders_table
JOIN dim_store_details
    ON orders_table.store_code = dim_store_details.store_code
GROUP BY store_type
ORDER BY "percentage_total(%)" DESC;

-- 6. month & year with highest sales
SELECT DATE_PART('year', date) AS year, DATE_PART('month', date) AS month, COUNT(date) AS total_sales
FROM dim_date_times
GROUP BY year, month
ORDER BY total_sales DESC;

-- 7. staff headcount by country
SELECT SUM(staff_numbers) AS total_staff_numbers, country_code
FROM dim_store_details
GROUP BY country_code
ORDER BY total_staff_numbers DESC;

-- 8. sales by German store type
SELECT  country_code, 
        store_type, 
        COUNT(store_type) AS total_sales
FROM orders_table
JOIN dim_store_details
    ON orders_table.store_code = dim_store_details.store_code
WHERE country_code = 'DE'
GROUP BY country_code, store_type;

-- 9. average time to sell
SELECT  DATE_PART('year', date) AS year, 
        CONCAT(
                24 * 365 / COUNT(date), 
                ' hours, ', 
                (60 * 24 * 365 / COUNT(date)) % 60, 
                ' minutes, ', 
                (3600 * 24 * 365 / COUNT(date)) % 60,
                ' seconds'
                ) AS actual_time_taken_per_sale
FROM dim_date_times
GROUP BY year
ORDER BY actual_time_taken_per_sale DESC;

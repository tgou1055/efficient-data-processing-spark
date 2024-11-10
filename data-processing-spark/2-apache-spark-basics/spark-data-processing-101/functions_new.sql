/*
SQL in-build functions
*/

-- String operations

SELECT LENGTH('hi')

USE tpch;

SELECT 
    CONCAT(clerk, '-', orderpriority) 
FROM
    orders
LIMIT 
    5;

SELECT
    SPLIT(clerk, '#') 
FROM 
    orders 
LIMIT 
    5;

-- SUBSTR (Extracts the first 5 characters from the clerk column.)
SELECT
    clerk,
    SUBSTR(clerk,1,5)
FROM
    orders
LIMIT
    5;

SELECT TRIM(' hi ');

-- DATE operations
SELECT
    DATEDIFF('2022-11-05', '2022-10-01') diff_in_days,
    MONTHS_BETWEEN('2022-11-05', '2022-10-01') diff_in_months,
    MONTHS_BETWEEN('2022-11-05', '2022-10-01') / 12  diff_in_years,
    DATE_ADD('2022-11-05', 10) AS new_date;

SELECT 
    DATE '2022-11-05' + INTERVAL '10' DAY;

SELECT 
    TO_DATE('11-05-2023', 'MM-dd-yyyy') AS parse_date, 
    TO_TIMESTAMP('11-05-2023', 'MM-dd-yyyy') AS parse_ts;

SELECT 
    DATE_FORMAT(orderdate, 'yyyy-MM-01') AS first_month_date
FROM
    orders
LIMIT 5;
-- This query will return the date as YYYY-MM-01, 
-- effectively giving the first day of each month based on the orderdate.

SELECT
    YEAR(DATE '2024-11-10');

-- NUMBER operations
SELECT
    ROUND(100.102345, 2);

SELECT 
    ABS(-100), ABS(100);

SELECT
    CEIL(100.1), FLOOR(100.1);
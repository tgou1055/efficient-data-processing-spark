USE tpch;

/*
The following is supposed to fail, but spark
makes it work and generate a NULL!
(Not what we expect)
*/

SELECT DATEDIFF('2024-11-10', 'invalid_date');

SELECT
    DATEDIFF(
        CAST('2024-11-10' AS DATE),
        CAST('invalid_date' AS DATE)
    );

/*
-- Notes:
In Spark SQL, if you use DATEDIFF with an invalid date, it returns NULL because Spark 
handles invalid or unparseable dates by producing a NULL value rather than throwing an error. 
This behavior is in line with SQL's handling of invalid data, where operations involving NULL values 
result in NULL to ensure the query doesn't break due to parsing issues.
*/

USE tpch;

/* Analytic query:
    This query identifies orders that do not have any associated line items within a ±5-day window 
    of the order date, potentially flagging orders that might have missing, delayed, or 
    incorrect line items (and order fulfillment)
*/
SELECT
    o.orderkey, 
    o.orderdate,
    COALESCE(l.orderkey, 9999999) AS lineitem_orderkey,
    o.orderdate,
    l.shipdate
FROM
    orders o
    LEFT JOIN lineitem l ON o.orderkey = l.orderkey
    AND o.orderdate BETWEEN l.shipdate - INTERVAL '5' DAY
    AND l.shipdate + INTERVAL '5' DAY
WHERE
    l.shipdate IS NULL
LIMIT
    20;


/*
In a database or data warehousing context, a "lineitem" typically refers to a row or entry 
in a table that represents a single item or detail line within a larger transaction or order. 
This term is often used in databases modeling transactional or business data, particularly 
in contexts like retail, sales, or order management systems. 

Each line item typically includes details about individual products or services associated with 
an order or invoice.
*/




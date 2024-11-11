/*
GROUP BY 
*/

-- COUNT number of orders for each order_priority level

USE tpch;

SELECT
    orderpriority,
    count(*) AS number_orders
FROM 
    orders
GROUP BY
    orderpriority;

    -- Numbers may vary slightly
/*
 5-LOW   300589
 3-MEDIUM        298723
 1-URGENT        300343
 4-NOT SPECIFIED 300254
 2-HIGH  300091
 */
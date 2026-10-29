CREATE OR REPLACE TABLE dbacademy.get_started_de.daily_sales
USING DELTA AS
SELECT
    CAST(o.order_purchase_timestamp AS DATE) AS sales_date,
    COUNT(DISTINCT o.order_id) AS order_count,
    COUNT(*) AS item_count,
    ROUND(SUM(i.price), 2) AS merchandise_revenue,
    ROUND(SUM(i.freight_value), 2) AS freight_revenue
FROM dbacademy.get_started_de.orders o
JOIN dbacademy.get_started_de.order_items i ON o.order_id = i.order_id
WHERE o.order_purchase_timestamp IS NOT NULL
GROUP BY CAST(o.order_purchase_timestamp AS DATE);

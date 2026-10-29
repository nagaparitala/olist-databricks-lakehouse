CREATE OR REPLACE TABLE dbacademy.get_started_de.category_sales
USING DELTA AS
SELECT
    p.product_category_name,
    COUNT(DISTINCT i.order_id) AS order_count,
    COUNT(*) AS item_count,
    ROUND(SUM(i.price), 2) AS merchandise_revenue
FROM dbacademy.get_started_de.order_items i
JOIN dbacademy.get_started_de.products p ON i.product_id = p.product_id
GROUP BY p.product_category_name;

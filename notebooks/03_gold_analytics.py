# Databricks notebook source
# MAGIC %sql
# MAGIC --Gold Layer
# MAGIC --Support Analytics
# MAGIC --Calculates merchandise and freight revenue from the item table. It does not claim to represent net revenue or total payments, which would require separate busiess rules for cancellations, refunds, and payment records
# MAGIC CREATE OR REPLACE TABLE dbacademy.get_started_de.daily_sales
# MAGIC USING DELTA AS
# MAGIC SELECT
# MAGIC     CAST(o.order_purchase_timestamp AS DATE) AS sales_date,
# MAGIC     COUNT(DISTINCT o.order_id) AS order_count,
# MAGIC     COUNT(*) AS item_count,
# MAGIC     ROUND(SUM(i.price), 2) AS merchandise_revenue,
# MAGIC     ROUND(SUM(i.freight_value), 2) AS freight_revenue
# MAGIC FROM dbacademy.get_started_de.orders o
# MAGIC JOIN dbacademy.get_started_de.order_items i
# MAGIC ON o.order_id = i.order_id
# MAGIC WHERE o.order_purchase_timestamp is NOT NULL
# MAGIC GROUP BY CAST(o.order_purchase_timestamp AS DATE);

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE dbacademy.get_started_de.category_sales
# MAGIC USING DELTA AS
# MAGIC SELECT
# MAGIC     p.product_category_name,
# MAGIC     COUNT(DISTINCT i.order_id) AS order_count,
# MAGIC     COUNT(*) AS item_count,
# MAGIC     ROUND(SUM(i.price), 2) AS merchandise_revenue
# MAGIC FROM dbacademy.get_started_de.order_items i
# MAGIC JOIN dbacademy.get_started_de.products p
# MAGIC     ON i.product_id = p.product_id
# MAGIC GROUP BY p.product_category_name;

# COMMAND ----------

# MAGIC %sql
# MAGIC MERGE INTO dbacademy.get_started_de.orders t
# MAGIC USING dbacademy.get_started_de.orders_updates s
# MAGIC ON t.order_id = s.order_id
# MAGIC WHEN MATCHED THEN
# MAGIC     UPDATE SET *
# MAGIC WHEN NOT MATCHED THEN
# MAGIC     INSERT *;
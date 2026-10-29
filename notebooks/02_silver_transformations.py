# Databricks notebook source
#Clean and validate orders
#Silver Layer is this cell and next couple
#Silver layer is to clean and validate records
from pyspark.sql import functions as F

orders = spark.table("dbacademy.get_started_de.orders_raw")

orders_clean = (
    orders
    .dropDuplicates(["order_id"])
    .withColumn(
        "order_purchase_timestamp",
        F.to_timestamp("order_purchase_timestamp")
    )
    .withColumn(
        "order_delivered_customer_date",
        F.to_timestamp("order_delivered_customer_date")
    )
    .withColumn(
        "order_estimated_delivery_date",
        F.to_timestamp("order_estimated_delivery_date")
    )
    .filter(F.col("order_id").isNotNull())
)
(orders_clean.write
 .format("delta")
 .mode("overwrite")
 .option("overwriteSchema","true")
 .saveAsTable("dbacademy.get_started_de.orders"))

# COMMAND ----------

#Clean order items
items = spark.table("dbacademy.get_started_de.order_items_raw")

items_clean = (
    items
    .dropDuplicates(["order_id", "order_item_id"])
    .withColumn("price", F.col("price").cast("decimal(12,2)"))
    .withColumn("freight_value",
                F.col("freight_value").cast("decimal(12,2)"))
    .filter(F.col("order_id").isNotNull())
    .filter(F.col("price")>=0)
)

(items_clean.write
 .format("delta")
 .mode("overwrite")
 .option("overwriteSchema","true")
 .saveAsTable("dbacademy.get_started_de.order_items"))

# COMMAND ----------

#Clean customers
customers = spark.table("dbacademy.get_started_de.customers_raw")

customers_clean = (
    customers
    .dropDuplicates(["customer_id"])
    .filter(F.col("customer_id").isNotNull())
)
(customers_clean.write
 .format("delta")
 .mode("overwrite")
 .option("overwriteSchema","true")
 .saveAsTable("dbacademy.get_started_de.customers"))

#For production-quality work, don't silently discard bad records. Add a rejected-records table, document your rules, and reconcile source counts against accepted and rejected records. Also, deduplication by key alone is a starting point; for changing records, choose the latest valid version using an update timestamp or defined source ordering
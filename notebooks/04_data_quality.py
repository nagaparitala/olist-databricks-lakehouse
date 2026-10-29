# Databricks notebook source
from pyspark.sql import functions as F

orders = spark.table("dbacademy.get_started_de.orders")
items = spark.table("dbacademy.get_started_de.order_items")

# Check primary-key uniqueness
duplicate_orders = (
    orders.groupBy("order_id")
    .count()
    .filter(F.col("count") > 1)
)

# Check required fields
missing_order_ids = orders.filter(
    F.col("order_id").isNull()
)

# Check referential integrity
orphan_items = items.join(
    orders.select("order_id"),
    on="order_id",
    how="left_anti"
)

assert duplicate_orders.count() == 0, "Duplicate order IDs found"
assert missing_order_ids.count() == 0, "Missing order IDs found"
assert orphan_items.count() == 0, "Order items without matching orders"

print("All data quality checks passed.")
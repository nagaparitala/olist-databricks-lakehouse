# Databricks notebook source
from pyspark.sql import functions as F

CATALOG = "dbacademy"
SCHEMA = "get_started_de"

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.{SCHEMA}")

source_files = {
    "orders_raw": "/Volumes/dbacademy/get_started_de/myfiles/olist_orders_dataset.csv",
    "customers_raw": "/Volumes/dbacademy/get_started_de/myfiles/olist_customers_dataset.csv",
    "order_items_raw": "/Volumes/dbacademy/get_started_de/myfiles/olist_order_items_dataset.csv"
}

for table_name, file_path in source_files.items():

    full_table_name = f"{CATALOG}.{SCHEMA}.{table_name}"

    # Skip tables that already exist
    if spark.catalog.tableExists(full_table_name):
        print(f"Skipping {full_table_name}: already exists")
        continue

    df = (
        spark.read
        .option("header", True)
        .option("inferSchema", True)
        .csv(file_path)
        .withColumn("_ingested_at", F.current_timestamp())
        .withColumn("_source_file", F.col("_metadata.file_path"))
    )

    (
        df.write
        .format("delta")
        .mode("errorifexists")
        .saveAsTable(full_table_name)
    )

    print(f"Loaded {full_table_name}: {df.count()} rows")

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT COUNT(*) FROM dbacademy.get_started_de.orders_raw;
# MAGIC
# MAGIC SELECT * FROM dbacademy.get_started_de.orders_raw;

# COMMAND ----------

from pyspark.sql import functions as F

file_path = "/Volumes/dbacademy/get_started_de/myfiles/olist_products_dataset.csv"

# BRONZE: Read products CSV
products_raw = (
    spark.read
    .option("header", True)
    .option("inferSchema", True)
    .csv(file_path)
    .withColumn("_ingested_at", F.current_timestamp())
    .withColumn("_source_file", F.col("_metadata.file_path"))
)

# Save Bronze table
(products_raw.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable("dbacademy.get_started_de.products_raw"))

# SILVER: Clean products
products_clean = (
    products_raw
    .filter(F.col("product_id").isNotNull())
    .dropDuplicates(["product_id"])
)

# Save Silver table
(products_clean.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable("dbacademy.get_started_de.products"))

print("Products tables created successfully!")

# COMMAND ----------

# MAGIC %sql
# MAGIC --What are the daily order volumes and sales amounts?
# MAGIC --This calculates merchandise and freight revenue from the item table. It does not claim to represent net revenue or total payments, which would require separate business rules for cancellations, refunds, and payment records
# MAGIC CREATE or REPLACE TABLE dbacademy.get_started_de.category_sales
# MAGIC USING DELTA AS
# MAGIC SELECT
# MAGIC     p.product_category_name,
# MAGIC     COUNT(DISTINCT i.order_id) AS order_count,
# MAGIC     COUNT(*) AS item_count,
# MAGIC     ROUND(SUM(i.price),2) AS merchandise_revenue
# MAGIC FROM dbacademy.get_started_de.order_items i
# MAGIC JOIN dbacademy.get_started_de.products p
# MAGIC ON i.product_id = p.product_id
# MAGIC GROUP BY p.product_category_name;

# COMMAND ----------

# MAGIC %sql
# MAGIC --Add the remaining Olist datasets later, particularly payments, reviews, sellers, and the product category translation file. The full dataset makes it possible to investigate delivery performance, customer satisfaction, and payment methods.
# MAGIC CREATE OR REPLACE TABLE dbacademy.get_started_de.category_sales
# MAGIC USING DELTA AS
# MAGIC SELECT
# MAGIC     p.product_category_name,
# MAGIC     COUNT(DISTINCT i.order_id) AS order_count,
# MAGIC     COUNT(*) AS item_count,
# MAGIC     ROUND(SUM(i.price),2) AS merchandise_revenue
# MAGIC FROM dbacademy.get_started_de.order_items i
# MAGIC JOIN dbacademy.get_started_de.products p
# MAGIC ON i.product_id = p.product_id
# MAGIC GROUP BY p.product_category_name;

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE dbacademy.get_started_de.orders_updates
# MAGIC USING DELTA AS
# MAGIC SELECT *
# MAGIC FROM dbacademy.get_started_de.orders
# MAGIC WHERE 1 = 0;

# COMMAND ----------

# MAGIC %sql
# MAGIC MERGE INTO dbacademy.get_started_de.orders AS target
# MAGIC USING dbacademy.get_started_de.orders_updates as source
# MAGIC ON target.order_id = source.order_id
# MAGIC
# MAGIC WHEN MATCHED THEN UPDATE SET *
# MAGIC
# MAGIC WHEN NOT MATCHED THEN INSERT *;

# COMMAND ----------

from pyspark.sql import functions as F

orders = spark.table("dbacademy.get_started_de.orders")
items = spark.table("dbacademy.get_started_de.order_items")

#Check primary-key uniqueness
duplicate_orders = (
    orders.groupBy("order_id")
    .count()
    .filter(F.col("count")>1)
)

#Check required fields
missing_order_ids = orders.filter(
    F.col("order_id").isNull()
)

#Check referential integrity
orphan_items = items.join(
    orders.select("order_id"),
    on="order_id",
    how="left_anti"
)

assert duplicate_orders.count() == 0, "Duplicate order IDs found"
assert missing_order_ids.count() == 0, "Missing order IDs found"
assert orphan_items.count() == 0, "Order items without matching orders"

print("All data quality checks passed.")
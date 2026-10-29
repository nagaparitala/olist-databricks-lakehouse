# Databricks Retail Lakehouse

A Databricks practice project using PySpark, SQL, Delta Lake and the Olist e-commerce dataset. The notebooks are original Databricks source exports.

The observed job ran Bronze → Silver → Data Quality → Gold and succeeded on October 28, 2026 using serverless compute. See [architecture](docs/architecture.md) for dependencies and caveats.

| Path | Purpose |
| --- | --- |
| `notebooks/01_bronze_ingestion.py` | Ingests orders, customers, items and products; also includes exploratory transformations and checks. |
| `notebooks/02_silver_transformations.py` | Cleans and deduplicates orders, order items and customers. |
| `notebooks/04_data_quality.py` | Checks order IDs and referential integrity. |
| `notebooks/03_gold_analytics.py` | Creates daily and category sales tables and demonstrates a merge. |
| `sql/` | Standalone copies of the two Gold aggregation queries. |
| `tests/` | Local structural checks for the quality notebook, not a substitute for running it with Spark. |
| `resources/jobs.yml` | Illustrative job configuration, not an export of the original job. |

## Setup

1. Import the Python files as Databricks notebooks. Their SQL magic cells require Databricks.
2. Obtain the Olist Brazilian e-commerce dataset separately and place `olist_orders_dataset.csv`, `olist_customers_dataset.csv`, `olist_order_items_dataset.csv`, and `olist_products_dataset.csv` in `/Volumes/dbacademy/get_started_de/myfiles/`.
3. Ensure the catalog, schema and volume exist and you have read/write permissions. Update the hard-coded `dbacademy.get_started_de` and volume references for your workspace.
4. On a clean workspace, run the Bronze cells through creation of `products`, then run Silver. The remaining Bronze cells reference Silver tables, create `orders_updates` and run checks. Execute those after Silver, then run Data Quality and Gold. The original Bronze-first job order can fail on a clean workspace because later Bronze cells depend on Silver tables.

The raw tables for orders, customers and items are skipped if already present; products and Silver tables are overwritten. The empty `orders_updates` table is a merge demonstration. Sales sums are not net revenue or total payments. No data, credentials, original job definition or execution logs are included.

Run local checks with `python3 -m unittest discover -s tests`. They do not execute Spark or validate dataset results.

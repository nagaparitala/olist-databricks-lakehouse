# Architecture

```text
Olist CSVs in Unity Catalog volume
  → Bronze Delta tables (orders, customers, items, products)
  → Silver cleaned orders, customers, items and products
  → Data quality assertions
  → Gold daily sales and category sales
```

The original run graph showed Bronze → Silver → Data Quality → Gold. The exported Bronze notebook also creates the clean products table, aggregates category sales using Silver order items, creates an empty orders update table, performs a merge and runs quality checks. A clean run needs the staged bootstrap described in the README.

Silver drops duplicate keys without specifying a deterministic winner, filters invalid records without a rejection table, and overwrites output tables. Quality assertions check the resulting tables rather than reconciling raw inputs. Gold sums do not account for cancellations, refunds or payments. The sample job YAML is reconstructed from the visible graph and must be adapted and validated; it is not the original job definition.

# Databricks notebook source
# MAGIC %md
# MAGIC # Vacuum multiple tables
# MAGIC
# MAGIC With DiscoverX you can run maintenance operations like vacuum over multiple tables at once.
# MAGIC
# MAGIC You can use the wildcard `*` to match any string. Eg. `prod_*.*.*gold*` will match any table containing the word `gold` in a catalog that starts with `prod_`.

# COMMAND ----------

# MAGIC %pip install dbl-discoverx

# COMMAND ----------

dbutils.widgets.text("from_tables", "*.*.*")
from_tables = dbutils.widgets.get("from_tables")

# COMMAND ----------

from discoverx import DX

dx = DX()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Explain the commands that will be executed

# COMMAND ----------

dx.from_tables(from_tables)\
  .apply_sql("VACUUM {full_table_name}")\
  .explain()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Run VACUUM

# COMMAND ----------

(dx.from_tables(from_tables)
  .apply_sql("VACUUM {full_table_name}")
  .execute()
)

# COMMAND ----------

df = (dx.from_tables("sample_data_discoverx.*.*")
  .having_columns("email")
  .with_sql("OPTIMIZE {full_table_name} ZORDER BY (email)")
  .execute()
)

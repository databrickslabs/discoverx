# Databricks notebook source
# MAGIC %md
# MAGIC # Run arbitrary operations across multiple tables
# MAGIC

# COMMAND ----------

# TODO: update the table mathching pattern:
from_tables = "discoverx_sample.*.*"

# COMMAND ----------

from discoverx import DX

dx = DX()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Define a custom function

# COMMAND ----------

from discoverx.table_info import TableInfo


def my_function(table_info: TableInfo):

    # TODO: Add your logic here
    print(f"{table_info.catalog}.{table_info.schema}.{table_info.table} has {len(table_info.columns)} columns")

    if table_info.tags:
        print(f" - catalog_tags: {table_info.tags.catalog_tags}")
        print(f" - schema_tags: {table_info.tags.schema_tags}")
        print(f" - table_tags: {table_info.tags.table_tags}")
        print(f" - column_tags: {table_info.tags.column_tags}")

    # The returned objects will be concatenated in the map result
    return table_info


# COMMAND ----------

# MAGIC %md
# MAGIC ## Run the custom fuctions for multiple tables

# COMMAND ----------

result = (
    dx.from_tables(from_tables)
    .with_concurrency(1)  # You can increase the concurrency with this parameter
    .map(my_function)
)

# COMMAND ----------

len(result)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Use tags

# COMMAND ----------

result = (
    dx.from_tables(from_tables)
    .with_concurrency(1)  # You can increase the concurrency with this parameter
    .with_tags()  # This will ensure we add tag information to the table info
    .map(my_function)
)

# COMMAND ----------

len(result)

# COMMAND ----------

# Databricks notebook source
# MAGIC %md
# MAGIC # Detect tables with too many small files
# MAGIC
# MAGIC Delta tables are composed of multiple `parquet` files. A table with too many small files might lead to performance degradation. The optimal file size depends on the workload, but it generally ranges between `10 MB` and `1000 MB`.
# MAGIC
# MAGIC As a rule of thumb, if a table has more than `100` files and average file size smaller than `10 MB`, then we can consider it having too many small files.
# MAGIC
# MAGIC Some common causes of too many small files are:
# MAGIC * Overpartitioning: the cardinality of the partition columns is too high
# MAGIC * Lack of scheduled maintenance operations like `OPTIMIZE`
# MAGIC * Missing auto optimize on write
# MAGIC
# MAGIC This notebook will help you to identify the tables that might require a review.

# COMMAND ----------

# MAGIC %pip install dbl-discoverx

# COMMAND ----------

dbutils.widgets.text("from_tables", "*.*.*")
from_tables = dbutils.widgets.get("from_tables")

# Define how small is too small
small_file_max_size_MB = 10

# It's okay to have small files as long as there are not too many
min_file_number = 100

# COMMAND ----------

from discoverx import DX

dx = DX()

# COMMAND ----------

from pyspark.sql.functions import col, lit

dx.from_tables(from_tables).with_sql("DESCRIBE DETAIL {full_table_name}").apply().withColumn(
    "average_file_size_MB", col("sizeInBytes") / col("numFiles") / 1024 / 1024
).withColumn(
    "has_too_many_small_files",
    (col("average_file_size_MB") < small_file_max_size_MB) & (col("numFiles") > min_file_number),
).filter(
    "has_too_many_small_files"
).display()

# COMMAND ----------

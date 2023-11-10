# Databricks notebook source
# MAGIC %pip install dbl-discoverx

# COMMAND ----------

dbutils.widgets.text("from_tables", "sample_data_discoverx.*.*")
from_tables = dbutils.widgets.get("from_tables")


# COMMAND ----------

from discoverx import DX

dx = DX()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Which are the biggest 10 tables in the "sample_data_discoverx" catalog?
# MAGIC
# MAGIC

# COMMAND ----------

from pyspark.sql.functions import col

(dx
   .from_tables("sample_data_discoverx.*.*")
   .with_sql("DESCRIBE DETAIL {full_table_name}")
   .apply()
   .orderBy(col("sizeInBytes").desc())
   .display()
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Which tables have the most daily transactions?

# COMMAND ----------

from pyspark.sql.functions import window

(dx
  .from_tables("sample_data_discoverx.*.*")
  .with_sql("DESCRIBE HISTORY {full_table_name}")
  .apply()
  .groupBy("table_catalog", "table_schema", "table_name", window("timestamp", "1 day"))
  .count()
  .display()
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Which tables have too many small files?

# COMMAND ----------

from pyspark.sql.functions import col, lit

(dx
   .from_tables("sample_data_discoverx.*.*")
   .with_sql("DESCRIBE DETAIL {full_table_name}")
   .apply()
   .withColumn("average_file_size", col("sizeInBytes") / col("numFiles"))
   .withColumn("has_many_small_files", 
               (col("average_file_size") < 10000000) & (col("numFiles") > 100))
   .orderBy("average_file_size")
   .display()
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Which tables contain email addresses?

# COMMAND ----------

result = (dx
  .from_tables("sample_data_discoverx.*.*")
  .scan()
)

# COMMAND ----------

result.search("erni@databricks.com").display()

# COMMAND ----------



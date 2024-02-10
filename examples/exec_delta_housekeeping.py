# Databricks notebook source
# MAGIC %md
# MAGIC # Run Delta Housekeeping across multiple tables
# MAGIC Analysis that provides stats on Delta tables / recommendations for improvements, including:
# MAGIC - stats:size of tables and number of files, timestamps of latest OPTIMIZE & VACUUM operations, stats of OPTIMIZE)
# MAGIC - recommendations on tables that need to be OPTIMIZED/VACUUM'ed
# MAGIC - are tables OPTIMIZED/VACUUM'ed often enough
# MAGIC - tables that have small files / tables for which ZORDER is not being effective
# MAGIC

# COMMAND ----------

# MAGIC %pip install dbl-discoverx

# COMMAND ----------

# MAGIC %md
# MAGIC ### Declare Variables

# COMMAND ----------

dbutils.widgets.text("catalogs", "*", "Catalogs")
dbutils.widgets.text("schemas", "*", "Schemas")
dbutils.widgets.text("tables", "*", "Tables")

# COMMAND ----------

catalogs = dbutils.widgets.get("catalogs")
schemas = dbutils.widgets.get("schemas")
tables = dbutils.widgets.get("tables")
from_table_statement = ".".join([catalogs, schemas, tables])

# COMMAND ----------

from discoverx import DX

dx = DX()

# COMMAND ----------

# DBTITLE 1,Run the discoverx DeltaHousekeeping operation -generates an output object on which you can run operations
output = (
  dx.from_tables(from_table_statement)
  .delta_housekeeping()
)

# COMMAND ----------

# DBTITLE 1,apply() operation generates a spark dataframe with recommendations
result = output.apply()
result.select("catalog", "database", "tableName", "rec_optimize", "rec_optimize_reason", "rec_vacuum", "rec_vacuum_reason", "rec_misc", "rec_misc_reason").display()

# COMMAND ----------

# DBTITLE 1,display() runs apply and displays the full result (including stats per table)
output.display()

# COMMAND ----------

# DBTITLE 1,explain() outputs the DeltaHousekeeping recommendations in HTML format
output.explain()

# COMMAND ----------



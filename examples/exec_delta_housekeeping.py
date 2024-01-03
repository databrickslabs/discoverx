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

from discoverx import DX

dx = DX()

# COMMAND ----------

# DBTITLE 1,Run the discoverx DeltaHousekeeping operation -generates an output object you can apply operations to
output = (
  dx.from_tables("lorenzorubi.*.*")
  .delta_housekeeping()
)

# COMMAND ----------

# DBTITLE 1,Display the stats per table
stats = output.stats()
stats.display()

# COMMAND ----------

# DBTITLE 1,apply() operation generates a list of dictionaries (if you need to postprocess the output)
result = output.apply()
result.display()

# COMMAND ----------

# DBTITLE 1,display() runs apply and displays the result
output.display()

# COMMAND ----------

# DBTITLE 1,explain() outputs the DeltaHousekeeping recommendations in HTML format
output.explain()

# COMMAND ----------


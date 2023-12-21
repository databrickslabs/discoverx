# Databricks notebook source
# MAGIC %md
# MAGIC # Run Delta Housekeeping across multiple tables
# MAGIC

# COMMAND ----------

# TODO remove
%reload_ext autoreload
%autoreload 2

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

# DBTITLE 1,Generate a pandas dataframe with stats per table
display(output.stats())

# COMMAND ----------

# DBTITLE 1,apply() operation generates a list of dictionaries (if you need to postprocess the output)
result = output.apply()

# COMMAND ----------

for r in result:
  print(list(r.keys())[0])
  display(list(r.values())[0])

# COMMAND ----------

# DBTITLE 1,to_html() outputs the DeltaHousekeeping recommendations
displayHTML(output.to_html())

# COMMAND ----------



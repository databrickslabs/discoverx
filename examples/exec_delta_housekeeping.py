# Databricks notebook source
# MAGIC %md
# MAGIC # Run arbitrary operations across multiple tables
# MAGIC

# COMMAND ----------

# TODO remove
# MAGIC %reload_ext autoreload
# MAGIC %autoreload 2

# COMMAND ----------


from discoverx import DX

dx = DX()

# COMMAND ----------

result = (
    dx.from_tables("lorenzorubi.*.*")
    .delta_housekeeping()
    .stats()
)
display(result)

# COMMAND ----------

result = (
    dx.from_tables("lorenzorubi.*.*")
    .delta_housekeeping()
    .apply()
)

# COMMAND ----------

result = (
    dx.from_tables("lorenzorubi.*.*")
    .delta_housekeeping()
    .html()
)

displayHTML(result)

# COMMAND ----------



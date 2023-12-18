# Databricks notebook source
# MAGIC %md
# MAGIC # Run arbitrary operations across multiple tables
# MAGIC

# COMMAND ----------

# MAGIC %reload_ext autoreload
# MAGIC %autoreload 2

# COMMAND ----------


from discoverx import DX

dx = DX()

# COMMAND ----------

result = (
    dx.from_tables("lorenzorubi.*.*")
    .with_concurrency(1)  # You can increase the concurrency with this parameter
    .delta_housekeeping()
)
print(len(result))

# COMMAND ----------



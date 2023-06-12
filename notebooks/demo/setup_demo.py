# Databricks notebook source
# MAGIC %md
# MAGIC ## Setup Demo

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Clean up old demos
# MAGIC DROP TABLE IF EXISTS _discoverx.classification.classes;

# COMMAND ----------

# Generate sample data
dbutils.notebook.run("./sample_data", timeout_seconds=0, arguments={"discoverx_sample_catalog": "discoverx_sample"} )

# COMMAND ----------



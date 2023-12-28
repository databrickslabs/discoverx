# Databricks notebook source
# MAGIC %md
# MAGIC # Scan Tables with User Specified Data Source Formats

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

# MAGIC %md
# MAGIC ### Initiaize discoverx

# COMMAND ----------

from discoverx import DX

dx = DX()

# COMMAND ----------

# MAGIC %md 
# MAGIC ### DiscoverX will scan all delta tables by default

# COMMAND ----------

dx.from_tables(from_table_statement).scan()

# COMMAND ----------

# MAGIC %md 
# MAGIC ### User can specify data source formats as follows

# COMMAND ----------

(dx.from_tables(from_table_statement)
.with_data_source_formats(["DELTA","JSON"])
.scan())

# COMMAND ----------



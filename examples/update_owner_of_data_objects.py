# Databricks notebook source
# MAGIC %md
# MAGIC #Update Owner of Data Objects

# COMMAND ----------

# MAGIC %md
# MAGIC ### Install discoverx lib

# COMMAND ----------

# %pip install dbl-discoverx
# dbutils.library.restartPython()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Declare Variables

# COMMAND ----------

dbutils.widgets.text("catalogs", "*", "Catalogs")
dbutils.widgets.text("schemas", "*", "Schemas")
dbutils.widgets.text("tables", "*", "Tables")
dbutils.widgets.text("owner","sourav.gulati@databricks.com","owner")
dbutils.widgets.dropdown("if_update_catalog_owner", "YES", ["YES","NO"])
dbutils.widgets.dropdown("if_update_schema_owner", "YES", ["YES","NO"])

# COMMAND ----------

catalogs = dbutils.widgets.get("catalogs")
schemas = dbutils.widgets.get("schemas")
tables = dbutils.widgets.get("tables")
owner = dbutils.widgets.get("owner")
if_update_catalog_owner = dbutils.widgets.get("if_update_catalog_owner")
if_update_schema_owner = dbutils.widgets.get("if_update_schema_owner")
from_table_statement = ".".join([catalogs, schemas, tables])

# COMMAND ----------

# MAGIC %md
# MAGIC ### Initiaize discoverx

# COMMAND ----------

from discoverx import DX

dx = DX()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Update Owner of data objects to user specified value

# COMMAND ----------

def update_owner(table_info):
  catalog_owner_alter_sql = f""" ALTER CATALOG `{table_info.catalog}` SET OWNER TO `{owner}`"""
  schema_owner_alter_sql = f""" ALTER SCHEMA `{table_info.catalog}`.`{table_info.schema}` SET OWNER TO `{owner}`"""
  table_owner_alter_sql = f""" ALTER TABLE `{table_info.catalog}`.`{table_info.schema}`.`{table_info.table}` SET OWNER TO `{owner}`"""
  try:
    if(if_update_catalog_owner == 'YES'):
      print(f"Executing {catalog_owner_alter_sql}")
      spark.sql(catalog_owner_alter_sql)
      
    if(if_update_schema_owner == 'YES'):
      print(f"Executing {schema_owner_alter_sql}")
      spark.sql(schema_owner_alter_sql)

    print(f"Executing {table_owner_alter_sql}")
    spark.sql(table_owner_alter_sql)
  except Exception as exception: 
    print(f"  Exception occurred while updating owner: {exception}")

# COMMAND ----------

dx.from_tables(from_table_statement).map(update_owner)

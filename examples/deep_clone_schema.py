# Databricks notebook source
# MAGIC %md
# MAGIC # Deep Clone a Schema
# MAGIC
# MAGIC Databricks' Deep Clone functionality enables the effortless creation of a data replica with minimal coding and maintenance overhead. Using the `CLONE` command, you can efficiently generate a duplicate of an existing Delta Lake table on Databricks at a designated version. The cloning process is incremental, ensuring that only new changes since the last clone are applied to the table.
# MAGIC
# MAGIC
# MAGIC Deep cloning is applied on a per-table basis, requiring a separate invocation for each table within your schema. In scenarios where automation is desirable, such as when dealing with shared schemas through Delta sharing, replicating the entire schema can be achieved using DiscoverX. This approach eliminates the need to manually inspect and modify your code each time a new table is added to the schema by the provider.
# MAGIC
# MAGIC This notebook serves as an example of utilizing DiscoverX to automate the replication of a schema using Delta Deep Clone.
# MAGIC
# MAGIC Our recommendation is to schedule this notebook as a job at the recipient side.
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ### Specify a source and a destination catalog

# COMMAND ----------

dbutils.widgets.text("1.source_catalog", "_discoverx_deep_clone")
dbutils.widgets.text("2.destination_catalog", "_discoverx_deep_clone_replica")

source_catalog = dbutils.widgets.get("1.source_catalog")
destination_catalog = dbutils.widgets.get("2.destination_catalog")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Use DiscoverX for cloning

# COMMAND ----------

# %pip install dbl-discoverx==0.0.8
# dbutils.library.restartPython()

# COMMAND ----------

from discoverx import DX

dx = DX()

# COMMAND ----------

spark.sql(f"CREATE CATALOG IF NOT EXISTS {destination_catalog}")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Define a function for cloning

# COMMAND ----------


def clone_tables(table_info):
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {destination_catalog}.{table_info.schema}")
    try:
        spark.sql(
            f"""CREATE OR REPLACE TABLE 
    {destination_catalog}.{table_info.schema}.{table_info.table} 
    CLONE {table_info.catalog}.{table_info.schema}.{table_info.table}
    """
        )
        result = {
            "source": f"{table_info.catalog}.{table_info.schema}.{table_info.table}",
            "destination": f"{destination_catalog}.{table_info.schema}.{table_info.table}",
            "success": True,
            "info": None,
        }
    # Cloning Views is not supported
    except Exception as error:
        result = {
            "source": f"{table_info.catalog}.{table_info.schema}.{table_info.table}",
            "destination": f"{destination_catalog}.{table_info.schema}.{table_info.table}",
            "success": False,
            "info": error,
        }
    return result


# COMMAND ----------

# MAGIC %md
# MAGIC ## Apply the custom fuctions for multiple tables
# MAGIC

# COMMAND ----------

res = dx.from_tables(f"{source_catalog}.*.*").map(clone_tables)

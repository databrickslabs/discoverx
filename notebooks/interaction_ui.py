# Databricks notebook source
# MAGIC %md
# MAGIC # DiscoverX
# MAGIC This notebook demonstrates DiscoverX by Databricks Labs.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Setup Demo

# COMMAND ----------

# MAGIC %pip install pydantic
# MAGIC %pip install ipydatagrid

# COMMAND ----------

# MAGIC %load_ext autoreload
# MAGIC %autoreload 2

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Clean up old demos
# MAGIC DROP TABLE IF EXISTS _discoverx.classification.classes;

# COMMAND ----------

# Generate sample data
dbutils.notebook.run("./sample_data", timeout_seconds=0, arguments={"discoverx_sample_catalog": "discoverx_sample"} )

# COMMAND ----------

# MAGIC %md
# MAGIC ## DiscoverX Interaction
# MAGIC
# MAGIC In the following we demonstrate how to interact with DiscoverX.

# COMMAND ----------

from discoverx import DX
dx = DX(locale="US")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Scan
# MAGIC This section demonstrates a typical DiscoverX workflow which consists of the following steps:
# MAGIC - `dx.scan()`: Scan the lakehouse including catalogs with names starting with `discoverx`
# MAGIC - `dx.inspect()`: Inspect and manually adjust the scan result using the DiscoverX inspection tool
# MAGIC - `dx.publish()`: Publish the classification result which save/merge the result to a system table maintained by DiscoverX
# MAGIC - `dx.search()`: Search your across your previously classified lakehouse for specific records or general classifications/classes

# COMMAND ----------

dx.scan(from_tables="discoverx*.*.*")

# COMMAND ----------

dx.publish()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Search
# MAGIC
# MAGIC This command can be used to search inside the content of tables.
# MAGIC
# MAGIC If the tables have ben scanned before, the search will restrict the scope to only the columns that could contain the search term based on the avaialble rules.

# COMMAND ----------

dx.search(search_term='erni@databricks.com', from_tables="*.*.*").display()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Select by class

# COMMAND ----------

dx.select_by_classes(from_tables="discoverx*.*.*", by_classes=["email"]).display()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Select by class with aggregations
# MAGIC
# MAGIC The select output can be aggregated like a normal Spark dataframe.

# COMMAND ----------

import pyspark.sql.functions as func

# Count the occurrences of each IP address per table per IP column
(dx
  .select_by_classes(from_tables="discoverx*.*.*", by_classes=["ip_v4"])
  .groupby(["catalog", "schema", "table", "classified_columns.ip_v4.column", "classified_columns.ip_v4.value"])
  .agg(func.count("classified_columns.ip_v4.value").alias("count"))  
).display()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Deletes - GDPR - Right To Be Forgotten

# COMMAND ----------

# MAGIC %md
# MAGIC ### Delete from all tables - WHAT_IF

# COMMAND ----------

dx.delete_by_class(
  from_tables="discoverx*.*.*", 
  by_class="email", 
  values=['erni@databricks.com'], 
  yes_i_am_sure=False
)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Delete from all tables

# COMMAND ----------

dx.delete_by_class(from_tables="discoverx*.*.*", by_class="email", values=['erni@databricks.com'], yes_i_am_sure=True).display()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Configuration
# MAGIC
# MAGIC This section is optional, and can be used to customize the behaviour of DiscoverX

# COMMAND ----------

# You can change the classificaiton threshold with
dx = DX(classification_threshold=0.95)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Custom rules

# COMMAND ----------

dx.display_rules()

# COMMAND ----------

from discoverx.rules import RegexRule

contains_confidential = RegexRule(
  name='contains_confidential',
  description='Contains the words "confidential" or "restricted" (case insensitive)',
  definition=r'^(?i).*(confidential|restricted).*$',
  match_example=['Some confidential information', 'this is restricted to...', 'Confidential data'],
  nomatch_example=['Any other text']
)

dx = DX(custom_rules=[contains_confidential])

dx.scan(from_tables="*.*.*document*", sample_size=1000, rules="contains_confidential")


# COMMAND ----------

dx.scan(from_tables="*.*.*document*", sample_size=1000, rules="contains_confidential")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Help

# COMMAND ----------

help(DX)

# COMMAND ----------



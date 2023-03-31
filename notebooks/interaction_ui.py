# Databricks notebook source
# MAGIC %md
# MAGIC # DiscoverX interaction

# COMMAND ----------

# MAGIC %pip install pydantic
# MAGIC %pip install ipydatagrid

# COMMAND ----------

# MAGIC %load_ext autoreload
# MAGIC %autoreload 2

# COMMAND ----------

# DBTITLE 1,Clean Up Old Demos
# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS _discoverx.classification.tags;
# MAGIC ALTER TABLE discoverx_sample_dt.sample_datasets.cyber_data ALTER COLUMN ip_v6_address UNSET TAGS ('dx_ip_v6');
# MAGIC ALTER TABLE discoverx_sample_dt.sample_datasets.cyber_data ALTER COLUMN ip_v4_address UNSET TAGS ('dx_ip_v4');
# MAGIC ALTER TABLE discoverx_sample_dt.sample_datasets.cyber_data_2 ALTER COLUMN source_address UNSET TAGS ('dx_ip_v4');
# MAGIC ALTER TABLE discoverx_sample_dt.sample_datasets.cyber_data_2 ALTER COLUMN destination_address UNSET TAGS ('dx_ip_v4');
# MAGIC ALTER TABLE discoverx_sample_dt.sample_datasets.cyber_data_2 ALTER COLUMN content UNSET TAGS ('dx_ip_v6');

# COMMAND ----------

# MAGIC %md
# MAGIC ## Generate sample data

# COMMAND ----------

dbutils.notebook.run("./sample_data", timeout_seconds=0, arguments={"discoverx_sample_catalog": "discoverx_sample_dt"} )

# COMMAND ----------

# MAGIC %md
# MAGIC ## Plain functions flow
# MAGIC 
# MAGIC This is a demo interaction with pure functions

# COMMAND ----------

from discoverx import DX
dx = DX()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Scan
# MAGIC This section demonstrates a typical DiscoverX workflow which consists of the following steps:
# MAGIC - `dx.scan()`: Scan the lakehouse including catalogs with names starting with `discoverx`
# MAGIC - `dx.inspect()`: Inspect and manually adjust the scan result using the DiscoverX inspection tool
# MAGIC - `dx.publish()`: Publish the classification result which save/merge the result to a system table maintained by DiscoverX
# MAGIC - `dx.search()`: Search your across your previously classified lakehouse for specific records or general classifications/tags

# COMMAND ----------

dx.scan(catalogs="discoverx*")

# COMMAND ----------

dx.inspect()

# COMMAND ----------

# after saving you can see the tags in the data explorer under table details -> properties
dx.publish(publish_uc_tags=True)

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM `_discoverx`.classification.tags

# COMMAND ----------

# DBTITLE 1,Simulate some manually added tags and previously classified columns
# MAGIC %sql
# MAGIC UPDATE _discoverx.classification.tags SET current = false, end_timestamp = current_timestamp() WHERE table_name = "cyber_data";
# MAGIC INSERT INTO _discoverx.classification.tags VALUES 
# MAGIC   ("discoverx_sample_dt",	"sample_datasets", "cyber_data_2", "content", "ip_v6", "inactive", current_timestamp(), "true", null),
# MAGIC   ("discoverx_sample_dt",	"sample_datasets", "cyber_data", "ip_v6_address", "ip_v6", "inactive", current_timestamp(), "true", null);
# MAGIC ALTER TABLE discoverx_sample_dt.sample_datasets.cyber_data ALTER COLUMN ip_v6_address UNSET TAGS ('dx_ip_v6');
# MAGIC ALTER TABLE discoverx_sample_dt.sample_datasets.cyber_data ALTER COLUMN ip_v4_address UNSET TAGS ('dx_ip_v4')

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM `_discoverx`.classification.tags

# COMMAND ----------

# DBTITLE 1,Now rerun scan and inspection and manually change some tag-statuses ...
dx.scan(catalogs="discoverx*")
dx.inspect()

# COMMAND ----------

# MAGIC %md
# MAGIC After saving the tags in the next cell, only those chosen to be 'active' should be seen in Unity Catalog. All changes are captured in the classification table `_dicoverx.classification.tags`.

# COMMAND ----------

dx.publish(publish_uc_tags=True)

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM `_discoverx`.classification.tags WHERE current = True AND tag_status = "active"

# COMMAND ----------

# MAGIC %md
# MAGIC ## Search
# MAGIC 
# MAGIC This command can be used to search inside the content of tables.
# MAGIC 
# MAGIC If the tables have ben scanned before, the search will restrict the scope to only the columns that could contain the search term based on the avaialble rules.

# COMMAND ----------

# instantiate a new discoverx object
dx_search = DX()

# COMMAND ----------

# DBTITLE 1,Search for all records representing the IP 1.2.3.4. Inference of matching rule type is automatic.
dx.search(search_term='1.2.3.4').display()

# COMMAND ----------

import pyspark.sql.functions as func

dx_search.search(search_tags="ip_v4").groupby(
    ["catalog", "database", "table", "search_result.ip_v4"]
).agg(func.count("search_result.ip_v4").alias("count")).display()

# COMMAND ----------

# MAGIC %md
# MAGIC ## M-SQL (Multiplex SQL)
# MAGIC M-SQL lets you run SQL statements across a wide number of table by leveraging the tags

# COMMAND ----------

# MAGIC %md
# MAGIC ### Search for a specific IP across all tables

# COMMAND ----------

dx.msql_experimental("""
SELECT 
  '[ip_v4]' AS ip_v4_column,
  [ip_v4] AS ip_v4, 
  to_json(struct(*)) AS row_content
FROM discoverx*.*.*
WHERE [ip_v4] = '1.2.3.4'
""").display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Distinct IP per table

# COMMAND ----------

dx.msql_experimental("""
SELECT 
  '[ip_v4]' AS ip_v4_column, 
  [ip_v4] AS ip_v4, 
  count([ip_v4]) AS count 
FROM discoverx*.*.*
GROUP BY [ip_v4]
""").display()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Deletes - Right To Be Forgotten Use Cases

# COMMAND ----------

# MAGIC %md
# MAGIC ### Delete from all tables - WHAT_IF

# COMMAND ----------

dx.msql_experimental("DELETE FROM discoverx*.*.* WHERE [ip_v4] = '0.0.0.0'", what_if=True)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Delete from all tables

# COMMAND ----------

dx.msql_experimental("DELETE FROM discoverx*.*.* WHERE [ip_v4] = '0.0.0.0'").display()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Configuration
# MAGIC 
# MAGIC This section is optional, and can be used to customize the behaviour of DiscoverX

# COMMAND ----------

conf = {
  'classification_threshold': 0.95,
}
dx = DX(**conf)

dx = DX(classification_threshold=0.95)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Custom rules

# COMMAND ----------

dx.display_rules()

# COMMAND ----------

from discoverx.rules import Rule


resource_request_id_rule = {
  'name': 'resource_request_id',
  'type': 'regex',
  'description': 'Resource request ID',
  'definition': r'^AR-\d{9}$',
  'match_example': ['AR-123456789']
}

resource_request_id_rule = Rule(**resource_request_id_rule)

dx_custm_rules = DX(custom_rules=[resource_request_id_rule])
dx_custm_rules.display_rules()
# # dx.register_rules(custom_rules)

# COMMAND ----------

dx_custm_rules.scan(catalogs="discoverx*", sample_size=1000)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Help

# COMMAND ----------

help(DX)

# COMMAND ----------



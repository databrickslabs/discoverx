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

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS _discoverx.classification.tags (table_catalog string, table_schema string, table_name string, column_name string, tag_name string, effective_timestamp timestamp, current boolean, end_timestamp timestamp);
# MAGIC INSERT INTO _discoverx.classification.tags VALUES 
# MAGIC   ("discoverx_sample_dt",	"sample_datasets", "cyber_data_2", "content", "ip_v6", current_timestamp(), "true", null),
# MAGIC   ("discoverx_sample_dt",	"sample_datasets", "cyber_data", "ip_v6_address", "ip_v6", current_timestamp(), "true", null),
# MAGIC   ("discoverx_sample_dt",	"sample_datasets", "cyber_data", "ip_v4_address", "pii", current_timestamp(), "true", null);

# COMMAND ----------

dx.scan(catalogs="discoverx*")

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from `_discoverx`.classification.tags

# COMMAND ----------

dx.classifier.classification_result

# COMMAND ----------

# simulate some manual changes in the UI
dx.classifier.classification_result.at[0, "Tags to be published"] = ["ip_v4"]
dx.classifier.classification_result.at[0, "Tags changed"] = True
dx.classifier.classification_result.at[1, "Tags to be published"] = ["ip_v6", "pii"]
dx.classifier.classification_result.at[1, "Tags changed"] = True
dx.classifier.classification_result.at[2, "Tags to be published"] = []
dx.classifier.classification_result.at[2, "Tags changed"] = True
dx.classifier.classification_result

# COMMAND ----------

dx.classifier._stage_updates(dx.classifier.classification_result)
dx.classifier.staged_updates

# COMMAND ----------

dx.classifier.publish(publish_uc_tags=True)

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from `_discoverx`.classification.tags

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
  'column_type_classification_threshold': 0.95,
}
dx = DX(**conf)

dx = DX(column_type_classification_threshold=0.95)

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



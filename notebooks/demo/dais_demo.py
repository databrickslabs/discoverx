# Databricks notebook source
# MAGIC %md
# MAGIC # DiscoverX
# MAGIC This notebook demonstrates DiscoverX by Databricks Labs.
# MAGIC
# MAGIC

# COMMAND ----------

from discoverx import DX
dx = DX(locale="US")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2 Simple Steps to Map and Query your Lakehouse
# MAGIC
# MAGIC ![DiscoverX Steps](files/tables/discoverx_demo/scan_and_query_dais_demo.png)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Scan & Classify
# MAGIC This section demonstrates a typical DiscoverX workflow which consists of the following steps:
# MAGIC - `dx.scan()`: Scans and classifies the lakehouse including all tables which match the specified wildcards
# MAGIC - `dx.publish()`: Publish the classification. This will save the result to a system table maintained by DiscoverX

# COMMAND ----------

dx.scan(from_tables="discoverx*.*.*")

# COMMAND ----------

display(dx.scan_result())

# COMMAND ----------

dx.publish()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Cross-table Queries
# MAGIC Query for specific records or classes across your lakehouse.

# COMMAND ----------

# MAGIC %md
# MAGIC ### Search

# COMMAND ----------

dx.search(search_term='1.2.3.4', from_tables="*.*.*").display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Select by class

# COMMAND ----------

dx.select_by_classes(from_tables="discoverx*.*.*", by_classes=["ip_v4", "ip_v6"]).display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Select by class with aggregations

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
# MAGIC ## Cross-table Deletes
# MAGIC Relevant for __GDPR & Right To Be Forgotten__ use cases. The `yes_i_am_sure` parameter allows to monitor and validate deletion-related queries before they are actually executed.

# COMMAND ----------

# MAGIC %md
# MAGIC ### Delete from all tables - What If

# COMMAND ----------

dx.delete_by_class(from_tables="discoverx*.*.*", by_class="ip_v4", values=['0.0.0.0', '0.0.0.1'], yes_i_am_sure=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Delete from all tables

# COMMAND ----------

dx.delete_by_class(from_tables="discoverx*.*.*", by_class="ip_v4", values=['0.0.0.0', '0.0.0.1'], yes_i_am_sure=True).display()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Custom Rules
# MAGIC DiscoverX comes with a set of built-in rules to detect classes such as IP-adresses, Email-addresses, URLs and many more. In addition, you can define your own custom regex-based rules.

# COMMAND ----------

# MAGIC %md
# MAGIC ### Built-in Rules

# COMMAND ----------

dx.display_rules()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Add custom rule

# COMMAND ----------

from discoverx.rules import RegexRule


resource_request_id_rule = {
  'name': 'resource_request_id',
  'description': 'Resource request ID',
  'definition': r'^AR-\d{9}$',
  'match_example': ['AR-123456789'],
  'nomatch_example': ['R-123']
}

resource_request_id_rule = RegexRule(**resource_request_id_rule)

dx = DX(custom_rules=[resource_request_id_rule])
dx.display_rules()

# COMMAND ----------

dx.scan(from_tables="discoverx*.*.*", sample_size=1000)

# COMMAND ----------

# MAGIC %md
# MAGIC ## What's Next?
# MAGIC
# MAGIC ### Regular Scans
# MAGIC Schedule regular Scans with Databricks Workflows to keep the Classification up-to-date
# MAGIC
# MAGIC ![DiscoverX Job](files/tables/discoverx_demo/Workflows_example.png)
# MAGIC
# MAGIC ### Dashboard, Report, Alert, ...
# MAGIC Use the DiscoverX-managed Classification table to ...
# MAGIC   - Create a Dashboard that informs about the actual state of the Classification
# MAGIC   - Send reports and alerts to subscribers
# MAGIC
# MAGIC ![DiscoverX Job](files/tables/discoverx_demo/dashboard_example.png)

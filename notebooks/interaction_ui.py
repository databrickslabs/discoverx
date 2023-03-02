# Databricks notebook source
# MAGIC %md
# MAGIC # DiscoverX interaction

# COMMAND ----------

# MAGIC %pip install pydantic

# COMMAND ----------

# MAGIC %load_ext autoreload
# MAGIC %autoreload 2

# COMMAND ----------

# MAGIC %md
# MAGIC ## Plain functions flow
# MAGIC 
# MAGIC This is a demo interaction with pure functions

# COMMAND ----------

from discoverx import DX
dx = DX()

# COMMAND ----------

# MAGIC %sql
# MAGIC   SELECT 
# MAGIC   table_catalog, 
# MAGIC   table_schema, 
# MAGIC   sum(INT(less_than_1_day_ago)) AS less_than_1_day_ago,
# MAGIC   sum(INT(less_than_1_week_ago)) AS less_than_1_week_ago,
# MAGIC   sum(INT(less_than_30_days_ago)) AS less_than_30_days_ago,
# MAGIC   sum(INT(less_than_1_year_ago)) AS less_than_1_year_ago,
# MAGIC   sum(INT(more_than_1_year_ago)) AS more_than_1_year_ago
# MAGIC FROM (
# MAGIC   SELECT
# MAGIC     *,
# MAGIC     (last_altered_days_ago < 1) AS less_than_1_day_ago,
# MAGIC     (last_altered_days_ago >= 1 AND last_altered_days_ago < 7) AS less_than_1_week_ago,
# MAGIC     (last_altered_days_ago >= 7 AND last_altered_days_ago < 30) AS less_than_30_days_ago,
# MAGIC     (last_altered_days_ago >= 30 AND last_altered_days_ago < 365) AS less_than_1_year_ago,
# MAGIC     (last_altered_days_ago >= 365) AS more_than_1_year_ago
# MAGIC   FROM (
# MAGIC     SELECT 
# MAGIC       date_diff(now(), last_altered) AS last_altered_days_ago,
# MAGIC       table_catalog,
# MAGIC       table_schema,
# MAGIC       table_name
# MAGIC     FROM system.information_schema.tables 
# MAGIC     WHERE table_schema != "information_schema" AND table_catalog != "system"
# MAGIC   )
# MAGIC )
# MAGIC GROUP BY 1, 2

# COMMAND ----------

# MAGIC %md
# MAGIC ### Scan

# COMMAND ----------

dx.scan(catalogs="*")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Configuration
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
# MAGIC ### Rules

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

dx_custm_rules.scan(sample_size=1000)

# COMMAND ----------

#  IDEA:
# pipeline = [
#   { 'task_type': 'scan',
#     'task_id': 'pii_dev',
#     'configuration': {
#       'catalogs': '*',
#       'databases': 'dev_*',
#       'tables': '*',
#       'sample_size': 10000,
#       'rules': ['custom_device_id', 'dx_ip_address']
#     }
#   }
# ]

# dx.run(pipeline)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Help

# COMMAND ----------

help(DX)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Search
# MAGIC 
# MAGIC This command can be used to search inside the content of tables.
# MAGIC 
# MAGIC If the tables have ben scanned before, the search will restrict the scope to only the columns that could contain the search term based on the avaialble rules.

# COMMAND ----------

# dx.search("erni@databricks.com", databases="prod_*") # This will only search inside columns tagged with dx_email.
# dx.search("127.0.0.1", databases="prod_*") # This will only search inside columns tagged as dx_ip_address.
# dx.search("127.0.0.1", restrict_to_matched_rules=False) # This not use tags to restrict the columns to search 


# COMMAND ----------

# MAGIC %md
# MAGIC ### Tagging

# COMMAND ----------

# dx.tag_columns(rule_match_frequency_table=None, column_type_classification_threshold=0.95) # This will show the SQL commands to apply tags from the temp view discoverx_temp_rule_match_frequency_table

# dx.tag_columns(rule_match_frequency_table="", yes_i_am_sure=True) # This will apply the tags 

# COMMAND ----------



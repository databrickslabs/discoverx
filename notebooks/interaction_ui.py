# Databricks notebook source
# MAGIC %md
# MAGIC # DiscoverX interaction

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
from discoverx.rules import Rule
dx = DX()

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

device_rule_def = {
    'name': 'custom_device_id',
    'type': 'regex',
    'description': 'Custom device ID XX-XXXX-XXXXXXXX',
    'definition': '\d{2}-\d{4}-\d{8}}',
    'example': '00-1111-22222222',
    'tag': 'device_id'
  }

device_rule = Rule(**device_rule_def)

dx = DX(custom_rules=[device_rule])
dx.display_rules()
# dx.register_rules(custom_rules)

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
# MAGIC ### Scan

# COMMAND ----------

dx.scan(catalogs="discoverx", databases="*")

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



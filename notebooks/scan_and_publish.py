# Databricks notebook source
# MAGIC %md
# MAGIC # DiscoverX
# MAGIC This notebook can be used for regular jobs which scan and classify/tag the lakehouse content.

# COMMAND ----------

# DBTITLE 1,Get job input through Widgets
dbutils.widgets.text("catalogs", "*", "Catalogs")
dbutils.widgets.text("schemas", "*", "Schemas")
dbutils.widgets.text("tables", "*", "Tables")

catalogs = dbutils.widgets.get("catalogs")
schemas = dbutils.widgets.get("schemas")
tables = dbutils.widgets.get("tables")

from_table_statement = ".".join([catalogs, schemas, tables])

# COMMAND ----------

# DBTITLE 1,Define custom rules (if needed)
from discoverx.rules import Rule


resource_request_id_rule = {
  'name': 'resource_request_id',
  'type': 'regex',
  'description': 'Resource request ID',
  'definition': r'^AR-\d{9}$',
  'match_example': ['AR-123456789'],
  'nomatch_example': ['R-123']
}

resource_request_id_rule = Rule(**resource_request_id_rule)

# COMMAND ----------

# DBTITLE 1,Set up DiscoverX
from discoverx import DX
dx = DX(custom_rules=[resource_request_id_rule])

# COMMAND ----------

# DBTITLE 1,Scan the Lakehouse
dx.scan(from_tables=from_table_statement)

# COMMAND ----------

# DBTITLE 1,Publish Classification/Tags
dx.publish()

# COMMAND ----------



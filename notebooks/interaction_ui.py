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
# MAGIC - `dx.publish()`: Publish the classification result which save/merge the result to a system table maintained by DiscoverX
# MAGIC - `dx.search()`: Search your across your previously classified lakehouse for specific records or general classifications/classes

# COMMAND ----------

dx.scan(from_tables="discoverx*.*.*")

# COMMAND ----------

dx.scan_result()

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

dx.search(search_term='1.2.3.4', from_tables="*.*.*").display()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Select by class

# COMMAND ----------

dx.select_by_classes(from_tables="discoverx*.*.*", by_classes=["ip_v4"]).display()

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

dx.delete_by_class(from_tables="discoverx*.*.*", by_class="ip_v4", values=['0.0.0.0', '0.0.0.1'], yes_i_am_sure=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Delete from all tables

# COMMAND ----------

dx.delete_by_class(from_tables="discoverx*.*.*", by_class="ip_v4", values=['0.0.0.0', '0.0.0.1'], yes_i_am_sure=True).display()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Configuration
# MAGIC
# MAGIC This section is optional, and can be used to customize the behaviour of DiscoverX

# COMMAND ----------

# You can change the classificaiton threshold with
dx = DX(classification_threshold=0.65)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Custom rules

# COMMAND ----------

dx.display_rules()

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
# # dx.register_rules(custom_rules)

# COMMAND ----------

dx.scan(from_tables="discoverx*.*.*", sample_size=1000)

# COMMAND ----------

# MAGIC %md
# MAGIC ## LLMs and zero-shot-learning

# COMMAND ----------

from discoverx.classifiers import ZeroShotClassifier

my_classifier = ZeroShotClassifier(name="my_classifier",
                   model_name="cross-encoder/nli-deberta-v3-xsmall",
                   candidate_labels=[
                      "financial data",
                      "customer information",
                      "personal data",
                      "IP address",
                      "email",
                      "anything else"
                   ],
                   spark=spark)

# COMMAND ----------

dx.scan(from_tables="discoverx*.*.*pii*", classifier=my_classifier, sample_size=10)

# COMMAND ----------

display(dx.scan_result())

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC     'discoverx_sample' as table_catalog,
# MAGIC     'sample_datasets' as table_schema,
# MAGIC     'fake_pii_examples' as table_name,
# MAGIC     column_name,
# MAGIC     class_name,
# MAGIC     (avg(score)) as score
# MAGIC FROM
# MAGIC (
# MAGIC     SELECT column_name, class_result.*
# MAGIC     FROM
# MAGIC     (
# MAGIC         SELECT column_name, explode(classification_result) AS class_result
# MAGIC         FROM
# MAGIC         (
# MAGIC             SELECT
# MAGIC             column_name,
# MAGIC             my_classifier_udf(value) AS classification_result
# MAGIC             FROM (
# MAGIC               SELECT column_name, array_join(
# MAGIC                 concat(
# MAGIC                 array(concat('Column name: ', column_name), 'values:'), 
# MAGIC                 collect_list(value)
# MAGIC                 ), ',\n', '') AS value
# MAGIC               FROM
# MAGIC               (
# MAGIC                 SELECT
# MAGIC                     stack(3, 'ip_address', `ip_address`, 'name', `name`, 'email', `email`) AS (column_name, value)
# MAGIC                 FROM discoverx_sample.sample_datasets.fake_pii_examples
# MAGIC                 TABLESAMPLE (100 ROWS)
# MAGIC               )
# MAGIC               WHERE value IS NOT NULL
# MAGIC               GROUP BY column_name
# MAGIC             )
# MAGIC         )
# MAGIC     )
# MAGIC )
# MAGIC GROUP BY table_catalog, table_schema, table_name, column_name, class_name
# MAGIC
# MAGIC -- 

# COMMAND ----------

# MAGIC %md
# MAGIC ## Help

# COMMAND ----------

help(DX)

# COMMAND ----------



# Databricks notebook source
# MAGIC %md
# MAGIC # Check delta protocol version
# MAGIC
# MAGIC This notebook will check the delta read and write protocol versions of multiple tables.
# MAGIC
# MAGIC Feature compatibility between delta lake versions is managed through [read protocol](https://docs.delta.io/latest/versioning.html#read-protocol) and [write protocol](https://docs.delta.io/latest/versioning.html#write-protocol).
# MAGIC
# MAGIC Check out the [feature by protocol version table](https://docs.delta.io/latest/versioning.html#features-by-protocol-version) for more details.

# COMMAND ----------

# MAGIC %pip install dbl-discoverx

# COMMAND ----------

dbutils.widgets.text("from_tables", "sample_data_discoverx.*.*")
from_tables = dbutils.widgets.get("from_tables")

# COMMAND ----------

from discoverx import DX

dx = DX()

# COMMAND ----------

dx.from_tables(from_tables)\
    .with_sql("SHOW TBLPROPERTIES {full_table_name}")\
    .apply()\
    .filter('key = "delta.minReaderVersion" OR key = "delta.minWriterVersion"')\
    .display()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Show delta feature compatibility

# COMMAND ----------

from pyspark.sql.functions import col, expr

result = (dx.from_tables(from_tables)\
    .with_sql("SHOW TBLPROPERTIES {full_table_name}")
    .apply()
    .filter('key = "delta.minReaderVersion" OR key = "delta.minWriterVersion"')
    .withColumn("value", col("value").cast("int"))
    .groupBy("table_catalog", "table_schema", "table_name")
    .pivot("key", ["delta.minWriterVersion", "delta.minReaderVersion"])
    .min("value")
    .withColumnRenamed("delta.minReaderVersion", "minReaderVersion")
    .withColumnRenamed("delta.minWriterVersion", "minWriterVersion")
    .withColumn("supports_basic_functionality", expr("minWriterVersion >= 2 AND minReaderVersion >= 1"))
    .withColumn("supports_check_constraint", expr("minWriterVersion >= 3 AND minReaderVersion >= 1"))
    .withColumn("supports_change_data_feed", expr("minWriterVersion >= 4 AND minReaderVersion >= 1"))
    .withColumn("supports_generated_columns", expr("minWriterVersion >= 4 AND minReaderVersion >= 1"))
    .withColumn("supports_column_mapping", expr("minWriterVersion >= 5 AND minReaderVersion >= 2"))
    .withColumn("supports_table_features_read", expr("minWriterVersion >= 7 AND minReaderVersion >= 1"))
    .withColumn("supports_table_features_write", expr("minWriterVersion >= 7 AND minReaderVersion >= 3"))
    .withColumn("supports_deletion_vectors", expr("minWriterVersion >= 7 AND minReaderVersion >= 3"))
    .withColumn("supports_timestamp_without_timezone", expr("minWriterVersion >= 7 AND minReaderVersion >= 3"))
    .withColumn("supports_iceberg_compatibilty_v1", expr("minWriterVersion >= 7 AND minReaderVersion >= 2"))
    .withColumn("supports_v2_checkpoints", expr("minWriterVersion >= 7 AND minReaderVersion >= 3"))
)

result.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Update protocol version
# MAGIC
# MAGIC You can update the table protocol read and write versions by uncommenting the following snippet.
# MAGIC
# MAGIC !!! BE CAREFUL !!!
# MAGIC
# MAGIC Upgrading a reader or writer version might impact older DBR version's ability to read or write the tables. Check [this page](https://docs.databricks.com/en/delta/feature-compatibility.html#what-delta-lake-features-require-databricks-runtime-upgrades) for more details.

# COMMAND ----------

# (dx.from_tables(from_tables)
#     .with_sql("ALTER TABLE {full_table_name} SET TBLPROPERTIES('delta.minWriterVersion' = '5', 'delta.minReaderVersion' = '2')")
#     .apply()
# )

# COMMAND ----------



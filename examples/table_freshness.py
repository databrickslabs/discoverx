# Databricks notebook source
# MAGIC %pip install dbl-discoverx

# COMMAND ----------

dbutils.widgets.text("from_tables", "sample_data_discoverx.*.*")
from_tables = dbutils.widgets.get("from_tables")
dbutils.widgets.text("time_span", "1 day")
time_span = dbutils.widgets.get("time_span")

# COMMAND ----------

from discoverx import DX

dx = DX()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Number of delta table versions per time period

# COMMAND ----------

from pyspark.sql.functions import window, count

(dx
  .from_tables(from_tables)
  .with_sql("DESCRIBE HISTORY {full_table_name}")
  .apply()
  .groupBy("table_catalog", "table_schema", "table_name", window("timestamp", time_span))
  .agg(count("*").alias("delta_versions_count"))
  .display()
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Number of processed rows

# COMMAND ----------

sql_template = f"""
    WITH metrics AS (
        SELECT timestamp, operation, explode(operationMetrics) AS (metric, value)
        FROM (
            DESCRIBE HISTORY {{full_table_name}}
        )
    ),

    metrics_window AS (
        SELECT window(timestamp, '{time_span}') AS time_window, metric, sum(value) as total_rows 
        FROM metrics
        WHERE metric IN (
            -- Written
            "numCopiedRows", 
            "numUpdatedRows", 
            "numOutputRows",
            -- Deleted
            "numDeletedRows", 
            "numTargetRowsDeleted"
        )
        GROUP BY 1, 2
    ),

    metrics_pivot AS (
      SELECT * 
      FROM metrics_window
      PIVOT (sum(total_rows) as total_rows 
      FOR (metric) IN (
              -- Written
              "numCopiedRows", 
              "numUpdatedRows", 
              "numOutputRows",
              
              -- Deleted
              "numDeletedRows", 
              "numTargetRowsDeleted"
        )
      )
    )

    SELECT 
      time_window, 
      -- Written rows include copied, updated and added rows
      (COALESCE(numCopiedRows, 0) + COALESCE(numUpdatedRows, 0) + COALESCE(numOutputRows, 0)) AS totNumWrittenRows,
      -- Deleted rows from both delete and merge operations
      (COALESCE(numDeletedRows, 0) + COALESCE(numTargetRowsDeleted, 0)) AS totNumDeletedRows
    FROM metrics_pivot
"""

processed_rows = (dx
  .from_tables(from_tables)
  .with_sql(sql_template)
  .apply()
).toPandas()

# COMMAND ----------

display(processed_rows)

# COMMAND ----------



# Databricks notebook source
# MAGIC %md
# MAGIC # Text analysis with DiscoverX, MosaicML & Databricks MLflow
# MAGIC
# MAGIC This notebooks uses [DiscoverX](https://github.com/databrickslabs/discoverx) to analyze text with [AI Functions](https://docs.databricks.com/aws/en/large-language-models/ai-functions) over a set of tables in Unity Catalog.
# MAGIC
# MAGIC The notebook will:
# MAGIC 1. Use DiscoverX to sample a set of tables from Unity Catalog and unpivot all string columns into a long format dataset
# MAGIC 2. Find free text columns in scanned tables
# MAGIC 3. Provide a set of possible use cases for that text with cost estimation and example query
# MAGIC
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ## Install dependencies

# COMMAND ----------

# MAGIC %pip install dbl-discoverx==0.0.8
# MAGIC dbutils.library.restartPython()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Setup widgets

# COMMAND ----------

dbutils.widgets.text("from_tables", "your_catalog.*.*", "from tables")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Import required libs and initialize variables

# COMMAND ----------

import pandas as pd
from pyspark.sql.functions import (
    pandas_udf,
    col,
    concat,
    lit,
    explode,
    count,
    avg,
    min,
    max,
    sum,
    collect_set,
    concat_ws,
)
from pyspark.sql.types import ArrayType, StringType, StructType, FloatType, StructField
from typing import Iterator

# COMMAND ----------

from_tables = dbutils.widgets.get("from_tables")

# Set the sample rows size
sample_size = 3


# COMMAND ----------

# MAGIC %md
# MAGIC ## Initialize discoverx

# COMMAND ----------

from discoverx import DX

dx = DX()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Extract samples from all string values

# COMMAND ----------

from pyspark.sql.functions import col, expr

unpivoted_df = (
    dx.from_tables(from_tables)
    .unpivot_string_columns(sample_size=sample_size)
    .apply()
    
)

# COMMAND ----------

display(unpivoted_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Empirically find free-text columns

# COMMAND ----------

from pyspark.sql.functions import length, col, expr, stddev

free_text_columns = (unpivoted_df

            .withColumn("str_length", length(col("string_value")))
            .withColumn("space_count", expr("LENGTH(string_value) - LENGTH(REPLACE(string_value, ' ', ''))"))
            .withColumn("string_value", expr("IF(LENGTH(string_value) > 1000, SUBSTRING(string_value, 1, 1000), string_value)"))
            .groupBy("table_catalog", "table_schema", "table_name", "column_name")
            .agg(
                avg("str_length").alias("avg_str_length"),
                stddev("str_length").alias("stddev_str_length"),
                avg("space_count").alias("avg_space_count"),
                stddev("space_count").alias("stddev_space_count"),
                collect_set("string_value").alias("sample_values"),
            )
            .filter( # Find free text columns empirically
                (col("avg_str_length") > 40) & 
                (col("avg_space_count") > 5) &
                (col("stddev_str_length") > 0) &
                (col("stddev_space_count") > 0))
            )



# COMMAND ----------

# MAGIC %md
# MAGIC ### GenAI use cases

# COMMAND ----------

free_text_columns.display()


# COMMAND ----------

expression = """ai_query(
                  "databricks-meta-llama-3-3-70b-instruct",
                  concat('Provide 2-3 useful, interesting and creative genAI use cases for batch processing a column named ', column_name, ' for a table named ', table_catalog, '.', table_schema, '.', table_name, '. Provide the use cases as a JSON array of objects with the following properties: title, description, type, example_prompt. The example_prompt should be a prompt that can be used process the use case, the row content will be appeneded to the example_prompt. Sample data: ', string(sample_values)),
                  responseFormat => '{
                    "type": "json_schema",
                    "json_schema": {
                      "name": "response",
                      "schema": {
                        "type": "object",
                        "properties": {
                          "use_cases": {
                            "type": "array", 
                            "items": {
                              "type": "object",
                              "properties": {
                                "title": {"type": "string"},
                                "description": {"type": "string"},
                                "type": {"type": "string", "enum": ["classification", "information extraction", "question answering", "summarization", "translation", "sentiment analysis", "other"]},
                                "example_prompt": {"type": "string"},
                                "output_json_schema": {"type": "string", "description": "A JSON schema that could be used to process the output of the AI query executed with example_prompt."},
                                "expected_average_output_tokens": {"type": "number"}
                              },
                              "required": ["title", "description", "type", "example_prompt", "output_json_schema", "expected_average_output_tokens"]
                            }
                          }
                        }
                      }
                    }
                  }'
                )"""

# COMMAND ----------

from pyspark.sql.functions import from_json, explode, col
from pyspark.sql.types import StructType, StructField, StringType, ArrayType, FloatType

schema = StructType([
    StructField("use_cases", ArrayType(StructType([
        StructField("title", StringType(), True),
        StructField("description", StringType(), True),
        StructField("type", StringType(), True),
        StructField("example_prompt", StringType(), True),
        StructField("output_json_schema", StringType(), True),
        StructField("expected_average_output_tokens", FloatType(), True)
    ])), True)
])

use_cases = (free_text_columns
             .withColumn("use_case", expr(expression))
             .withColumn("use_case", from_json(col("use_case"), schema))
             .withColumn("use_case", explode(col("use_case.use_cases")))             
)
use_cases = spark.createDataFrame(use_cases.toPandas()) # Caching the result in pandas to avoid re-running the AI query
display(use_cases)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Count the number of rows per table

# COMMAND ----------

row_count = dx.from_tables(from_tables).with_sql("SELECT COUNT(*) AS row_count FROM {full_table_name}").apply()
row_count = spark.createDataFrame(row_count.toPandas())
display(row_count)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Estimate cost and provide SQL examples

# COMMAND ----------

# Check costs on https://www.databricks.com/product/pricing/foundation-model-serving
cost_per_M_input_tokens = 0.5
cost_per_M_output_tokens = 1.5

result = (use_cases
            .withColumn("estimated_token_count_per_row", expr("ROUND((LENGTH(use_case.example_prompt) + avg_str_length)/ 4)"))
            .join(row_count, ["table_catalog", "table_schema", "table_name"])
            .withColumn("estimated_total_table_token_count", expr("estimated_token_count_per_row * row_count"))
            .withColumn("estimated_total_table_input_processing_cost", col("estimated_total_table_token_count") * cost_per_M_input_tokens / 1000000)
            .withColumn("estimated_total_table_output_processing_cost", col("row_count") * col("use_case.expected_average_output_tokens") * cost_per_M_output_tokens / 1000000)
            .withColumn("example_query", expr("""
              "SELECT ai_query('databricks-meta-llama-3-3-70b-instruct', concat('" ||
              use_case.example_prompt ||
              "', " ||
              column_name ||
              "), responseFormat => '{\\\"type\\\": \\\"json_schema\\\", \\\"json_schema\\\": {\\\"name\\\": \\\"response\\\", \\\"schema\\\": " || use_case.output_json_schema || "}}'" ||
              ") AS ai_output, * FROM " ||
              table_catalog || "." || table_schema || "." || table_name || " LIMIT 100;"
            """))

)

display(result)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Try out some sample queryes from above

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC -- TODO: Copy-paste a query form the result above. Se documentation on https://docs.databricks.com/aws/en/large-language-models/ai-functions
# MAGIC
# MAGIC

# COMMAND ----------


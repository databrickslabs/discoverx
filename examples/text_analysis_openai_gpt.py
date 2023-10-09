# Databricks notebook source
# MAGIC %md
# MAGIC # Text analysis with DiscoverX, Azure OpenAI & Databricks MLflow
# MAGIC
# MAGIC This notebooks uses [DiscoverX](https://github.com/databrickslabs/discoverx) to analyze text with [AZURE OpenAI API](https://learn.microsoft.com/en-us/azure/ai-services/openai/chatgpt-quickstart?tabs=command-line&pivots=programming-language-studio) over a set of tables in Unity Catalog.
# MAGIC
# MAGIC The notebook will:
# MAGIC 1. Use DiscoverX to sample a set of tables from Unity Catalog and unpivot all string columns into a long format dataset
# MAGIC 2. Run text analysis with Azure OpenAI GPT model & Databricks MLflow
# MAGIC
# MAGIC **NOTE**:
# MAGIC - This notebook requires >= DBR 13.3 LTS ML Runtime
# MAGIC - This notebook requires Mlflow gateway route for OpenAI. Please refer to the [example notebook](./mlflow_gateway_routes_examples.py) for the steps to create route

# COMMAND ----------

# MAGIC %md
# MAGIC ## Install dependencies

# COMMAND ----------

# MAGIC %pip install mlflow[gateway]
# MAGIC dbutils.library.restartPython()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Setup widgets

# COMMAND ----------

dbutils.widgets.text("from_tables", "discoverx_sample.*.*", "from tables")
dbutils.widgets.text("openai_route_name", "discoverx-openai-gpt-3.5-completions", "openai route name")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Import required libs and initialize variables

# COMMAND ----------

import openai
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
    regexp_replace,
    upper,
)
from pyspark.sql.types import ArrayType, StringType, StructType, FloatType, StructField
from typing import Iterator

# COMMAND ----------

from_tables = dbutils.widgets.get("from_tables")
openai_route_name = dbutils.widgets.get("openai_route_name")

# Set the sample rows size
sample_size = 100

# COMMAND ----------

# MAGIC %md
# MAGIC ## Initialize discoverx

# COMMAND ----------

from discoverx import DX

dx = DX()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Transform all sampled tables

# COMMAND ----------

unpivoted_df = (
    dx.from_tables(from_tables)
    .unpivot_string_columns(sample_size=sample_size)
    .to_union_dataframe()
    .localCheckpoint()  # Checkpointing to reduce the query plan size
)

# unpivoted_df.display()

# COMMAND ----------

display(unpivoted_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Define udf to use gpt apis

# COMMAND ----------

import mlflow
from mlflow import gateway


@pandas_udf(StringType())
def predict_value_udf(s):
    def predict_value(s):
        data = {
            "prompt": f""" 
        Reply with either YES or NO
        Is this news article related to aquisition/merger ?
        News Article: {s}
        """
        }
        r = mlflow.gateway.query(route=openai_route_name, data=data)
        return r["candidates"][0]["text"]

    return s.apply(predict_value)


# COMMAND ----------

# MAGIC %md
# MAGIC ### Run Predictions

# COMMAND ----------

df_with_prediction = unpivoted_df.withColumn(
    "is_realted_to_aquisition", predict_value_udf(col("string_value"))
).withColumn("is_realted_to_aquisition", upper(regexp_replace(col("is_realted_to_aquisition"), "\\.", "")))

# COMMAND ----------

display(df_with_prediction)

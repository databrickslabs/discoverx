# Databricks notebook source
# MAGIC %md
# MAGIC # Text analysis with DiscoverX & Azure OpenAI
# MAGIC
# MAGIC This notebooks uses [DiscoverX](https://github.com/databrickslabs/discoverx) to analyze text with [AZURE OpenAI API](https://learn.microsoft.com/en-us/azure/ai-services/openai/chatgpt-quickstart?tabs=command-line&pivots=programming-language-studio) over a set of tables in Unity Catalog.
# MAGIC
# MAGIC The notebook will:
# MAGIC 1. Use DiscoverX to sample a set of tables from Unity Catalog and unpivot all string columns into a long format dataset
# MAGIC 2. Run text analysis with Azure OpenAI GPT model

# COMMAND ----------

# MAGIC %md
# MAGIC ## Install dependencies

# COMMAND ----------

# MAGIC %pip install mlflow[gateway]

# COMMAND ----------

# MAGIC %md
# MAGIC ## Setup widgets

# COMMAND ----------

# dbutils.widgets.text("secret_scope", "discoverx", "Secret Scope")
# dbutils.widgets.text("open_ai_base", "openaibase", "Secret Key of Open API Base")
# dbutils.widgets.text("open_ai_key", "openaikey", "Secret Key of Open AI API Key")
dbutils.widgets.text("from_tables", "discoverx_sample.*.*", "from tables")

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

# openai_base_broadcast = sc.broadcast(
#     dbutils.secrets.get(dbutils.widgets.get("secret_scope"), dbutils.widgets.get("open_ai_base"))
# )
# openai_key_broadcast = sc.broadcast(
#     dbutils.secrets.get(dbutils.widgets.get("secret_scope"), dbutils.widgets.get("open_ai_key"))
# )

# COMMAND ----------

# # Define the UDF function
# @pandas_udf(StringType())
# def predict_value_udf(s):
#     openai.api_base = openai_base_broadcast.value  # Your Azure OpenAI resource's endpoint value.
#     openai.api_key = openai_key_broadcast.value
#     openai.api_type = "azure"
#     openai.api_version = "2023-05-15"

#     def predict_value(s):
#         content = f""" Reply strictly with YES/NO
#         Is this news article related to aquisition/merger? 
#         News Article: {s}
#         """

#         response = openai.ChatCompletion.create(
#             engine="gpt-35-turbo",  # The deployment name you chose when you deployed the GPT-35-Turbo or GPT-4 model.
#             messages=[{"role": "user", "content": content}],
#         )
#         return response["choices"][0]["message"]["content"]

#     return s.apply(predict_value)


# COMMAND ----------

import mlflow
from mlflow import gateway
openai_route_name = "discoverx-openai-gpt-3.5-completions"
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

# import mlflow
# from mlflow import gateway
# openai_route_name = "discoverx-openai-gpt-3.5-completions"
# data = {
#             "prompt": f""" what is databricks"""
#         }
# r = mlflow.gateway.query(route=openai_route_name, data=data)
# print(r)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Run Predictions

# COMMAND ----------

df_with_prediction = unpivoted_df.withColumn(
    "is_realted_to_aquisition", predict_value_udf(col("string_value"))
).withColumn("is_realted_to_aquisition", upper(regexp_replace(col("is_realted_to_aquisition"), "\\.", "")))

# COMMAND ----------

display(df_with_prediction)

# COMMAND ----------



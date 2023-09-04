# Databricks notebook source
# MAGIC %md
# MAGIC # PII detection with DiscoverX & Presidio
# MAGIC
# MAGIC This notebooks uses [DiscoverX](https://github.com/databrickslabs/discoverx) to run PII detection with [Presidio](https://microsoft.github.io/presidio/) over a set of tables in Unity Catalog.
# MAGIC
# MAGIC The notebook will:
# MAGIC 1. Use DiscoverX to sample a set of tables from Unity Catalog and unpivot all string columns into a long format dataset
# MAGIC 2. Run PII detection with Presidio
# MAGIC 3. Compute summarised statistics per table and column

# COMMAND ----------

# MAGIC %md
# MAGIC ## Install dependencies

# COMMAND ----------

# MAGIC %pip install openai

# COMMAND ----------



# COMMAND ----------

import openai
import pandas as pd
from pyspark.sql.functions import pandas_udf, col, concat, lit, explode, count, avg, min, max, sum,collect_set,concat_ws
from pyspark.sql.types import ArrayType, StringType, StructType, FloatType, StructField
from typing import Iterator

# COMMAND ----------

# TODO: Change the table selection
# from_tables = "sample_data_discoverx.*.*"
from_tables = "discoverx_sample.sample_datasets.cyber_data"

# TODO: Change the num of rows to sample
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

openai_key_broadcast = sc.broadcast(dbutils.secrets.get("discoverx","openaikey"))

# COMMAND ----------

# Define the UDF function
@pandas_udf(StringType())
def predict_value_udf(s):
    openai.api_base = "https://testopanai.openai.azure.com/"  # Your Azure OpenAI resource's endpoint value.
    openai.api_key = openai_key_broadcast.value
    openai.api_type = "azure"
    openai.api_version = "2023-05-15"
    
    def predict_value(s):
        content = f"Please categorize the following value {s} based on its format into one of the following categories: IPv4, IPv6, MAC, NOT MATCHED. Please reply with just category name"
        response = openai.ChatCompletion.create(
            engine="gpt-35-turbo",  # The deployment name you chose when you deployed the GPT-35-Turbo or GPT-4 model.
            messages=[{"role": "user", "content": content}]
        )
        return response['choices'][0]['message']['content']
    
    return s.apply(predict_value)


# COMMAND ----------

df_with_prediction = unpivoted_df.withColumn("entity_type",predict_value_udf(col("string_value")))

# COMMAND ----------

display(df_with_prediction)

# COMMAND ----------



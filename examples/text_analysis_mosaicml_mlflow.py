# Databricks notebook source
# MAGIC %md
# MAGIC # Text analysis with DiscoverX, MosaicML & Databricks MLflow
# MAGIC
# MAGIC This notebooks uses [DiscoverX](https://github.com/databrickslabs/discoverx) to analyze text with [Mosiac Ml](https://www.mosaicml.com/blog/llama2-inference) over a set of tables in Unity Catalog.
# MAGIC
# MAGIC The notebook will:
# MAGIC 1. Use DiscoverX to sample a set of tables from Unity Catalog and unpivot all string columns into a long format dataset
# MAGIC 2. Run text analysis with MosaicML llama2-70b model & Databricks MLflow
# MAGIC
# MAGIC **NOTE**: This notebook requires >= DBR 13.3 LTS ML Runtime

# COMMAND ----------

# MAGIC %md
# MAGIC ## Install dependencies

# COMMAND ----------

# MAGIC %pip install mlflow[gateway] databricks-sdk==0.8.0
# MAGIC dbutils.library.restartPython()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Setup widgets

# COMMAND ----------

dbutils.widgets.text("from_tables", "discoverx_sample.*.*", "from tables")

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

# get or create mosaic route
import mlflow
from mlflow import gateway

gateway.set_gateway_uri(gateway_uri="databricks")

mosaic_route_name = "mosaicml-llama2-70b-completions"

try:
    route = gateway.get_route(mosaic_route_name)
except:
    # Create a route for embeddings with MosaicML
    print(f"Creating the route {mosaic_route_name}")
    print(
        gateway.create_route(
            name=mosaic_route_name,
            route_type="llm/v1/completions",
            model={
                "name": "llama2-70b-chat",
                "provider": "mosaicml",
                "mosaicml_config": {"mosaicml_api_key": dbutils.secrets.get(scope="dbdemos", key="mosaic_ml_api_key")},
            },
        )
    )

# COMMAND ----------

from mlflow import gateway

@pandas_udf(StringType())
def predict_value_udf(s):
    def predict_value(s):
        data = {
            "prompt": f""" [INST] 
        <<SYS>>
        Reply with either YES or NO
        <</SYS>>
        Is this news article related to aquisition/merger ?
        News Article: {s}
         [/INST]
        """
        }
        r = mlflow.gateway.query(route="mosaicml-llama2-70b-completions", data=data)
        return r["candidates"][0]["text"]

    return s.apply(predict_value)


# COMMAND ----------

# MAGIC %md
# MAGIC ### Run Predictions

# COMMAND ----------

df_with_prediction = unpivoted_df.withColumn("is_realted_to_aquisition", predict_value_udf(col("string_value")))

# COMMAND ----------

display(df_with_prediction)

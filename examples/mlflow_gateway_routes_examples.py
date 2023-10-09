# Databricks notebook source
# MAGIC %md
# MAGIC #Create MLflow Gateway Routes for MosaicML & OpenAI
# MAGIC This notebook provides examples of creating mlflow gateway routes for MosaicML & OpenAI
# MAGIC
# MAGIC **NOTE**: 
# MAGIC - This notebook requires >= DBR 13.3 LTS ML Runtime
# MAGIC - Please refer to [configuring-the-ai-gateway](https://mlflow.org/docs/latest/gateway/index.html#configuring-the-ai-gateway) for more info

# COMMAND ----------

# MAGIC %md
# MAGIC ### Install dependencies

# COMMAND ----------

# MAGIC %pip install mlflow[gateway]
# MAGIC dbutils.library.restartPython()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Setup widgets

# COMMAND ----------

dbutils.widgets.text("moasicml_route_name","discoverx-mosaicml-llama2-70b-completions","mosaicml route name")
dbutils.widgets.text("openai_route_name","discoverx-openai-gpt-3.5-completions","openai route name")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Import required libs and initialize variables

# COMMAND ----------

import mlflow
from mlflow import gateway

# COMMAND ----------

moasicml_route_name = dbutils.widgets.get("moasicml_route_name")
openai_route_name = dbutils.widgets.get("openai_route_name")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Create MLflow fateway route for MosaicML (llama2 model)

# COMMAND ----------

# get or create mosaic route
import mlflow
from mlflow import gateway

gateway.set_gateway_uri(gateway_uri="databricks")

try:
    route = gateway.get_route(moasicml_route_name)
except:
    # Create a route for embeddings with MosaicML
    print(f"Creating the route {moasicml_route_name}")
    print(
          gateway.create_route(
              name=moasicml_route_name,
              route_type="llm/v1/completions",
              model={
                  "name": "llama2-70b-chat",
                  "provider": "mosaicml",
                  "mosaicml_config": {"mosaicml_api_key": dbutils.secrets.get(scope="discoverx", key="mosaic_ml_api_key")},
              },
          )
    )

# COMMAND ----------

# MAGIC %md
# MAGIC ### Create MLflow fateway route for Open AI (GPT 3.5 model)

# COMMAND ----------

# get or create mosaic route
import mlflow
from mlflow import gateway

gateway.set_gateway_uri(gateway_uri="databricks")
try:
    route = gateway.get_route(openai_route_name)
except:
    # Create a route for embeddings with MosaicML
    print(f"Creating the route {openai_route_name}")
    print(
          gateway.create_route(
              name=openai_route_name,
              route_type="llm/v1/completions",
              model={
                  "name": "gpt-35-turbo",
                  "provider": "openai",
                  "openai_config": {"openai_api_key": dbutils.secrets.get(scope="discoverx", key="openaikey"),"openai_api_base":dbutils.secrets.get(scope="discoverx", key="openaibase"),"openai_deployment_name":dbutils.secrets.get(scope="discoverx", key="openai_deployment_name"),"openai_api_type":"azure","openai_api_version":"2023-05-15"},
              },
          )
    )

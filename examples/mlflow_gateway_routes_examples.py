# Databricks notebook source
# MAGIC %md
# MAGIC #Create MLflow Gateway Routes for MosaicML & OpenAI
# MAGIC This notebook provides examples iof creating mlflow gateway routes for MosaicML & OpenAI

# COMMAND ----------

# MAGIC %md
# MAGIC ### Create MosaicML route 

# COMMAND ----------

# MAGIC %pip install mlflow[gateway]
# MAGIC dbutils.library.restartPython()

# COMMAND ----------

# get or create mosaic route
import mlflow
from mlflow import gateway

gateway.set_gateway_uri(gateway_uri="databricks")

mosaic_route_name = "discoverx-mosaicml-llama2-70b-completions"

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
                  "mosaicml_config": {"mosaicml_api_key": dbutils.secrets.get(scope="discoverx", key="mosaic_ml_api_key")},
              },
          )
    )

# COMMAND ----------

# get or create mosaic route
import mlflow
from mlflow import gateway

gateway.set_gateway_uri(gateway_uri="databricks")

openai_route_name = "discoverx-openai-gpt-3.5-completions"
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
                  # "openai_config": {"openai_api_key": dbutils.secrets.get(scope="discoverx", key="openaikey"),"openai_api_base":dbutils.secrets.get(scope="discoverx", key="openaibase"),"openai_deployment_name":"TestOpanAI","openai_api_type":"azure","openai_api_version":"2023-05-15"},
                  "openai_config": {"openai_api_key": dbutils.secrets.get(scope="discoverx", key="openaikey"),"openai_api_base":dbutils.secrets.get(scope="discoverx", key="openaibase"),"openai_deployment_name":"gpt-35-turbo","openai_api_type":"azure","openai_api_version":"2023-05-15"},
              },
          )
    )

# COMMAND ----------

# openai_api_key

# COMMAND ----------

gateway.delete_route(openai_route_name)

# COMMAND ----------



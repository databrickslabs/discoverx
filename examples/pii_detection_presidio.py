# Databricks notebook source
# MAGIC %md
# MAGIC # PII detection with DiscoverX & Presidio
# MAGIC
# MAGIC This notebooks uses [DiscoverX](https://github.com/databrickslabs/discoverx) to run PII detection with [Presidio](https://microsoft.github.io/presidio/) over a set of tables in Unity Catalog.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Install dependencies

# COMMAND ----------

# MAGIC %pip install presidio_analyzer

# COMMAND ----------

# MAGIC %md
# MAGIC ## Download detection model

# COMMAND ----------

# MAGIC %sh python -m spacy download en_core_web_lg

# COMMAND ----------

# MAGIC %md
# MAGIC ## Define variables

# COMMAND ----------

# TODO: Change the table selection
from_tables = "sample_data_discoverx.*.*"

# TODO: Change the num of rows to sample
sample_size = 100


# COMMAND ----------

from presidio_analyzer import AnalyzerEngine, PatternRecognizer
from pyspark.sql.functions import pandas_udf, col, concat, lit, explode, count, avg, sum
from pyspark.sql.types import ArrayType, StringType, StructType, FloatType, StructField
import pandas as pd

# COMMAND ----------

from discoverx import DX
dx = DX()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Transform all sampled tables to long format
# MAGIC
# MAGIC This will take all columns of type string and melt (unpivot) them into a long format dataset

# COMMAND ----------

melted_df = (
  dx.from_tables(from_tables)
  .melt_string_columns(sample_size=sample_size)
  .to_union_dataframe()
  .localCheckpoint() # Checkpointing to reduce the query plan size
)

# melted_df.display()

# COMMAND ----------

melted_stats = (melted_df
  .groupBy("table_catalog", "table_schema", "table_name", "column_name")
  .agg(count("string_value").alias("sampled_rows_count"))
)

# melted_stats.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Define Presidio UDFs

# COMMAND ----------

d = {}
d["a"] = 1
d.keys()

# d


# COMMAND ----------

# Define the analyzer, and add custom matchers if needed
analyzer = AnalyzerEngine()

# broadcast the engines to the cluster nodes
broadcasted_analyzer = sc.broadcast(analyzer)

# define a pandas UDF function and a series function over it.
def analyze_text(text: str) -> list[str]:
    try:
      analyzer = broadcasted_analyzer.value
      analyzer_results = analyzer.analyze(text=text, language="en")
      dic = {}
      # Deduplicate the detections and take the max scode per entity type
      for r in analyzer_results:
        if r.entity_type in dic.keys():
          dic[r.entity_type] = max(r.score, dic[r.entity_type])
        else:
          dic[r.entity_type] = r.score
      return [{"entity_type": k, "score": dic[k]} for k in dic.keys()]
    except:
      return []

def analyze_series(s: pd.Series) -> pd.Series:
    return s.apply(analyze_text)


# define a the function as pandas UDF
analyze = pandas_udf(analyze_series, returnType=ArrayType(StructType([StructField("entity_type", StringType()), StructField("score", FloatType())])))


# COMMAND ----------

# MAGIC %md
# MAGIC ## Run PII detections

# COMMAND ----------

detections = (melted_df
      .withColumn("text", concat(col("table_name"), lit(" "), col("column_name"),  lit(": "), col("string_value")))
      .withColumn("detection", explode(analyze(col("text"))))
      .select("table_catalog", "table_schema", "table_name", "column_name", "string_value", "detection.*")
)

# detections.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Compute summarised statistics

# COMMAND ----------


summarised_detections = (detections
                         .groupBy("table_catalog", "table_schema", "table_name", "column_name", "entity_type")
                         .agg(
                            count("string_value").alias("value_count"), 
                            avg("score").alias("avg_score"), 
                            sum("score").alias("sum_score")
                            )
                          .join(melted_stats, ["table_catalog", "table_schema", "table_name", "column_name"])
                          .withColumn("score", col("sum_score") / col("sampled_rows_count"))
                          .select("table_catalog", "table_schema", "table_name", "column_name", "entity_type", "score")
                         )
                         
summarised_detections.display()

# COMMAND ----------

# TODO: Store result to a table
# summarised_detections.write.saveAsTable("default..")

# COMMAND ----------



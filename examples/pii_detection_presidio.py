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

# MAGIC %pip install presidio_analyzer==2.2.33 dbl-discoverx==0.0.5

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

import pandas as pd
from presidio_analyzer import AnalyzerEngine, PatternRecognizer
from pyspark.sql.functions import pandas_udf, col, concat, lit, explode, count, avg, min, max, sum
from pyspark.sql.types import ArrayType, StringType, StructType, FloatType, StructField
from typing import Iterator

# COMMAND ----------

from discoverx import DX

dx = DX()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Transform all sampled tables to long format
# MAGIC
# MAGIC This will take all columns of type string and unpivot (melt) them into a long format dataset

# COMMAND ----------

unpivoted_df = (
    dx.from_tables(from_tables)
    .unpivot_string_columns(sample_size=sample_size)
    .apply()
    .localCheckpoint()  # Checkpointing to reduce the query plan size
)

# unpivoted_df.display()

# COMMAND ----------

unpivoted_stats = unpivoted_df.groupBy("table_catalog", "table_schema", "table_name", "column_name").agg(
    count("string_value").alias("sampled_rows_count")
)

# unpivoted_stats.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Define Presidio UDFs
# MAGIC

# COMMAND ----------


# Define the analyzer, and add custom matchers if needed
analyzer = AnalyzerEngine()

# broadcast the engines to the cluster nodes
broadcasted_analyzer = sc.broadcast(analyzer)


# define a pandas UDF function and a series function over it.
def analyze_text(text: str, analyzer: AnalyzerEngine) -> list[str]:
    try:
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


# define the iterator of series to minimize
def analyze_series(iterator: Iterator[pd.Series]) -> Iterator[pd.Series]:
    analyzer = broadcasted_analyzer.value
    for series in iterator:
        # Use that state for whole iterator.
        yield series.apply(lambda t: analyze_text(t, analyzer))


# define a the function as pandas UDF
analyze = pandas_udf(
    analyze_series,
    returnType=ArrayType(StructType([StructField("entity_type", StringType()), StructField("score", FloatType())])),
)


# COMMAND ----------

# MAGIC %md
# MAGIC ## Run PII detections

# COMMAND ----------

detections = (
    unpivoted_df.withColumn(
        "text", concat(col("table_name"), lit(" "), col("column_name"), lit(": "), col("string_value"))
    )
    .withColumn("detection", explode(analyze(col("text"))))
    .select("table_catalog", "table_schema", "table_name", "column_name", "string_value", "detection.*")
)

# detections.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Compute summarised statistics

# COMMAND ----------


summarised_detections = (
    detections.groupBy("table_catalog", "table_schema", "table_name", "column_name", "entity_type")
    .agg(count("string_value").alias("value_count"), max("score").alias("max_score"), sum("score").alias("sum_score"))
    .join(unpivoted_stats, ["table_catalog", "table_schema", "table_name", "column_name"])
    .withColumn("score", col("sum_score") / col("sampled_rows_count"))
    .select("table_catalog", "table_schema", "table_name", "column_name", "entity_type", "score", "max_score")
)

# TODO: Comment out the display when saving the result to table
summarised_detections.display()

# COMMAND ----------

# TODO: Store result to a table
# summarised_detections.write.saveAsTable("default..")

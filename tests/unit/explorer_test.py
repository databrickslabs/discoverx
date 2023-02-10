from discoverx.explorer import Explorer
from pyspark.sql import SparkSession
from pathlib import Path
import logging

# def test_jobs(spark: SparkSession, tmp_path: Path):
#     logging.info("Testing the ETL job")
#     common_config = {"database": "default", "table": "sklearn_housing"}
#     test_etl_config = {"output": common_config}
#     etl_job = Explorer(spark, test_etl_config)
#     etl_job.launch()

#     # table_name = f"{test_etl_config['output']['database']}.{test_etl_config['output']['table']}"
#     _count = spark.table("sklearn_housing").count()

#     assert _count > 0
#     logging.info("Testing the ETL job - done")


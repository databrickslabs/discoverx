"""
This module contains common utility functions used throughout the package
"""
from pyspark.sql import SparkSession

def get_dbutils(
    spark: SparkSession,
):
    """
    please note that this function is used in mocking by its name
    """
    try:
        # pylint: disable=import-outside-toplevel
        from pyspark.dbutils import DBUtils  # noqa

        if "dbutils" not in locals():
            utils = DBUtils(spark)
            return utils
        return locals().get("dbutils")
    except ImportError:
        return None


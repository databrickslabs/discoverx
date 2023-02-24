import pandas as pd
from discoverx.config import TableInfo
from discoverx.rules import Rule
from discoverx.data_model import DataModel
from discoverx.logging import Logging
from discoverx.sql_builder import SqlBuilder
from pyspark.sql import SparkSession

class Explorer:

    def __init__(self, date_model: DataModel = None, logger=None):
        if logger is None:
            logger = Logging()
        self.logger = logger

        if date_model is None:
            date_model = DataModel()
        self.data_model = date_model

        self.spark = SparkSession.getActiveSession()

    def get_table_list(self, catalogs_filter, databases_filter, tables_filter):
        return self.data_model.get_table_list(catalogs_filter, databases_filter, tables_filter)

    def scan(self, table_list: list[TableInfo], rule_list: list[Rule], sample_size: int) -> pd.DataFrame:

        self.logger.debug("Launching lakehouse scanning task\n")
        
        n_tables = len(table_list)
        builder = SqlBuilder()
        dfs = []

        for i, table in enumerate(table_list):
            self.logger.friendly(
                f"Scanning table '{table.catalog}.{table.database}.{table.table}' ({i + 1}/{n_tables})"
            )
            
            try:
                # Build rule matching SQL
                sql = builder.rule_matching_sql(table, rule_list, sample_size)

                # Execute SQL and append result
                dfs.append(self.spark.sql(sql).toPandas())
            except Exception as e:
                self.logger.error(f"Error while scanning table '{table.catalog}.{table.database}.{table.table}': {e}")
                continue        

        self.logger.debug("Finished lakehouse scanning task")
        
        if dfs:
          return pd.concat(dfs)
        else:
          return pd.DataFrame()
        



        # self.spark.sql("CREATE TABLE IF NOT EXISTS default.sklearn_housing (value INT)")
        # self.spark.sql("INSERT INTO default.sklearn_housing VALUES (1), (2), (3), (4), (5), (6), (7), (8), (9), (10)")

        # self.logger.info("Rule matching task finished!")

from discoverx.config import ColumnInfo, TableInfo
from discoverx.sql_builder import SqlBuilder

class DataModel:
    def __init__(self, logger=None):
        
        if logger is None:
            from discoverx.logging import Logging
            logger = Logging()
        self.logger = logger


    def get_table_list(self, catalogs_filter, databases_filter, tables_filter):
        sql = SqlBuilder().get_table_list_sql(catalogs_filter, databases_filter, tables_filter)
        rows = spark.sql(sql).collect()
        filtered_tables = [
            TableInfo(
                row["table_catalog"], 
                row["table_schema"], 
                row["table_name"], 
                [
                    ColumnInfo(
                        col["column_name"], 
                        col["data_type"], 
                        col["partition_index"]
                    ) for col in row['table_columns']
                ]
            ) for row in rows]

        return filtered_tables

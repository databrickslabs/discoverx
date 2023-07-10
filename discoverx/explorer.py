import concurrent.futures
from functools import reduce
from discoverx import logging
from discoverx.common.helper import strip_margin
from discoverx.msql import Msql
from discoverx.scanner import ColumnInfo, TableInfo

logger = logging.Logging()


class DataExplorer:
    def __init__(self, from_tables):
        self.from_tables = from_tables
        self.catalogs, self.schemas, self.tables = Msql.validate_from_tables(from_tables)
        self.table_info_df = self._get_tables_info_df(self.catalogs, self.schemas, self.tables)
        self.columns_table_name = "system.information_schema.columns"
        # self._having_columns = []
        # self._having_classes = []
        # self.sql_query = ""
        self.max_workers = 10

    def _get_tables_info_df(self, catalogs, schemas, tables):
        # Filter tables by matching filter
        table_list_sql = self._get_table_list_sql()

        filtered_tables = self.spark.sql(table_list_sql).collect()

        if len(filtered_tables) == 0:
            raise ValueError(f"No tables found matching filter: {catalogs}.{schemas}.{tables}")

        return filtered_tables

    def _get_table_list_sql(self):
        """
        Returns a SQL expression which returns a list of columns matching
        the specified filters

        Returns:
            string: The SQL expression
        """

        catalog_sql = f"""AND regexp_like(table_catalog, "^{self.catalogs.replace("*", ".*")}$")"""
        schema_sql = f"""AND regexp_like(table_schema, "^{self.schemas.replace("*", ".*")}$")"""
        table_sql = f"""AND regexp_like(table_name, "^{self.tables.replace("*", ".*")}$")"""

        sql = f"""
        SELECT 
            table_catalog, 
            table_schema, 
            table_name, 
            collect_list(struct(column_name, data_type, partition_index)) as table_columns
        FROM {self.columns_table_name}
        WHERE 
            table_schema != "information_schema" 
            {catalog_sql if self.catalogs != "*" else ""}
            {schema_sql if self.schemas != "*" else ""}
            {table_sql if self.tables != "*" else ""}
        GROUP BY table_catalog, table_schema, table_name
        """

        return strip_margin(sql)

    def _run_sql(self, sql_template: str, table_info: TableInfo):
        sql = sql_template.format(
            table_catalog=table_info.table_catalog,
            table_schema=table_info.table_schema,
            table_name=table_info.table_name,
        )
        logger.debug(f"Running SQL query: {sql}")

        try:
            df = self.spark.sql(sql)
            logger.debug(f"Finished running SQL query: {sql}")
            return df
        except Exception as e:
            logger.error(
                f"Error running SQL query for: {table_info.catalog}.{table_info.schema}.{table_info.table_name}."
            )
            logger.error(e)
            return None

    def _to_info_list(self, df):
        filtered_tables = [
            TableInfo(
                row["table_catalog"],
                row["table_schema"],
                row["table_name"],
                [
                    ColumnInfo(col["column_name"], col["data_type"], col["partition_index"], [])
                    for col in row["table_columns"]
                ],
            )
            for row in df
        ]
        return filtered_tables

    def sql(self, sql_template: str):
        # logger.friendly("""Ok, I'm going to scan your lakehouse for data that matches your rules.""")

        logger.debug("Launching lakehouse scanning task\n")

        table_list = self._to_info_list(self.table_info_df)
        dfs = []
        with concurrent.futures.ThreadPoolExecutor(max_workers=self.max_workers) as executor:
            # Submit tasks to the thread pool
            futures = [executor.submit(self._run_sql, sql_template, table) for table in table_list]

            # Process completed tasks
            for future in concurrent.futures.as_completed(futures):
                result = future.result()
                if result is not None:
                    dfs.append(result)

        logger.debug("Finished lakehouse scanning task")

        if dfs:
            return reduce(lambda x, y: x.union(y), dfs)
        else:
            raise Exception("No SQL statements were successfully executed.")

    # def having_columns(self, *columns):
    #     new_obj = copy.deepcopy(self)
    #     new_obj.columns.extend(columns)
    #     return new_obj

    # def having_classes(self, *classes):
    #     new_obj = copy.deepcopy(self)
    #     new_obj.classes.extend(classes)
    #     return new_obj

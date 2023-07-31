import concurrent.futures
import copy
from functools import reduce

import pandas as pd
from discoverx import logging
from discoverx.common.helper import strip_margin
from discoverx.msql import Msql
from discoverx.scanner import ColumnInfo, TableInfo
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import lit

logger = logging.Logging()


class InfoFetcher:
    def __init__(self, spark, columns_table_name="system.information_schema.columns") -> None:
        self.columns_table_name = columns_table_name
        self.spark = spark

    def _to_info_list(df: pd.DataFrame) -> list[TableInfo]:
        def collect_column_info(row):
            return ColumnInfo(row["column_name"], row["data_type"], row["partition_index"], [])

        grouped = df.groupby(["table_catalog", "table_schema", "table_name"], dropna=False).apply(
            lambda group: TableInfo(
                group["table_catalog"].iloc[0],
                group["table_schema"].iloc[0],
                group["table_name"].iloc[0],
                [collect_column_info(row) for _, row in group.iterrows()],
            )
        )
        return grouped.tolist()

    def get_tables_info(self, catalogs, schemas, tables) -> list[TableInfo]:
        # Filter tables by matching filter
        table_list_sql = self._get_table_list_sql(catalogs, schemas, tables)

        filtered_tables = self.spark.sql(table_list_sql).toPandas()

        if len(filtered_tables) == 0:
            raise ValueError(f"No tables found matching filter: {catalogs}.{schemas}.{tables}")

        return self._to_info_list(filtered_tables)

    def _get_table_list_sql(self, catalogs, schemas, tables) -> str:
        """
        Returns a SQL expression which returns a list of columns matching
        the specified filters

        Returns:
            string: The SQL expression
        """

        catalog_sql = f"""AND regexp_like(table_catalog, "^{catalogs.replace("*", ".*")}$")"""
        schema_sql = f"""AND regexp_like(table_schema, "^{schemas.replace("*", ".*")}$")"""
        table_sql = f"""AND regexp_like(table_name, "^{tables.replace("*", ".*")}$")"""

        sql = f"""
        SELECT 
            table_catalog, 
            table_schema, 
            table_name, 
            column_name, 
            data_type, 
            partition_index
        FROM {self.columns_table_name}
        WHERE 
            table_schema != "information_schema" 
            {catalog_sql if catalogs != "*" else ""}
            {schema_sql if schemas != "*" else ""}
            {table_sql if tables != "*" else ""}
        """

        return strip_margin(sql)


class DataExplorer:
    def __init__(self, from_tables, spark: SparkSession, info_fetcher: InfoFetcher):
        self.info_fetcher = info_fetcher
        self.from_tables = from_tables
        self.spark = spark
        self.catalogs, self.schemas, self.tables = Msql.validate_from_components(from_tables)
        self._having_columns = []
        self._having_classes = []
        self._sql_query_template = None
        self._max_concurrency = 10

    def _build_sql(self, sql_template: str, table_info: TableInfo) -> str:
        if table_info.catalog and table_info.catalog != "None":
            full_table_name = f"{table_info.catalog}.{table_info.schema}.{table_info.table}"
        else:
            full_table_name = f"{table_info.schema}.{table_info.table}"
        sql = sql_template.format(
            table_catalog=table_info.catalog,
            table_schema=table_info.schema,
            table_name=table_info.table,
            full_table_name=full_table_name,
        )
        return sql

    def _run_sql(self, sql: str, table_info: TableInfo) -> DataFrame:
        logger.debug(f"Running SQL query: {sql}")

        try:
            df = (
                self.spark.sql(sql)
                .withColumn("table_catalog", lit(table_info.catalog))
                .withColumn("table_schema", lit(table_info.schema))
                .withColumn("table_name", lit(table_info.table))
            )
            logger.debug(f"Finished running SQL query: {sql}")
            return df
        except Exception as e:
            logger.error(f"Error running SQL query for: {table_info.catalog}.{table_info.schema}.{table_info.table}.")
            logger.error(e)
            return None

    def _get_sql_commands(self) -> list[tuple[str, TableInfo]]:
        logger.debug("Launching lakehouse scanning task\n")

        table_list = self.info_fetcher.get_tables_info(self.catalogs, self.schemas, self.tables)
        sql_commands = [(self._build_sql(self._sql_query_template, table), table) for table in table_list]
        return sql_commands

    def __deepcopy__(self, memo):
        new_obj = type(self).__new__(self.__class__)
        new_obj.__dict__.update(self.__dict__)
        new_obj.catalogs = copy.deepcopy(self.catalogs)
        new_obj.schemas = copy.deepcopy(self.schemas)
        new_obj.tables = copy.deepcopy(self.tables)
        new_obj._having_columns = copy.deepcopy(self._having_columns)
        new_obj._having_classes = copy.deepcopy(self._having_classes)
        new_obj._sql_query_template = copy.deepcopy(self._sql_query_template)
        new_obj._max_concurrency = copy.deepcopy(self._max_concurrency)

        # We can't deepcopy spark session, so we just copy the reference
        new_obj.spark = self.spark

        return new_obj

    def having_columns(self, *columns) -> "DataExplorer":
        new_obj = copy.deepcopy(self)
        new_obj._having_columns.extend(columns)
        return new_obj

    def having_classes(self, *classes) -> "DataExplorer":
        new_obj = copy.deepcopy(self)
        new_obj._having_classes.extend(classes)
        return new_obj

    def with_concurrency(self, max_concurrency) -> "DataExplorer":
        new_obj = copy.deepcopy(self)
        new_obj._max_concurrency = max_concurrency
        return new_obj

    def apply_sql(self, sql_query_template) -> "DataExplorer":
        new_obj = copy.deepcopy(self)
        new_obj._sql_query_template = sql_query_template
        return new_obj

    def explain(self):
        column_filter_explanation = ""
        class_filter_explanation = ""

        if self.having_columns:
            column_filter_explanation = f"""
            only for tables that have all the following columns: {self.having_columns}
            """
        if self.having_classes:
            class_filter_explanation = f"""
            only for tables that have all the following classes: {self.having_classes}
            """

        explanation = f"""
        DiscoverX will apply the following template
        {self._sql_query_template}

        to the tables in the following catalog, schema, table combinations:
        {self.from_tables}
        {column_filter_explanation}
        {class_filter_explanation}

        The SQL to be executed is:
        (just a moment, I am generating it)
        """

        sql_commands = self._get_sql_commands()
        for sql, table in sql_commands:
            explanation += f"""
            <p>For table: {table.catalog}.{table.schema}.{table.table}</p>
            <pre><code>{sql}</code></pre>
            """

        logger.friendlyHTML(explanation)

    def to_dataframe(self) -> DataFrame:
        sql_commands = self._get_sql_commands()
        dfs = []
        with concurrent.futures.ThreadPoolExecutor(max_workers=self._max_concurrency) as executor:
            # Submit tasks to the thread pool
            futures = [executor.submit(self._run_sql, sql, table) for sql, table in sql_commands]

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

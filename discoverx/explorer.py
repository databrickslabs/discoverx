import concurrent.futures
import copy
import re
import pandas as pd
from discoverx import logging
from discoverx.common import helper
from discoverx.scanner import ColumnInfo, TableInfo
from functools import reduce
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import lit

logger = logging.Logging()


class InfoFetcher:
    def __init__(self, spark, columns_table_name="system.information_schema.columns") -> None:
        self.columns_table_name = columns_table_name
        self.spark = spark

    def _to_info_list(self, df: pd.DataFrame) -> list[TableInfo]:
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

    def get_tables_info(self, catalogs: str, schemas: str, tables: str, columns: list[str] = []) -> list[TableInfo]:
        # Filter tables by matching filter
        table_list_sql = self._get_table_list_sql(catalogs, schemas, tables, columns)

        filtered_tables = self.spark.sql(table_list_sql).toPandas()

        if len(filtered_tables) == 0:
            raise ValueError(f"No tables found matching filter: {catalogs}.{schemas}.{tables}")

        return self._to_info_list(filtered_tables)

    def _get_table_list_sql(self, catalogs: str, schemas: str, tables: str, columns: list[str] = []) -> str:
        """
        Returns a SQL expression which returns a list of columns matching
        the specified filters

        Returns:
            string: The SQL expression
        """

        catalog_sql = f"""AND regexp_like(table_catalog, "^{catalogs.replace("*", ".*")}$")"""
        schema_sql = f"""AND regexp_like(table_schema, "^{schemas.replace("*", ".*")}$")"""
        table_sql = f"""AND regexp_like(table_name, "^{tables.replace("*", ".*")}$")"""

        if columns:
            match_any_col = "|".join([f'({c.replace("*", ".*")})' for c in columns])
            columns_sql = f"""AND regexp_like(column_name, "^{match_any_col}$")"""

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
            {columns_sql if columns else ""}
        """

        return helper.strip_margin(sql)


class DataExplorer:
    from_components_expr = r"^(([0-9a-zA-Z_\*]+)\.([0-9a-zA-Z_\*]+)\.([0-9a-zA-Z_\*]+))$"

    def __init__(self, from_tables, spark: SparkSession, info_fetcher: InfoFetcher) -> None:
        self._from_tables = from_tables
        self._catalogs, self._schemas, self._tables = DataExplorer.validate_from_components(from_tables)
        self._spark = spark
        self._info_fetcher = info_fetcher
        self._having_columns = []
        # self._having_classes = []
        self._sql_query_template = None
        self._max_concurrency = 10

    @staticmethod
    def validate_from_components(from_tables: str):
        """Extracts the catalog, schema and table name from the from_table string"""
        matches = re.findall(DataExplorer.from_components_expr, from_tables)
        if len(matches) == 1 and len(matches[0]) == 4:
            return (matches[0][1], matches[0][2], matches[0][3])
        else:
            raise ValueError(
                f"Invalid from_tables statement '{from_tables}'. Should be a string in format 'table_catalog.table_schema.table_name'. You can use '*' as wildcard."
            )

    def __deepcopy__(self, memo):
        new_obj = type(self).__new__(self.__class__)
        new_obj.__dict__.update(self.__dict__)
        new_obj._catalogs = copy.deepcopy(self._catalogs)
        new_obj._schemas = copy.deepcopy(self._schemas)
        new_obj._tables = copy.deepcopy(self._tables)
        new_obj._having_columns = copy.deepcopy(self._having_columns)
        # new_obj._having_classes = copy.deepcopy(self._having_classes)
        new_obj._sql_query_template = copy.deepcopy(self._sql_query_template)
        new_obj._max_concurrency = copy.deepcopy(self._max_concurrency)

        new_obj._spark = self._spark
        new_obj._info_fetcher = self._info_fetcher

        return new_obj

    def having_columns(self, *columns) -> "DataExplorer":
        new_obj = copy.deepcopy(self)
        new_obj._having_columns.extend(columns)
        return new_obj

    # def having_classes(self, *classes) -> "DataExplorer":
    #     new_obj = copy.deepcopy(self)
    #     new_obj._having_classes.extend(classes)
    #     return new_obj

    def with_concurrency(self, max_concurrency) -> "DataExplorer":
        new_obj = copy.deepcopy(self)
        new_obj._max_concurrency = max_concurrency
        return new_obj

    def with_sql(self, sql_query_template: str) -> "DataExplorerActions":
        new_obj = copy.deepcopy(self)
        new_obj._sql_query_template = sql_query_template
        return DataExplorerActions(new_obj, spark=self._spark, info_fetcher=self._info_fetcher)


class DataExplorerActions:
    def __init__(
        self, data_explorer: DataExplorer, spark: SparkSession = None, info_fetcher: InfoFetcher = None
    ) -> None:
        self._data_explorer = data_explorer
        if spark is None:
            spark = SparkSession.builder.getOrCreate()
        if info_fetcher is None:
            info_fetcher = InfoFetcher(spark)
        self._info_fetcher = info_fetcher
        self._spark = spark

    @staticmethod
    def _build_sql(sql_template: str, table_info: TableInfo) -> str:
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
                self._spark.sql(sql)
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

    def _get_sql_commands(self, data_explorer: DataExplorer) -> list[tuple[str, TableInfo]]:
        logger.debug("Launching lakehouse scanning task\n")

        table_list = self._info_fetcher.get_tables_info(
            data_explorer._catalogs, data_explorer._schemas, data_explorer._tables, data_explorer._having_columns
        )
        sql_commands = [
            (DataExplorerActions._build_sql(data_explorer._sql_query_template, table), table) for table in table_list
        ]
        return sql_commands

    def explain(self) -> None:
        data_explorer = self._data_explorer
        column_filter_explanation = ""
        # class_filter_explanation = ""
        sql_explanation = ""

        if data_explorer._having_columns:
            column_filter_explanation = (
                f"only for tables that have all the following columns: {data_explorer._having_columns}"
            )
        # if data_explorer._having_classes:
        # class_filter_explanation = f"only for tables that have all the following classes: {data_explorer._having_classes}"
        if data_explorer._sql_query_template:
            sql_explanation = f"The SQL to be executed is (just a moment, generating it...):"

        explanation = f"""
        DiscoverX will apply the following SQL template

        {data_explorer._sql_query_template}

        to the tables in the following catalog, schema, table combinations:
        {data_explorer._from_tables}
        {column_filter_explanation}
        {sql_explanation}
        """
        logger.friendly(helper.strip_margin(explanation))

        detailed_explanation = ""
        if data_explorer._sql_query_template:
            sql_commands = self._get_sql_commands(data_explorer)
            for sql, table in sql_commands:
                detailed_explanation += f"""
                <p>For table: {table.catalog}.{table.schema}.{table.table}</p>
                <pre><code>{sql}</code></pre>
                """

            logger.friendlyHTML(detailed_explanation)

    def execute(self) -> DataFrame:
        df = self.to_union_dataframe()
        try:
            df.display()
        except Exception as e:
            df.show(truncate=False)

    def to_union_dataframe(self) -> DataFrame:
        data_explorer = self._data_explorer

        sql_commands = self._get_sql_commands(data_explorer)
        dfs = []
        with concurrent.futures.ThreadPoolExecutor(max_workers=data_explorer._max_concurrency) as executor:
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

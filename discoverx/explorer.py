import concurrent.futures
import copy
import re
import more_itertools
from typing import Optional, List, Callable, Iterable
from discoverx import logging
from discoverx.common import helper
from discoverx.discovery import Discovery
from discoverx.rules import Rule
from discoverx.table_info import TagsInfo, ColumnTagInfo, TagInfo, ColumnInfo, TableInfo
from functools import reduce
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import lit
from discoverx.table_info import InfoFetcher, TableInfo
from discoverx.delta_housekeeping import DeltaHousekeeping


logger = logging.Logging()


class DataExplorer:
    FROM_COMPONENTS_EXPR = r"^(([0-9a-zA-Z_\*]+)\.([0-9a-zA-Z_\*]+)\.([0-9a-zA-Z_\*]+))$"

    def __init__(self, from_tables, spark: SparkSession, info_fetcher: InfoFetcher) -> None:
        self._from_tables = from_tables
        (
            self._catalogs,
            self._schemas,
            self._tables,
        ) = DataExplorer.validate_from_components(from_tables)
        self._spark = spark
        self._info_fetcher = info_fetcher
        self._having_columns = []
        self._sql_query_template = None
        self._max_concurrency = 10
        self._with_tags = False

    @staticmethod
    def validate_from_components(from_tables: str):
        """Extracts the catalog, schema and table name from the from_table string"""
        matches = re.findall(DataExplorer.FROM_COMPONENTS_EXPR, from_tables)
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
        new_obj._sql_query_template = copy.deepcopy(self._sql_query_template)
        new_obj._max_concurrency = copy.deepcopy(self._max_concurrency)
        new_obj._with_tags = copy.deepcopy(self._with_tags)

        new_obj._spark = self._spark
        new_obj._info_fetcher = self._info_fetcher

        return new_obj

    def having_columns(self, *columns) -> "DataExplorer":
        """Will select tables that contain any of the provided columns

        Args:
            columns (list[str]): The list of column names to filter by
        """
        new_obj = copy.deepcopy(self)
        new_obj._having_columns.extend(columns)
        return new_obj

    def with_concurrency(self, max_concurrency) -> "DataExplorer":
        """Sets the maximum number of concurrent queries to run"""
        new_obj = copy.deepcopy(self)
        new_obj._max_concurrency = max_concurrency
        return new_obj

    def with_tags(self, use_tags=True) -> "DataExplorer":
        """Defines if tags should be collected when getting table metadata"""
        new_obj = copy.deepcopy(self)
        new_obj._with_tags = use_tags
        return new_obj

    def with_sql(self, sql_query_template: str) -> "DataExplorerActions":
        """Sets the SQL query template to use for the data exploration

        Args:
            sql_query_template (str): The SQL query template to use. The template might contain the following variables:
                - table_catalog: The table catalog name
                - table_schema: The table schema name
                - table_name: The table name
                - full_table_name: The full table name (catalog.schema.table)
                - stack_string_columns: A SQL expression that returns a table with two columns: column_name and string_value
        """
        return self.apply_sql(sql_query_template)

    def apply_sql(self, sql_query_template: str) -> "DataExplorerActions":
        """[DEPRECATED] Sets the SQL query template to use for the data exploration

        Args:
            sql_query_template (str): The SQL query template to use. The template might contain the following variables:
                - table_catalog: The table catalog name
                - table_schema: The table schema name
                - table_name: The table name
                - full_table_name: The full table name (catalog.schema.table)
                - stack_string_columns: A SQL expression that returns a table with two columns: column_name and string_value
        """
        new_obj = copy.deepcopy(self)
        new_obj._sql_query_template = sql_query_template
        return DataExplorerActions(new_obj, spark=self._spark, info_fetcher=self._info_fetcher)

    def unpivot_string_columns(self, sample_size=None) -> "DataExplorerActions":
        """Returns a DataExplorerActions object that will run a query that will melt all string columns into a pair of columns (column_name, string_value)

        Args:
            sample_size (int, optional): The number of rows to sample. Defaults to None (Return all rows).
        """

        sql_query_template = """
        SELECT
            {stack_string_columns} AS (column_name, string_value)
        FROM {full_table_name}
        """
        if sample_size is not None:
            sql_query_template += f"TABLESAMPLE ({sample_size} ROWS)"

        return self.with_sql(sql_query_template)

    def scan(
        self,
        rules="*",
        sample_size=10000,
        what_if: bool = False,
        custom_rules: Optional[List[Rule]] = None,
        locale: str = None,
    ):
        discover = Discovery(
            self._spark,
            self._catalogs,
            self._schemas,
            self._tables,
            self._info_fetcher.get_tables_info(self._catalogs, self._schemas, self._tables, self._having_columns),
            custom_rules=custom_rules,
            locale=locale,
        )
        discover.scan(rules=rules, sample_size=sample_size, what_if=what_if)
        return discover

    def map(self, f: Callable) -> list[any]:
        """Runs a function for each table in the data explorer

        Args:
            f (function): The function to run. The function should accept a TableInfo object as input and return any object as output.

        Returns:
            list[any]: A list of the results of running the function for each table
        """
        res = []
        table_list = self._info_fetcher.get_tables_info(
            self._catalogs,
            self._schemas,
            self._tables,
            self._having_columns,
            self._with_tags,
        )
        with concurrent.futures.ThreadPoolExecutor(max_workers=self._max_concurrency) as executor:
            # Submit tasks to the thread pool
            futures = [executor.submit(f, table_info) for table_info in table_list]

            # Process completed tasks
            for future in concurrent.futures.as_completed(futures):
                result = future.result()
                if result is not None:
                    res.append(result)

        logger.debug("Finished lakehouse map task")

        return res

    def map_chunked(self, f: Callable, tables_per_chunk: int, **kwargs) -> list[any]:
        """Runs a function for each table in the data explorer

        Args:
            f (function): The function to run. The function should accept either a list of TableInfo objects as input and return a list of any object as output.

        Returns:
            list[any]: A list of the results of running the function for each table
        """
        res = []
        table_list = self._info_fetcher.get_tables_info(
            self._catalogs,
            self._schemas,
            self._tables,
            self._having_columns,
            self._with_tags,
        )
        with concurrent.futures.ThreadPoolExecutor(max_workers=self._max_concurrency) as executor:
            # Submit tasks to the thread pool
            futures = [
                executor.submit(f, table_chunk, **kwargs) for table_chunk in more_itertools.chunked(table_list, tables_per_chunk)
            ]

            # Process completed tasks
            for future in concurrent.futures.as_completed(futures):
                result = future.result()
                if result is not None:
                    res.extend(result)

        logger.debug("Finished lakehouse map_chunked task")

        return res

    def delta_housekeeping(self) -> DataFrame:
        """

        """
        dh = DeltaHousekeeping()
        self.map(
            dh.scan
        )


class DataExplorerActions:
    def __init__(
        self,
        data_explorer: DataExplorer,
        spark: SparkSession = None,
        info_fetcher: InfoFetcher = None,
    ) -> None:
        self._data_explorer = data_explorer
        if spark is None:
            spark = SparkSession.builder.getOrCreate()
        if info_fetcher is None:
            info_fetcher = InfoFetcher(spark)
        self._info_fetcher = info_fetcher
        self._spark = spark

    @staticmethod
    def _get_stack_string_columns_expression(table_info: TableInfo) -> str:
        string_col_names = [c.name for c in table_info.columns if c.data_type.lower() == "string"]
        stack_parameters = ", ".join([f"'{c}', `{c}`" for c in string_col_names])
        return f"stack({len(string_col_names)}, {stack_parameters})"

    @staticmethod
    def _build_sql(sql_template: str, table_info: TableInfo) -> str:
        if table_info.catalog and table_info.catalog != "None":
            full_table_name = f"{table_info.catalog}.{table_info.schema}.{table_info.table}"
        else:
            full_table_name = f"{table_info.schema}.{table_info.table}"

        stack_string_columns = DataExplorerActions._get_stack_string_columns_expression(table_info)

        sql = sql_template.format(
            table_catalog=table_info.catalog,
            table_schema=table_info.schema,
            table_name=table_info.table,
            full_table_name=full_table_name,
            stack_string_columns=stack_string_columns,
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
            data_explorer._catalogs,
            data_explorer._schemas,
            data_explorer._tables,
            data_explorer._having_columns,
            data_explorer._with_tags,
        )
        sql_commands = [
            (
                DataExplorerActions._build_sql(data_explorer._sql_query_template, table),
                table,
            )
            for table in table_list
        ]
        return sql_commands

    def explain(self) -> None:
        """Prints a friendly explanation of the data exploration that will be performed

        The explanation will include the SQL query that will be executed, and the tables that will be scanned
        """
        column_filter_explanation = ""
        sql_explanation = ""

        if self._data_explorer._having_columns:
            column_filter_explanation = (
                f"only for tables that have all the following columns: {self._data_explorer._having_columns}"
            )
        if self._data_explorer._sql_query_template:
            sql_explanation = f"The SQL to be executed is (just a moment, generating it...):"

        explanation = f"""
        DiscoverX will apply the following SQL template

        {self._data_explorer._sql_query_template}

        to the tables in the following catalog, schema, table combinations:
        {self._data_explorer._from_tables}
        {column_filter_explanation}
        {sql_explanation}
        """
        logger.friendly(helper.strip_margin(explanation))

        detailed_explanation = ""
        if self._data_explorer._sql_query_template:
            sql_commands = self._get_sql_commands(self._data_explorer)
            for sql, table in sql_commands:
                detailed_explanation += f"""
                <p>For table: {table.catalog}.{table.schema}.{table.table}</p>
                <pre><code>{sql}</code></pre>
                """

            logger.friendlyHTML(detailed_explanation)

    def display(self) -> None:
        """Executes the data exploration queries and displays a sample of results"""
        return self.execute()

    def execute(self) -> None:
        """[DEPRECATED] Executes the data exploration queries and displays a sample of results"""
        df = self.to_union_dataframe()
        try:
            df.display()
        except Exception as e:
            df.show(truncate=False)

    def apply(self) -> DataFrame:
        """Executes the data exploration queries and returns a DataFrame with the results"""
        return self.to_union_dataframe()

    def to_union_dataframe(self) -> DataFrame:
        """[DEPRECATED] Executes the data exploration queries and returns a DataFrame with the results"""

        sql_commands = self._get_sql_commands(self._data_explorer)
        dfs = []
        with concurrent.futures.ThreadPoolExecutor(max_workers=self._data_explorer._max_concurrency) as executor:
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

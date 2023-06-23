from dataclasses import dataclass
import pandas as pd
import concurrent.futures
from pyspark.sql import SparkSession
from typing import Optional, List, Set

from discoverx.common.helper import strip_margin, format_regex
from discoverx import logging
from discoverx.constants import Conf
from discoverx.rules import Rules, RuleTypes

logger = logging.Logging()


@dataclass
class ColumnInfo:
    name: str
    data_type: str
    partition_index: int
    classes: list[str]


@dataclass
class TableInfo:
    catalog: Optional[str]
    schema: str
    table: str
    columns: list[ColumnInfo]

    def get_columns_by_class(self, class_name: str):
        return [ClassifiedColumn(col.name, class_name) for col in self.columns if class_name in col.classes]


@dataclass
class ClassifiedColumn:
    name: str
    class_name: str


@dataclass
class ScanContent:
    table_list: List[TableInfo]
    catalogs: Set[str]
    schemas: Set[str]

    @property
    def n_catalogs(self) -> int:
        return len(self.catalogs)

    @property
    def n_schemas(self) -> int:
        return len(self.schemas)

    @property
    def n_tables(self) -> int:
        return len(self.table_list)


@dataclass
class ScanResult:
    df: pd.DataFrame

    @property
    def is_empty(self) -> bool:
        return self.df.empty

    @property
    def n_scanned_columns(self) -> int:
        return len(self.df[["table_catalog", "table_schema", "table_name", "column_name"]].drop_duplicates())


class Scanner:

    def __init__(
        self,
        spark: SparkSession,
        rules: Rules,
        catalogs: str = "*",
        schemas: str = "*",
        tables: str = "*",
        rule_filter: str = "*",
        sample_size: int = 1000,
        what_if: bool = False,
    ):
        self.spark = spark
        self.rules = rules
        self.catalogs = catalogs
        self.schemas = schemas
        self.tables = tables
        self.rules_filter = rule_filter
        self.sample_size = sample_size
        self.what_if = what_if

        self.content: ScanContent = self._resolve_scan_content()
        self.rule_list = self.rules.get_rules(rule_filter=self.rules_filter)
        self.scan_result: Optional[ScanResult] = None

    def _get_list_of_tables(self) -> List[TableInfo]:
        table_list_sql = self._get_table_list_sql()

        rows = self.spark.sql(table_list_sql).collect()
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
            for row in rows
        ]
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
        FROM {Conf.COLUMNS_TABLE_NAME}
        WHERE 
            table_schema != "information_schema" 
            {catalog_sql if self.catalogs != "*" else ""}
            {schema_sql if self.schemas != "*" else ""}
            {table_sql if self.tables != "*" else ""}
        GROUP BY table_catalog, table_schema, table_name
        """

        return strip_margin(sql)

    def _resolve_scan_content(self) -> ScanContent:
        table_list = self._get_list_of_tables()
        catalogs = set(map(lambda x: x.catalog, table_list))
        schemas = set(map(lambda x: f"{x.catalog}.{x.schema}", table_list))

        return ScanContent(table_list, catalogs, schemas)

    def scan_table(self, table):
        try:
            if self.what_if:
                logger.friendly(f"SQL that would be executed for '{table.catalog}.{table.schema}.{table.table}'")
            else:
                logger.friendly(f"Scanning table '{table.catalog}.{table.schema}.{table.table}'")

            # Build rule matching SQL
            sql = self._rule_matching_sql(table)

            if self.what_if:
                logger.friendly(sql)
            else:
                # Execute SQL and return the result
                return self.spark.sql(sql).toPandas()
        except Exception as e:
            logger.error(f"Error while scanning table '{table.catalog}.{table.schema}.{table.table}': {e}")
            return None

    def scan(self):

        logger.friendly("""Ok, I'm going to scan your lakehouse for data that matches your rules.""")
        text = f"""
                This is what you asked for:

                    catalogs ({self.content.n_catalogs}) = {self.catalogs}
                    schemas ({self.content.n_schemas}) = {self.schemas}
                    tables ({self.content.n_tables}) = {self.tables}
                    rules ({len(self.rule_list)}) = {self.rules_filter}
                    sample_size = {self.sample_size}

                This may take a while, so please be patient. I'll let you know when I'm done.
                ...
                """

        logger.friendly(strip_margin(text))

        logger.debug("Launching lakehouse scanning task\n")

        if len(self.content.table_list) == 0:
            raise Exception("No tables found matching your filters")

        dfs = []
        with concurrent.futures.ThreadPoolExecutor(max_workers=Conf.MAX_WORKERS) as executor:
            # Submit tasks to the thread pool
            futures = [executor.submit(self.scan_table, table) for table in self.content.table_list]

            # Process completed tasks
            for future in concurrent.futures.as_completed(futures):
                result = future.result()
                if result is not None:
                    dfs.append(result)

        logger.debug("Finished lakehouse scanning task")

        if dfs:
            self.scan_result = ScanResult(df=pd.concat(dfs))
        else:
            raise Exception("No tables were scanned successfully.")

    def _rule_matching_sql(self, table_info: TableInfo):
        """
        Given a table and a set of rules this method will return a
        SQL expression which matches the table's columns against rules.
        If executed on the table using SQL the output will contain a
        matching frequency (probability) for each column and rule.
        Args:
            table_info (TableInfo): Specifies the table to be scanned

        Returns:
            string: The SQL expression

        """

        expressions = [r for r in self.rule_list if r.type == RuleTypes.REGEX]
        cols = [c for c in table_info.columns if c.data_type.lower() == "string"]

        if not cols:
            raise Exception(f"There are no columns of type string to be scanned in {table_info.table}")

        if not expressions:
            raise Exception(f"There are no rules to scan for.")

        catalog_str = f"{table_info.catalog}." if table_info.catalog else ""
        matching_columns = [
            f"INT(regexp_like(value, '{format_regex(r.definition)}')) AS `{r.name}`" for r in expressions
        ]
        matching_string = ",\n                    ".join(matching_columns)

        unpivot_expressions = ", ".join([f"'{r.name}', `{r.name}`" for r in expressions])
        unpivot_columns = ", ".join([f"'{c.name}', `{c.name}`" for c in cols])

        sql = f"""
            SELECT 
                '{table_info.catalog}' as table_catalog,
                '{table_info.schema}' as table_schema,
                '{table_info.table}' as table_name, 
                column_name,
                class_name,
                (sum(value) / count(value)) as frequency
            FROM
            (
                SELECT column_name, stack({len(expressions)}, {unpivot_expressions}) as (class_name, value)
                FROM 
                (
                    SELECT
                    column_name,
                    {matching_string}
                    FROM (
                        SELECT
                            stack({len(cols)}, {unpivot_columns}) AS (column_name, value)
                        FROM {catalog_str}{table_info.schema}.{table_info.table}
                        TABLESAMPLE ({self.sample_size} ROWS)
                    )
                )
            )
            GROUP BY table_catalog, table_schema, table_name, column_name, class_name
        """

        return strip_margin(sql)

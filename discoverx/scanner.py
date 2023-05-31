from dataclasses import dataclass
import pandas as pd
from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.types import _parse_datatype_string
from pyspark.sql.utils import ParseException
from typing import Optional, List, Set

from discoverx.common.helper import strip_margin, format_regex
from discoverx import logging
from discoverx.rules import Rules, RuleTypes

logger = logging.Logging()


@dataclass
class ColumnInfo:
    name: str
    data_type: str
    partition_index: int
    tags: list[str]


@dataclass
class TableInfo:
    catalog: Optional[str]
    database: str
    table: str
    columns: list[ColumnInfo]

    def get_columns_by_tag(self, tag: str):
        return [TaggedColumn(col.name, tag) for col in self.columns if tag in col.tags]


@dataclass
class TaggedColumn:
    name: str
    tag: str


@dataclass
class ScanContent:
    table_list: List[TableInfo]
    catalogs: Set[str]
    databases: Set[str]

    @property
    def n_catalogs(self) -> int:
        return len(self.catalogs)

    @property
    def n_databases(self) -> int:
        return len(self.databases)

    @property
    def n_tables(self) -> int:
        return len(self.table_list)


@dataclass
class ScanResult:
    df: pd.DataFrame

    @property
    def n_scanned_columns(self) -> int:
        return len(self.df[["catalog", "database", "table", "column"]].drop_duplicates())


class Scanner:

    COLUMNS_TABLE_NAME = "system.information_schema.columns"
    COMPLEX_TYPES = {StructType, ArrayType}

    def __init__(
        self,
        spark: SparkSession,
        rules: Rules,
        catalogs: str = "*",
        databases: str = "*",
        tables: str = "*",
        rule_filter: str = "*",
        sample_size: int = 1000,
        what_if: bool = False,
    ):
        self.spark = spark
        self.rules = rules
        self.catalogs = catalogs
        self.databases = databases
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
        database_sql = f"""AND regexp_like(table_schema, "^{self.databases.replace("*", ".*")}$")"""
        table_sql = f"""AND regexp_like(table_name, "^{self.tables.replace("*", ".*")}$")"""

        sql = f"""
        SELECT 
            table_catalog, 
            table_schema, 
            table_name, 
            collect_list(struct(column_name, data_type, partition_index)) as table_columns
        FROM {self.COLUMNS_TABLE_NAME}
        WHERE 
            table_schema != "information_schema" 
            {catalog_sql if self.catalogs != "*" else ""}
            {database_sql if self.databases != "*" else ""}
            {table_sql if self.tables != "*" else ""}
        GROUP BY table_catalog, table_schema, table_name
        """

        return strip_margin(sql)

    def _resolve_scan_content(self) -> ScanContent:
        table_list = self._get_list_of_tables()
        catalogs = set(map(lambda x: x.catalog, table_list))
        databases = set(map(lambda x: x.database, table_list))

        return ScanContent(table_list, catalogs, databases)

    def scan(self):

        logger.friendly("""Ok, I'm going to scan your lakehouse for data that matches your rules.""")
        text = f"""
                This is what you asked for:

                    catalogs ({self.content.n_catalogs}) = {self.catalogs}
                    databases ({self.content.n_databases}) = {self.databases}
                    tables ({self.content.n_tables}) = {self.tables}
                    rules ({len(self.rule_list)}) = {self.rules_filter}
                    sample_size = {self.sample_size}

                This may take a while, so please be patient. I'll let you know when I'm done.
                ...
                """

        logger.friendly(strip_margin(text))

        logger.debug("Launching lakehouse scanning task\n")

        dfs = []

        for i, table in enumerate(self.content.table_list):
            if self.what_if:
                logger.friendly(
                    f"SQL that would be executed for '{table.catalog}.{table.database}.{table.table}' ({i + 1}/{self.content.n_tables})"
                )
            else:
                logger.friendly(
                    f"Scanning table '{table.catalog}.{table.database}.{table.table}' ({i + 1}/{self.content.n_tables})"
                )

            try:
                # Build rule matching SQL
                sql = self._rule_matching_sql(table)

                if self.what_if:
                    logger.friendly(sql)
                else:
                    # Execute SQL and append result
                    dfs.append(self.spark.sql(sql).toPandas())
            except Exception as e:
                logger.error(f"Error while scanning table '{table.catalog}.{table.database}.{table.table}': {e}")
                continue

        logger.debug("Finished lakehouse scanning task")

        if dfs:
            self.scan_result = ScanResult(df=pd.concat(dfs).reset_index(drop=True))
        else:
            self.scan_result = ScanResult(df=pd.DataFrame())

    @staticmethod
    def backtick_col_name(col_name: str) -> str:
        col_name_splitted = col_name.split(".")
        return ".".join(["`" + col + "`" for col in col_name_splitted])

    def recursive_flatten_complex_type(self, col_name, schema, column_list):
        if type(schema) in self.COMPLEX_TYPES:
            iterable = schema
        elif type(schema) is StructField:
            iterable = schema.dataType
        elif schema == StringType():
            column_list.append({"col_name": col_name, "type": "string"})
            return column_list
        else:
            return column_list

        if type(iterable) is StructType:
            for field in iterable:
                if type(field.dataType) == StringType:
                    column_list.append(
                        {"col_name": self.backtick_col_name(col_name + "." + field.name), "type": "string"}
                    )
                elif type(field.dataType) in self.COMPLEX_TYPES:
                    column_list = self.recursive_flatten_complex_type(col_name + "." + field.name, field, column_list)
        elif type(iterable) is MapType:
            if type(iterable.valueType) not in self.COMPLEX_TYPES:
                column_list.append({"col_name": self.backtick_col_name(col_name), "type": "map_values"})
            if type(iterable.keyType) not in self.COMPLEX_TYPES:
                column_list.append({"col_name": self.backtick_col_name(col_name), "type": "map_keys"})
        elif type(iterable) is ArrayType:
            column_list.append({"col_name": self.backtick_col_name(col_name), "type": "array"})

        return column_list

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
        expr_pdf = pd.DataFrame([{"rule_name": r.name, "rule_definition": r.definition, "key": 0} for r in expressions])
        column_list = []
        for col in table_info.columns:
            try:
                data_type = _parse_datatype_string(col.data_type)
            except ParseException:
                data_type = None

            if data_type:
                self.recursive_flatten_complex_type(col.name, data_type, column_list)
        columns_pdf = pd.DataFrame(column_list)
        # # prepare for cross-join
        # columns_pdf["key"] = 0
        # # cross-join
        # col_expr_pdf = columns_pdf.merge(expr_pdf, on=["key"])
        #
        # def sum_expressions(row):
        #     if row.type == "string":
        #         return f"int(regexp_like({row.col_name}, '{row.rule_definition}'))"
        #     elif row.type == "array":
        #         return f"size(filter({row.col_name}, x -> x rlike '{row.rule_definition}'))"
        #     elif row.type == "map_values":
        #         return f"size(filter(map_values({row.col_name}), x -> x rlike '{row.rule_definition}'))"
        #     elif row.type == "map_keys":
        #         return f"size(filter(map_keys({row.col_name}), x -> x rlike '{row.rule_definition}'))"
        #     else:
        #         return None
        #
        # def count_expressions(row):
        #     if row.type == "string":
        #         return "1"
        #     elif row.type == "array":
        #         return f"size({row.col_name})"
        #     elif row.type == "map_values":
        #         return f"size(map_values({row.col_name}))"
        #     elif row.type == "map_keys":
        #         return f"size(map_keys({row.col_name}))"
        #     else:
        #         return None
        #
        # col_expr_pdf["sum_expression"] = col_expr_pdf.apply(sum_expressions, axis=1)
        # col_expr_pdf["count_expression"] = col_expr_pdf.apply(count_expressions, axis=1)
        # cols = [c for c in table_info.columns if c.data_type.lower() == "string"]
        if len(columns_pdf) == 0:
            raise Exception(f"There are no columns with supported types to be scanned in {table_info.table}")

        if not expressions:
            raise Exception(f"There are no rules to scan for.")

        string_cols = columns_pdf.loc[columns_pdf.type == "string", "col_name"].to_list()

        sql_list = []
        if len(string_cols) > 0:
            sql_list.append(self.string_col_sql(string_cols, expressions, table_info))

        array_cols = columns_pdf.loc[columns_pdf.type == "array", "col_name"].to_list()
        if len(array_cols) > 0:
            sql_list.append(self.array_col_sql(array_cols, expressions, table_info))

        all_sql = "\nUNION ALL \n".join(sql_list)
        return all_sql

    def string_col_sql(self, cols: List, expressions: List, table_info: TableInfo) -> str:
        catalog_str = f"{table_info.catalog}." if table_info.catalog else ""
        matching_columns = [
            f"INT(regexp_like(value, '{format_regex(r.definition)}')) AS `{r.name}`" for r in expressions
        ]
        matching_string = ",\n                    ".join(matching_columns)

        unpivot_expressions = ", ".join([f"'{r.name}', `{r.name}`" for r in expressions])
        unpivot_columns = ", ".join([f"'{c}', {c}" for c in cols])

        sql = f"""
                    SELECT 
                        '{table_info.catalog}' as catalog,
                        '{table_info.database}' as database,
                        '{table_info.table}' as table, 
                        column,
                        rule_name,
                        (sum(value) / count(value)) as frequency
                    FROM
                    (
                        SELECT column, stack({len(expressions)}, {unpivot_expressions}) as (rule_name, value)
                        FROM 
                        (
                            SELECT
                            column,
                            {matching_string}
                            FROM (
                                SELECT
                                    stack({len(cols)}, {unpivot_columns}) AS (column, value)
                                FROM {catalog_str}{table_info.database}.{table_info.table}
                                TABLESAMPLE ({self.sample_size} ROWS)
                            )
                        )
                    )
                    GROUP BY catalog, database, table, column, rule_name
                """
        return strip_margin(sql)

    def array_col_sql(self, cols: List, expressions: List, table_info: TableInfo) -> str:
        catalog_str = f"{table_info.catalog}." if table_info.catalog else ""
        matching_columns_sum = [
            f"size(filter(value, x -> x rlike '{r.definition}')) AS `{r.name}_sum`" for r in expressions
        ]
        matching_columns_count = [
            f"size(value) AS `{r.name}_count`" for r in expressions
        ]
        matching_columns = matching_columns_sum + matching_columns_count
        matching_string = ",\n                    ".join(matching_columns)

        unpivot_expressions = ", ".join([f"'{r.name}', `{r.name}_sum`, `{r.name}_count`" for r in expressions])
        unpivot_columns = ", ".join([f"'{c}', {c}" for c in cols])

        sql = f"""
                    SELECT 
                        '{table_info.catalog}' as catalog,
                        '{table_info.database}' as database,
                        '{table_info.table}' as table, 
                        column,
                        rule_name,
                        (sum(value_sum) / sum(value_count)) as frequency
                    FROM
                    (
                        SELECT column, stack({len(expressions)}, {unpivot_expressions}) as (rule_name, value_sum, value_count)
                        FROM 
                        (
                            SELECT
                            column,
                            {matching_string}
                            FROM (
                                SELECT
                                    stack({len(cols)}, {unpivot_columns}) AS (column, value)
                                FROM {catalog_str}{table_info.database}.{table_info.table}
                                TABLESAMPLE ({self.sample_size} ROWS)
                            )
                        )
                    )
                    GROUP BY catalog, database, table, column, rule_name
                """
        return strip_margin(sql)

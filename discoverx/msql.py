"""This module contains the M-SQL compiler"""
from dataclasses import dataclass
from functools import reduce
from discoverx import logging
from discoverx.scanner import ColumnInfo, TableInfo
from discoverx.common.helper import strip_margin
from fnmatch import fnmatch
from pyspark.sql.functions import lit
from pyspark.sql import DataFrame, SparkSession
import re
import itertools


@dataclass
class SQLRow:
    catalog: str
    schema: str
    table: str
    sql: str


class Msql:
    """This class compiles M-SQL expressions into regular SQL"""

    from_statement_expr = r"(FROM\s+)(([0-9a-zA-Z_\*]+).([0-9a-zA-Z_\*]+).([0-9a-zA-Z_\*]+))"
    from_components_expr = r"^(([0-9a-zA-Z_\*]+).([0-9a-zA-Z_\*]+).([0-9a-zA-Z_\*]+))$"
    command_expr = r"^\s*(\w+)\s"
    class_regex = r"\[([\w_-]+)\]"
    valid_commands = ["SELECT", "DELETE"]

    def __init__(self, msql: str) -> None:
        self.msql = msql

        # Find distinct classes in M-SQL expression
        self.classes = list(set(re.findall(self.class_regex, msql)))

        # Extract from clause components
        (self.catalogs, self.schemas, self.tables) = self._extract_from_components()

        # Extract command
        self.command = self._extract_command()

        self.logger = logging.Logging()

    def compile_msql(self, table_info: TableInfo) -> list[SQLRow]:
        """
        Compiles the M-SQL (Multiplex-SQL) expression into regular SQL
        Args:
            table_info (TableInfo): Table information

        Returns:
            list[string]: A list of SQL expressions which multiplexes the MSQL expression
        """

        # Replace from clause with table name
        msql = strip_margin(self.msql)
        msql = self._replace_from_statement(msql, table_info)

        # Get all columns matching the classes
        columns_by_class = [table_info.get_columns_by_class(class_name) for class_name in self.classes]

        # Create all possible combinations of classified columns to be queried
        col_class_combinations = list(itertools.product(*columns_by_class))

        # Replace classes in M-SQL expression with column names
        sql_statements = []
        for classified_cols in col_class_combinations:
            temp_sql = msql
            for classified_col in classified_cols:
                temp_sql = temp_sql.replace(f"[{classified_col.class_name}]", classified_col.name)
            sql_statements.append(SQLRow(table_info.catalog, table_info.schema, table_info.table, temp_sql))

        return sql_statements

    def build(self, classified_result_pdf) -> list[SQLRow]:
        """Builds the M-SQL expression into a SQL expression"""

        classified_cols = classified_result_pdf.copy()
        classified_cols = classified_cols[classified_cols["class_name"].isin(self.classes)]
        classified_cols = (
            classified_cols.groupby(["table_catalog", "table_schema", "table_name", "column_name"])
            .aggregate(lambda x: list(x))[["class_name"]]
            .reset_index()
        )

        classified_cols["col_classes"] = classified_cols[["column_name", "class_name"]].apply(tuple, axis=1)
        df = (
            classified_cols.groupby(["table_catalog", "table_schema", "table_name"])
            .aggregate(lambda x: list(x))[["col_classes"]]
            .reset_index()
        )

        # Filter tables by matching filter
        filtered_tables = [
            TableInfo(
                row[0],
                row[1],
                row[2],
                [ColumnInfo(col[0], "", None, col[1]) for col in row[3]],  # col name  # TODO  # TODO  # Classes
            )
            for _, row in df.iterrows()
            if fnmatch(row[0], self.catalogs) and fnmatch(row[1], self.schemas) and fnmatch(row[2], self.tables)
        ]

        if len(filtered_tables) == 0:
            raise ValueError(f"No tables found matching filter: {self.catalogs}.{self.schemas}.{self.tables}")

        sqls = flat_map(self.compile_msql, filtered_tables)

        return sqls

    def execute_sql_row(self, sql_row: SQLRow, spark: SparkSession) -> DataFrame:
        """Executes the SQL statement"""
        try:
            result = (
                spark.sql(sql_row.sql)
                .withColumn("table_catalog", lit(sql_row.catalog))
                .withColumn("table_schema", lit(sql_row.schema))
                .withColumn("table_name", lit(sql_row.table))
            )
            if self.command == "DELETE":
                result = result.withColumn("sql", lit(sql_row.sql))

            table_info_cols = ["table_catalog", "table_schema", "table_name"]
            select_cols = table_info_cols + [col for col in result.columns if col not in table_info_cols]
            result = result.select(*select_cols)

        except Exception as e:
            self.logger.info(f"Unable to execute SQL for {sql_row.catalog}.{sql_row.schema}.{sql_row.table}: {e}")
            result = None

        return result

    def execute_sql_rows(self, sqls: list[SQLRow], spark: SparkSession):
        """Executes the SQL statements"""
        results = [self.execute_sql_row(sql_row, spark) for sql_row in sqls]
        success_results = [result for result in results if result is not None]

        if len(success_results) == 0:
            raise ValueError(f"No SQL statements were successfully executed.")

        return reduce(lambda x, y: x.union(y), success_results)

    def _replace_from_statement(self, msql: str, table_info: TableInfo):
        """Replaces the FROM statement in the M-SQL expression with the specified table name"""
        if table_info.catalog and table_info.catalog != "None":
            replace_with = f"FROM {table_info.catalog}.{table_info.schema}.{table_info.table}"
        else:
            replace_with = f"FROM {table_info.schema}.{table_info.table}"

        return re.sub(self.from_statement_expr, replace_with, msql)

    def _extract_from_components(self):
        """Extracts the catalog, schema and table name from the FROM statement in the M-SQL expression"""
        matches = re.findall(self.from_statement_expr, self.msql)
        if len(matches) > 1:
            raise ValueError(f"Multiple FROM statements found in M-SQL expression: {self.msql}")
        elif len(matches) == 1:
            return (matches[0][2], matches[0][3], matches[0][4])
        else:
            raise ValueError(f"Could not extract table name from M-SQL expression: {self.msql}")

    @staticmethod
    def validate_from_components(from_tables: str):
        """Extracts the catalog, schema and table name from the from_table string"""
        matches = re.findall(Msql.from_components_expr, from_tables)
        if len(matches) == 1 and len(matches[0]) == 4:
            return (matches[0][1], matches[0][2], matches[0][3])
        else:
            raise ValueError(
                f"Invalid from_tables statement '{from_tables}'. Should be a string in format 'table_catalog.table_schema.table_name'. You can use '*' as wildcard."
            )

    def _extract_command(self):
        """Extracts the command from the M-SQL expression"""
        commands = re.findall(self.command_expr, self.msql)
        if len(commands) != 1:
            raise ValueError(
                f"Could not extract command from M-SQL expression: {self.msql}. Valid commands are SELECT and DELETE."
            )

        command = commands[0].upper()
        if command not in self.valid_commands:
            raise ValueError(f"Invalid command: {command}. Valid commands are SELECT and DELETE.")

        return command


def flat_map(f, xs):
    ys = []
    for x in xs:
        ys.extend(f(x))
    return ys

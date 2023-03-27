from dataclasses import dataclass, field
import pandas as pd
from pyspark.sql import SparkSession
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
            self.scan_result = ScanResult(df=pd.concat(dfs))
        else:
            self.scan_result = ScanResult(df=pd.DataFrame())

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
        matching_columns = [f"INT(regexp_like(value, '{format_regex(r.definition)}')) AS {r.name}" for r in expressions]
        matching_string = ",\n                    ".join(matching_columns)

        unpivot_expressions = ", ".join([f"'{r.name}', `{r.name}`" for r in expressions])
        unpivot_columns = ", ".join([f"'{c.name}', `{c.name}`" for c in cols])

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


@dataclass
class Classifier:
    classification_threshold: float
    scan_result: ScanResult
    classified_result: pd.DataFrame = field(init=False)

    def __post_init__(self):
        self.classified_result = self.scan_result.df[
            self.scan_result.df["frequency"] > self.classification_threshold
        ]

    @property
    def n_classified_columns(self) -> int:
        return len(self.classified_result[["catalog", "database", "table", "column"]].drop_duplicates())

    @property
    def rule_match_str(self) -> str:
        rule_match_counts = []
        df_summary = self.classified_result.groupby(["rule_name"]).agg({"frequency": "count"})
        df_summary = df_summary.reset_index()  # make sure indexes pair with number of rows
        for _, row in df_summary.iterrows():
            rule_match_counts.append(f"            <li>{row['frequency']} {row['rule_name']} columns</li>")
        return "\n".join(rule_match_counts)

    @property
    def summary_html(self) -> str:
        # Summary
        classified_cols = self.classified_result.copy()
        classified_cols.index = pd.MultiIndex.from_frame(classified_cols[["catalog", "database", "table", "column"]])
        summary_html_table = classified_cols[["rule_name", "frequency"]].to_html()

        return f"""
        <h2>Result summary</h2>
        <p>
          I've been able to classify {self.n_classified_columns} out of {self.scan_result.n_scanned_columns} columns.
        </p>
        <p>
          I've found:
          <ul>
            {self.rule_match_str}
          </ul>
        </p>
        <p>
          To be more precise:
        </p>
        {summary_html_table}
        <p>
          You can see the full classification output with 'dx.scan_result'.
        </p>


        """

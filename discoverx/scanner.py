from dataclasses import dataclass
from delta.tables import DeltaTable
import pandas as pd
import concurrent.futures
from pyspark.sql import SparkSession
import pyspark.sql.functions as func
from typing import Optional, List, Set
from pyspark.sql.utils import AnalysisException

from discoverx.common.helper import strip_margin, format_regex
from discoverx import logging
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
    spark: SparkSession

    @property
    def is_empty(self) -> bool:
        return self.df.empty

    @property
    def n_scanned_columns(self) -> int:
        return ScanResult.count_distinct_cols(self.df)

    @staticmethod
    def count_distinct_cols(df: pd.DataFrame) -> int:
        return len(df[["table_catalog", "table_schema", "table_name", "column_name"]].drop_duplicates())

    def n_classified_columns(self, min_score: Optional[float]) -> int:
        return ScanResult.count_distinct_cols(self.get_classes(min_score))

    def get_classes(self, min_score: Optional[float]):
        if self.df is None or self.df.empty:
            raise Exception("No scan result available. Please run dx.scan() or dx.load() first.")

        if min_score is None:
            return self.df[self.df["score"] > 0]
        elif (min_score >= 0) and (min_score <= 1):
            return self.df[self.df["score"] >= min_score]
        else:
            error_msg = f"min_score has to be either None or in interval [0,1]. Given value is {min_score}"
            logger.error(error_msg)
            raise ValueError(error_msg)

    def rule_match_str(self, min_score) -> str:
        rule_match_counts = []
        df_summary = self.get_classes(min_score=min_score).groupby(["class_name"]).agg({"score": "count"})
        df_summary = df_summary.reset_index()  # make sure indexes pair with number of rows
        for _, row in df_summary.iterrows():
            rule_match_counts.append(f"            <li>{row['score']} {row['class_name']} columns</li>")
        return "\n".join(rule_match_counts)

    def _create_databes_if_not_exists(self, scan_table_name: str):
        logger.friendly(f"The scan result table {scan_table_name} does not seem to exist. Trying to create it ...")
        (catalog, schema, table) = scan_table_name.split(".")
        try:
            self.spark.sql(f"DESCRIBE CATALOG {catalog}")
        except AnalysisException:
            self.spark.sql(f"CREATE CATALOG IF NOT EXISTS {catalog}")

        try:
            self.spark.sql(f"DESCRIBE DATABASE {catalog + '.' + schema}")
        except AnalysisException:
            self.spark.sql(f"CREATE DATABASE IF NOT EXISTS {catalog + '.' + schema}")

        self.spark.sql(
            f"CREATE TABLE IF NOT EXISTS {scan_table_name} (table_catalog string, table_schema string, table_name string, column_name string, class_name string, score double, effective_timestamp timestamp)"
        )
        logger.friendly(f"The scan result table {scan_table_name} has been created.")

    def _get_or_create_result_table_from_delta(self, scan_table_name: str) -> DeltaTable:
        try:
            return DeltaTable.forName(self.spark, scan_table_name)
        except Exception:
            self._create_databes_if_not_exists(scan_table_name)
            return DeltaTable.forName(self.spark, scan_table_name)

    def save(self, scan_table_name: str):
        scan_delta_table = self._get_or_create_result_table_from_delta(scan_table_name)

        scan_result_df = self.spark.createDataFrame(
            self.df,
            "table_catalog: string, table_schema: string, table_name: string, column_name: string, class_name: string, score: double",
        ).withColumn("effective_timestamp", func.current_timestamp())

        logger.friendly(f"Merging results into {scan_table_name}")

        scan_delta_table.alias("scan_delta_table").merge(
            scan_result_df.alias("scan_result_df"),
            "scan_delta_table.table_catalog = scan_result_df.table_catalog \
            and scan_delta_table.table_schema = scan_result_df.table_schema \
            and scan_delta_table.table_name = scan_result_df.table_name \
            and scan_delta_table.column_name = scan_result_df.column_name ",
        ).whenMatchedUpdateAll().whenNotMatchedInsertAll().execute()

    def load(self, scan_table_name: str):
        try:
            self.df = DeltaTable.forName(self.spark, scan_table_name).toDF().drop("effective_timestamp").toPandas()
        except Exception as e:
            logger.error(f"Error while reading the scan result table {scan_table_name}: {e}")
            raise e


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
        columns_table_name: str = "",
        max_workers: int = 10,
    ):
        self.spark = spark
        self.rules = rules
        self.catalogs = catalogs
        self.schemas = schemas
        self.tables = tables
        self.rules_filter = rule_filter
        self.sample_size = sample_size
        self.what_if = what_if
        self.columns_table_name = columns_table_name
        self.max_workers = max_workers

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
        FROM {self.columns_table_name}
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

    def scan_table(self, table: TableInfo):
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
        with concurrent.futures.ThreadPoolExecutor(max_workers=self.max_workers) as executor:
            # Submit tasks to the thread pool
            futures = [executor.submit(self.scan_table, table) for table in self.content.table_list]

            # Process completed tasks
            for future in concurrent.futures.as_completed(futures):
                result = future.result()
                if result is not None:
                    dfs.append(result)

        logger.debug("Finished lakehouse scanning task")

        if dfs:
            self.scan_result = ScanResult(df=pd.concat(dfs), spark=self.spark)
            return self.scan_result
        else:
            raise Exception("No tables were scanned successfully.")

    def _rule_matching_sql(self, table_info: TableInfo):
        """
        Given a table and a set of rules this method will return a
        SQL expression which matches the table's columns against rules.
        If executed on the table using SQL the output will contain a
        matching score (probability) for each column and rule.
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
                (sum(value) / count(value)) as score
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

    @property
    def summary_html(self) -> str:
        # Summary
        classified_cols = self.scan_result.get_classes(min_score=None)
        classified_cols.index = pd.MultiIndex.from_frame(
            classified_cols[["table_catalog", "table_schema", "table_name", "column_name"]]
        )
        summary_html_table = classified_cols[["class_name", "score"]].to_html()

        return f"""
        <h2>Result summary</h2>
        <p>
          I've been able to classify {self.scan_result.n_classified_columns(min_score=None)} out of {self.scan_result.n_scanned_columns} columns.
        </p>
        <p>
          I've found:
          <ul>
            {self.scan_result.rule_match_str(min_score=None)}
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

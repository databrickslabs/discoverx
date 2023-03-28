from delta.tables import DeltaTable
import pandas as pd
from pyspark.sql import SparkSession, DataFrame
import pyspark.sql.functions as func
from typing import Optional

from discoverx import logging
from discoverx.scanner import Scanner
from discoverx.inspection import InspectionTool

logger = logging.Logging()


class Classifier:
    def __init__(
        self,
        column_type_classification_threshold: float,
        scanner: Scanner,
        spark: SparkSession,
        classification_table_name: str,
    ):
        self.column_type_classification_threshold = column_type_classification_threshold
        self.scanner = scanner
        self.spark = spark
        self.classification_table_name = classification_table_name
        self.classification_table: Optional[DeltaTable] = None
        self.staged_updates_pdf: Optional[pd.DataFrame] = None
        self.inspection_tool: Optional[InspectionTool] = None

        # classify scan result based on threshold
        if self.scanner.scan_result is not None:
            self.classified_result = self.scanner.scan_result.df[
                self.scanner.scan_result.df["frequency"] > self.column_type_classification_threshold
            ].rename(
                columns={
                    "catalog": "table_catalog",
                    "database": "table_schema",
                    "table": "table_name",
                    "column": "column_name",
                }
            )
        else:
            raise Exception("No scan result available")

    def _get_classification_table_from_delta(self):
        (catalog, schema, table) = self.classification_table_name.split(".")
        self.spark.sql(f"CREATE CATALOG IF NOT EXISTS {catalog}")
        self.spark.sql(f"CREATE DATABASE IF NOT EXISTS {catalog + '.' + schema}")
        self.spark.sql(
            f"""
          CREATE TABLE IF NOT EXISTS {self.classification_table_name} (table_catalog string, table_schema string, table_name string, column_name string, rule_name string, tag_status string, effective_timestamp timestamp, current boolean, end_timestamp timestamp)
          """
        )
        return DeltaTable.forName(self.spark, self.classification_table_name)

    @property
    def n_classified_columns(self) -> int:
        return len(
            self.classified_result[["table_catalog", "table_schema", "table_name", "column_name"]].drop_duplicates()
        )

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
        classified_cols.index = pd.MultiIndex.from_frame(
            classified_cols[["table_catalog", "table_schema", "table_name", "column_name"]]
        )
        summary_html_table = classified_cols[["rule_name", "frequency"]].to_html()

        return f"""
        <h2>Result summary</h2>
        <p>
          I've been able to classify {self.n_classified_columns} out of {self.scanner.scan_result.n_scanned_columns} columns.
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

    def _get_staged_updates(self):
        self.classification_table = self._get_classification_table_from_delta()
        current_tags_df = (
            self.classification_table.toDF()
            .filter(func.col("current"))
            .drop("current", "end_timestamp", "effective_timestamp")
        )

        classified_df = self.spark.createDataFrame(
            self.classified_result,
            "table_catalog: string, table_schema: string, table_name: string, column_name: string, rule_name: string, frequency: double",
        ).select("table_catalog", "table_schema", "table_name", "column_name", "rule_name")

        #We need the eqNullSafe operator for testing purposes where
        #we don't have Unity Catalog and a 3-level namespace
        join_condition = (
                func.col("left_df.table_catalog").eqNullSafe(func.col("right_df.table_catalog"))
                & (func.col("left_df.table_schema") == func.col("right_df.table_schema"))
                & (func.col("left_df.table_name") == func.col("right_df.table_name"))
                & (func.col("left_df.column_name") == func.col("right_df.column_name"))
                & (func.col("left_df.rule_name") == func.col("right_df.rule_name"))
            )

        # dx tags which were previously set for a column which has not been classified in the
        # current scan or which has been added to the ignore-list should be unset
        to_be_unset_df = current_tags_df.alias("left_df").join(
            classified_df.alias("right_df"),
            join_condition,
            how="left_anti",
        ).select("left_df.*").withColumn("action", func.lit("unset"))

        # Set tag for columns which have been classified in current scan and which are not yet
        # tagged
        to_be_set_df = (
            classified_df.alias("left_df").join(
                current_tags_df.alias("right_df"),
                join_condition,
                how="left_anti",
            )
            .select("left_df.*")
            .withColumn("action", func.lit("set"))
            .withColumn("tag_status", func.lit("active"))
        )

        to_be_kept_df = current_tags_df.alias("left_df").join(
            classified_df.alias("right_df"),
            join_condition,
        ).select("left_df.*").withColumn("action", func.lit("keep"))

        # TODO: set current_timestamp first when merging
        staged_updates_df = to_be_set_df.alias("set").unionByName(
            to_be_unset_df.alias("unset").unionByName(to_be_kept_df.alias("keep"))
        )

        self.staged_updates_pdf = staged_updates_df.toPandas()

    def inspect(self):
        self._get_staged_updates()
        self.inspection_tool = InspectionTool(self.staged_updates_pdf)

    def save_tags(self):
        if self.inspection_tool is not None:
            # TODO: Can we make this smoother and with less data copies?
            self.staged_updates_pdf = self.inspection_tool.staged_updates_pdf
        else:
            self._get_staged_updates()

        staged_updates_df = self.spark.createDataFrame(
            self.staged_updates_pdf,
            "table_catalog: string, table_schema: string, table_name: string, column_name: string, rule_name: string, action: string, tag_status: string",
        )

        # TODO: Check if we can optimize performance by reusing some parts from _get_staged_updates
        current_tags_df = (
            self.classification_table.toDF()
            .filter(func.col("current"))
            .drop("current", "end_timestamp", "effective_timestamp")
        )
        staged_updates_df = staged_updates_df.filter(func.col("action") != "keep")

        # We need the eqNullSafe operator for testing purposes where
        # we don't have Unity Catalog and a 3-level namespace
        join_condition = (
                func.col("source.table_catalog").eqNullSafe(func.col("target.table_catalog"))
                & (func.col("source.table_schema") == func.col("target.table_schema"))
                & (func.col("source.table_name") == func.col("target.table_name"))
                & (func.col("source.column_name") == func.col("target.column_name"))
                & (func.col("source.rule_name") == func.col("target.rule_name"))
        )
        to_be_inserted_df = (
            staged_updates_df.alias("source")
            .filter(func.col("action") == "set")
            .join(
                current_tags_df.alias("target"),
                join_condition,
            )
            .filter("source.tag_status != target.tag_status")
        ).select("source.*")

        staged_updates_df = (
            staged_updates_df.withColumn("mergeKeyTable", func.col("table_name"))
            .unionByName(to_be_inserted_df.select("source.*").withColumn("mergeKeyTable", func.lit(None)))
            .withColumn("effective_timestamp", func.current_timestamp())
        )

        # merge using scd-typ2
        logger.friendly(f"Update classification table {self.classification_table_name}")

        self.classification_table.alias("target").merge(
            staged_updates_df.alias("source"),
            "target.table_catalog <=> source.table_catalog AND target.table_schema = source.table_schema AND target.table_name = source.mergeKeyTable AND target.column_name = source.column_name AND target.rule_name = source.rule_name AND target.current = true",
        ).whenMatchedUpdate(
            set={"current": "false", "end_timestamp": "source.effective_timestamp"}
        ).whenNotMatchedInsert(
            values={
                "table_catalog": "source.table_catalog",
                "table_schema": "source.table_schema",
                "table_name": "source.table_name",
                "column_name": "source.column_name",
                "rule_name": "source.rule_name",
                "tag_status": "source.tag_status",
                "effective_timestamp": "source.effective_timestamp",
                "current": "true",
                "end_timestamp": "null",
            }
        ).execute()

        # vacuum
        logger.debug("Vacuum classification table")
        self.classification_table.vacuum()

        # set unity catalog tags (private preview feature)
        logger.friendly("Set/Unset Unity Catalog Column Tags")
        self.staged_updates_pdf.apply(self._set_tag_uc, axis=1)

    def _set_tag_uc(self, series: pd.Series):
        if (series.action == "set") & (series.tag_status == "active"):
            logger.debug(
                f"Set tag {series.rule_name} for column {series.column_name} of table {series.table_catalog}.{series.table_schema}.{series.table_name}"
            )
            self.spark.sql(
                f"ALTER TABLE {series.table_catalog}.{series.table_schema}.{series.table_name} ALTER COLUMN {series.column_name} SET TAGS ('dx_{series.rule_name}')"
            )
        if (series.action == "unset") | (series.tag_status == "inactive"):
            logger.debug(
                f"Unset tag {series.rule_name} for column {series.column_name} of table {series.table_catalog}.{series.table_schema}.{series.table_name}"
            )
            self.spark.sql(
                f"ALTER TABLE {series.table_catalog}.{series.table_schema}.{series.table_name} ALTER COLUMN {series.column_name} UNSET TAGS ('dx_{series.rule_name}')"
            )

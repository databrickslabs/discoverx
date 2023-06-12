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
        classification_threshold: float,
        scanner: Scanner,
        spark: SparkSession,
        classification_table_name: str,
    ):
        self.classification_threshold = classification_threshold
        self.scanner = scanner
        self.spark = spark
        self.classification_table_name = classification_table_name
        self.classification_result: Optional[pd.DataFrame] = None
        self.inspection_tool: Optional[InspectionTool] = None
        self.staged_updates: Optional[pd.DataFrame] = None


    @property
    def above_threshold(self):
        # classify scan result based on threshold
        if self.scanner.scan_result is None:
            raise Exception("No scan result available")
        if self.scanner.scan_result.is_empty:
            return self.scanner.scan_result.df
        
        return self.scanner.scan_result.df[
            self.scanner.scan_result.df["frequency"] > self.classification_threshold
        ]
        

    def compute_classification_result(self):

        if self.above_threshold.empty:
            raise Exception(f"No columns with frequency above {self.classification_threshold} threshold.")
        
        classification_result = self.above_threshold.drop(columns=["frequency"])
        classification_result["status"] = "detected"
        classification_table = self._get_classification_table_from_delta()

        if classification_table is None:
            all_classes = classification_result
        else:
            current_classes = (
            classification_table.toDF()
            .filter(func.col("current"))
            .drop("current", "end_timestamp", "effective_timestamp")
            .select("*", func.lit("current").alias("status"))
            .toPandas()
            )

            all_classes = pd.concat([classification_result, current_classes])

        def aggregate_updates(pdf):
            current_classes = sorted(pdf.loc[pdf["status"] == "current", "class_name"].tolist())
            detected_classes = sorted(pdf.loc[pdf["status"] == "detected", "class_name"].tolist())
            published_classes = sorted(pdf.loc[:, "class_name"].unique().tolist())
            changed = current_classes != published_classes

            output = {
              "Current Classes": [current_classes],
              "Detected Classes": [detected_classes],
              "Classes to be published": [published_classes],
              "Classes changed": [changed]
            }

            return pd.DataFrame(output)
        
        if all_classes.empty:
            self.classification_result = pd.DataFrame(columns=["table_catalog", "table_schema", "table_name", "column_name", "Current Classes", "Detected Classes", "Classes to be published", "Classes changed"])
        else:
            self.classification_result = (all_classes
                                        .groupby(["table_catalog", "table_schema", "table_name", "column_name"], dropna=False, group_keys=True)
                                        .apply(aggregate_updates)
                                        .reset_index()
                                        .drop(columns=["level_4"])
            )
        # when testing we don't have a 3-level namespace but we need
        # to make sure we get None instead of NaN
        self.classification_result.table_catalog = self.classification_result.table_catalog.astype(object)
        self.classification_result.table_catalog = self.classification_result.table_catalog.where(pd.notnull(self.classification_result.table_catalog), None)


    def _get_classification_table_from_delta(self):
        try:
          return DeltaTable.forName(self.spark, self.classification_table_name)
        except Exception:
          return None
        
    def _get_or_create_classification_table_from_delta(self):
        try:
          return DeltaTable.forName(self.spark, self.classification_table_name)
        except Exception:
          logger.friendly(f"The classification table {self.classification_table_name} does not see to exist. Trying to create it ...")
          (catalog, schema, table) = self.classification_table_name.split(".")
          self.spark.sql(f"CREATE CATALOG IF NOT EXISTS {catalog}")
          self.spark.sql(f"CREATE DATABASE IF NOT EXISTS {catalog + '.' + schema}")
          self.spark.sql(
              f"""
            CREATE TABLE IF NOT EXISTS {self.classification_table_name} (table_catalog string, table_schema string, table_name string, column_name string, class_name string, effective_timestamp timestamp, current boolean, end_timestamp timestamp)
            """
          )
          logger.friendly(f"The classification table {self.classification_table_name} has been created.")
          return DeltaTable.forName(self.spark, self.classification_table_name)

    @property
    def n_classified_columns(self) -> int:
        return len(
            self.above_threshold[["table_catalog", "table_schema", "table_name", "column_name"]].drop_duplicates()
        )

    @property
    def rule_match_str(self) -> str:
        rule_match_counts = []
        df_summary = self.above_threshold.groupby(["class_name"]).agg({"frequency": "count"})
        df_summary = df_summary.reset_index()  # make sure indexes pair with number of rows
        for _, row in df_summary.iterrows():
            rule_match_counts.append(f"            <li>{row['frequency']} {row['class_name']} columns</li>")
        return "\n".join(rule_match_counts)

    @property
    def summary_html(self) -> str:
        # Summary
        classified_cols = self.above_threshold.copy()
        classified_cols.index = pd.MultiIndex.from_frame(
            classified_cols[["table_catalog", "table_schema", "table_name", "column_name"]]
        )
        summary_html_table = classified_cols[["class_name", "frequency"]].to_html()

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
          You can see the full classification output with 'dx.scan_result()'.
        </p>


        """

    def _stage_updates(self, input_classification_pdf: pd.DataFrame):
        
        classification_pdf = input_classification_pdf.copy()

        classification_pdf["to_be_unset"] = classification_pdf.apply(lambda x: list(set(x["Current Classes"]) - set(x["Classes to be published"])), axis=1)
        classification_pdf["to_be_set"] = classification_pdf.apply(lambda x: list(set(x["Classes to be published"]) - set(x["Current Classes"])), axis=1)
        classification_pdf["to_be_kept"] = classification_pdf.apply(lambda x: list(set(x["Classes to be published"]) & set(x["Current Classes"])), axis=1)

        self.staged_updates = pd.melt(classification_pdf, id_vars=["table_catalog", "table_schema", "table_name", "column_name"], value_vars=["to_be_unset", "to_be_set", "to_be_kept"], var_name="action", value_name="class_name").explode("class_name").dropna(subset=["class_name"]).reset_index(drop=True)


    def inspect(self):
        
        self.compute_classification_result()
        self.inspection_tool = InspectionTool(self.classification_result, self.publish)

    def publish(self):

        if self.inspection_tool is not None:
            self._stage_updates(self.inspection_tool.inspected_table)
        else:
            self.compute_classification_result()
            self._stage_updates(self.classification_result)
      
        staged_updates_df = self.spark.createDataFrame(
            self.staged_updates,
            "table_catalog: string, table_schema: string, table_name: string, column_name: string, action: string, class_name: string",
        ).withColumn("effective_timestamp", func.current_timestamp())
        # merge using scd-typ2
        logger.friendly(f"Update classification table {self.classification_table_name}")

        self._get_or_create_classification_table_from_delta().alias("target").merge(
            staged_updates_df.alias("source"),
            "target.table_catalog <=> source.table_catalog AND target.table_schema = source.table_schema AND target.table_name = source.table_name AND target.column_name = source.column_name AND target.class_name = source.class_name AND target.current = true",
        ).whenMatchedUpdate(
            condition = "source.action = 'to_be_unset'",
            set={"current": "false", "end_timestamp": "source.effective_timestamp"}
        ).whenNotMatchedInsert(
            values={
                "table_catalog": "source.table_catalog",
                "table_schema": "source.table_schema",
                "table_name": "source.table_name",
                "column_name": "source.column_name",
                "class_name": "source.class_name",
                "effective_timestamp": "source.effective_timestamp",
                "current": "true",
                "end_timestamp": "null",
            }
        ).execute()

        # vacuum
        logger.debug("Vacuum classification table")
        # self.classification_table.vacuum()


    def _set_tag_uc(self, series: pd.Series):
        if (series.action == "to_be_set"):
            logger.debug(
                f"Set tag {series.class_name} for column {series.column_name} of table {series.table_catalog}.{series.table_schema}.{series.table_name}"
            )
            if series.class_name != '':
              self.spark.sql(
                  f"ALTER TABLE {series.table_catalog}.{series.table_schema}.{series.table_name} ALTER COLUMN {series.column_name} SET TAGS ('dx_{series.class_name}')"
              )
        if (series.action == "to_be_unset"):
            logger.debug(
                f"Unset tag {series.class_name} for column {series.column_name} of table {series.table_catalog}.{series.table_schema}.{series.table_name}"
            )
            if series.class_name != '':
              self.spark.sql(
                  f"ALTER TABLE {series.table_catalog}.{series.table_schema}.{series.table_name} ALTER COLUMN {series.column_name} UNSET TAGS ('dx_{series.class_name}')"
              )

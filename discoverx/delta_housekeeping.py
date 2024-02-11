from typing import Iterable, Callable
from datetime import datetime, timezone
import pandas as pd

from discoverx.table_info import TableInfo

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.window import Window
import pyspark.sql.types as T
import pyspark.sql.functions as F


class DeltaHousekeeping:
    
    def __init__(self, spark: SparkSession) -> None:
        self._spark = spark
        self.empty_schema = T.StructType([
            T.StructField("catalog", T.StringType()),
            T.StructField("database", T.StringType()),
            T.StructField("tableName", T.StringType()),
        ])

    @staticmethod
    def _process_describe_history(
        describe_detail_df: DataFrame, describe_history_df: DataFrame
    ) -> pd.DataFrame:
        """
        processes the DESCRIBE HISTORY result of potentially several tables in different schemas/catalogs
        Provides
        - table stats (size and number of files)
        - timestamp for last & second last OPTIMIZE
        - stats of OPTIMIZE (including ZORDER)
        - timestamp for last & second last VACUUM

        returns a pandas DataFrame, and converts Spark internal dfs to pandas as soon as they are manageable
        the reason being that DESCRIBE HISTORY / DESCRIBE DETAIL cannot be cached
        """
        if not "operation" in describe_history_df.columns:
            return describe_detail_df.toPandas()

        # window over operation
        operation_order = (
            describe_history_df
            .filter(F.col("operation").isin(["OPTIMIZE", "VACUUM END"]))
            .withColumn("operation_order", F.row_number().over(
                Window.partitionBy(["catalog", "database", "tableName", "operation"]).orderBy(F.col("timestamp").desc())
            ))
        )

        if operation_order.isEmpty():
            return describe_detail_df.toPandas()

        operation_order = operation_order.toPandas()

        # max & 2nd timestamp of OPTIMIZE into output
        out = describe_detail_df.toPandas().merge(
            operation_order[(operation_order.operation == "OPTIMIZE") & (operation_order.operation_order == 1)]
            .loc[:, ["catalog", "database", "tableName", "timestamp"]]
            .rename(columns={'timestamp': 'max_optimize_timestamp'}),
            how="outer", on=["catalog", "database", "tableName"]
        )
        out = out.merge(
            operation_order[(operation_order.operation == "OPTIMIZE") & (operation_order.operation_order == 2)]
            .loc[:, ["catalog", "database", "tableName", "timestamp"]]
            .rename(columns={'timestamp': '2nd_optimize_timestamp'}),
            how="outer", on=["catalog", "database", "tableName"]
        )
        # max timestamp of VACUUM into output
        out = out.merge(
            operation_order[(operation_order.operation == "VACUUM END") & (operation_order.operation_order == 1)]
            .loc[:, ["catalog", "database", "tableName", "timestamp"]]
            .rename(columns={'timestamp': 'max_vacuum_timestamp'}),
            how="outer", on=["catalog", "database", "tableName"]
        )
        out = out.merge(
            operation_order[(operation_order.operation == "VACUUM END") & (operation_order.operation_order == 2)]
            .loc[:, ["catalog", "database", "tableName", "timestamp"]]
            .rename(columns={'timestamp': '2nd_vacuum_timestamp'}),
            how="outer", on=["catalog", "database", "tableName"]
        )
        # summary of table metrics
        table_metrics_1 = (
            operation_order[(operation_order['operation'] == 'OPTIMIZE') & (operation_order['operation_order'] == 1)]
            .loc[:, ['catalog', 'database', 'tableName', 'min_file_size', 'p50_file_size', 'max_file_size', 'z_order_by']]
        )

        # write to output
        out = out.merge(
            table_metrics_1,
            how="outer", on=["catalog", "database", "tableName"]
        )

        return out

    @staticmethod
    def save_as_table(
        result: DataFrame,
        housekeeping_table_name: str,
    ):
        """
        Static method to store intermediate results of the scan operation into Delta
        Would make sense only if using map_chunked from the `DataExplorer` object
        (otherwise tables are writen one by one into Delta with overhead)
        """
        (
            result
            .write
            .format("delta")
            .mode("append")
            .option("mergeSchema", "true")
            .saveAsTable(housekeeping_table_name)
        )

    def get_describe_detail(self, table_info: TableInfo):
        dd = self._spark.sql(f"""
            DESCRIBE DETAIL {table_info.catalog}.{table_info.schema}.{table_info.table};
        """)
        dd = (
            dd
            .withColumn("split", F.split(F.col('name'), '\.'))
            .withColumn("catalog", F.col("split").getItem(0))
            .withColumn("database", F.col("split").getItem(1))
            .withColumn("tableName", F.col("split").getItem(2))
            .select([
                F.col("catalog"),
                F.col("database"),
                F.col("tableName"),
                F.col("numFiles").alias("number_of_files"),
                F.col("sizeInBytes").alias("bytes"),
            ])
        )
        return dd

    @staticmethod
    def get_describe_history_statement(table_info: TableInfo):
        return f"""
                    SELECT 
                    '{table_info.catalog}' AS catalog,
                    '{table_info.schema}' AS database, 
                    '{table_info.table}' AS tableName, 
                    operation,
                    timestamp,
                    operationMetrics.minFileSize AS min_file_size,
                    operationMetrics.p50FileSize AS p50_file_size,
                    operationMetrics.maxFileSize AS max_file_size, 
                    operationParameters.zOrderBy AS z_order_by 
                    FROM (DESCRIBE HISTORY {table_info.catalog}.{table_info.schema}.{table_info.table})
                    WHERE operation in ('OPTIMIZE', 'VACUUM END')
                """

    def scan(
        self,
        table_info: TableInfo,
    ) -> pd.DataFrame:
        """
        Scans a table_info to fetch Delta stats
        - DESCRIBE DETAIL
        - DESCRIBE HISTORY
        """
        try:
            # runs a describe detail per table, figures out if exception
            dd = self.get_describe_detail(table_info)

            # prepares a DESCRIBE HISTORY statement per table (will be run outside the try-catch)
            statement = self.get_describe_history_statement(table_info)

            return self._process_describe_history(
                dd,
                self._spark.sql(statement),
            )

        except Exception as e:
            errors_df = self._spark.createDataFrame(
                [(table_info.catalog or "", table_info.schema, table_info.table, str(e))],
                ["catalog", "database", "tableName", "error"]
            )
            return errors_df.toPandas()


class DeltaHousekeepingActions:
    """
    Processes the output of the `DeltaHousekeeping` object to provide recommendations
    - tables that need to be OPTIMIZED/VACUUM'ed
    - are tables OPTIMIZED/VACUUM'ed often enough
    - tables that have small files / tables for which ZORDER is not being effective
    """

    def __init__(
        self,
        mapped_pd_dfs: Iterable[pd.DataFrame],
        spark: SparkSession = None,
        min_table_size_optimize: int = 128*1024*1024,  # i.e. 128 MB
        min_days_not_optimized: int = 7,  # in days
        min_days_not_vacuumed: int = 31,  # in days
        max_optimize_freq: int = 2,  # in days - e.g. 2 means that a daily run would be flagged
        max_vacuum_freq: int = 2,  # in days - e.g. 2 means that a daily run would be flagged
        small_file_threshold: int = 32*1024*1024,  # i.e. 32 MB
        min_number_of_files_for_zorder: int = 8,
        stats: pd.DataFrame = None,  # for testability only
    ) -> None:
        if spark is None:
            spark = SparkSession.builder.getOrCreate()
        self._spark = spark

        if stats is None:
            self._mapped_pd_dfs = mapped_pd_dfs
            stats = pd.concat(self._mapped_pd_dfs)
        self._stats_df: DataFrame = self._spark.createDataFrame(stats)
        self._stats_rec: DataFrame = None

        self.min_table_size_optimize = min_table_size_optimize
        self.min_days_not_optimized = min_days_not_optimized
        self.min_days_not_vacuumed = min_days_not_vacuumed
        self.max_optimize_freq = max_optimize_freq
        self.max_vacuum_freq = max_vacuum_freq
        self.small_file_threshold = small_file_threshold
        self.min_number_of_files_for_zorder = min_number_of_files_for_zorder
        self.reason_col_suffix = "_reason"
        self.recomendations_dict = {
            "not_optimized": {
                "legend": "Tables that have never been OPTIMIZED and would benefit from it",
                "col_name": "rec_not_optimized"
            },
            "not_vacuumed": {
                "legend": "Tables that have never been VACUUM'ed",
                "col_name": "rec_not_vacuumed"
            },
            "not_optimized_last_days": {
                "legend": "Tables that are not OPTIMIZED often enough",
                "col_name": "rec_not_optimized_last_days"
            },
            "not_vacuumed_last_days": {
                "legend": "Tables that are not VACUUM'ed often enough",
                "col_name": "rec_not_vacuumed_last_days"
            },
            "optimized_too_freq": {
                "legend": "Tables that are OPTIMIZED too often",
                "col_name": "rec_optimized_too_freq"
            },
            "vacuumed_too_freq": {
                "legend": "Tables that are VACUUM'ed too often",
                "col_name": "rec_vacuumed_too_freq"
            },
            "do_not_need_optimize": {
                "legend": "Tables that are too small to be OPTIMIZED",
                "col_name": "rec_do_not_need_optimize"
            },
            "to_analyze": {
                "legend": "Tables that need more analysis -small_files",
                "col_name": "rec_to_analyze"
            },
            "zorder_not_effective": {
                "legend": "Tables for which ZORDER is not being effective",
                "col_name": "rec_zorder_not_effective"
            },
        }

    def _need_optimize(self, stats_rec: DataFrame) -> DataFrame:
        conf_dict = self.recomendations_dict["not_optimized"]
        return stats_rec.withColumn(
            conf_dict["col_name"],
            F.when(
                (F.col("max_optimize_timestamp").isNull() | F.isnan("max_optimize_timestamp")) &
                F.col("bytes").isNotNull() & (F.col("bytes").astype("int") > F.lit(self.min_table_size_optimize)),
                F.lit(True)
            ).otherwise(F.lit(False))
        ).withColumn(
            conf_dict["col_name"] + self.reason_col_suffix, F.lit(conf_dict["legend"])
        )

    def _optimize_not_needed(self, stats_rec: DataFrame) -> DataFrame:
        conf_dict = self.recomendations_dict["do_not_need_optimize"]
        return stats_rec.withColumn(
            conf_dict["col_name"],
            F.when(
                F.col("max_optimize_timestamp").isNotNull() & ~F.isnan(F.col("max_optimize_timestamp")) &
                F.col("bytes").isNotNull() &
                (F.col("bytes").astype("int") < F.lit(self.min_table_size_optimize)),
                F.lit(True)
            ).otherwise(F.lit(False))
        ).withColumn(
            conf_dict["col_name"] + self.reason_col_suffix, F.lit(conf_dict["legend"])
        )

    def _not_optimized_last_days(self, stats_rec: DataFrame) -> DataFrame:
        conf_dict = self.recomendations_dict["not_optimized_last_days"]
        return stats_rec.withColumn(
            "optimize_lag",
            F.date_diff(F.lit(datetime.today()), F.col("max_optimize_timestamp"))
        ).withColumn(
            conf_dict["col_name"],
            F.when(
                F.col("optimize_lag") > F.lit(self.min_days_not_optimized),
                F.lit(True)
            ).otherwise(F.lit(False))
        ).withColumn(
            conf_dict["col_name"] + self.reason_col_suffix, F.lit(conf_dict["legend"])
        )

    def _optimized_too_frequently(self, stats_rec: DataFrame) -> DataFrame:
        conf_dict = self.recomendations_dict["optimized_too_freq"]
        return stats_rec.withColumn(
            "optimize_freq",
            F.when(
                F.col("max_optimize_timestamp").isNotNull() & ~F.isnan(F.col("max_optimize_timestamp")) &
                F.col("2nd_optimize_timestamp").isNotNull() & ~F.isnan(F.col("2nd_optimize_timestamp")),
                F.date_diff(F.col("max_optimize_timestamp"), F.col("2nd_optimize_timestamp"))
            )
        ).withColumn(
            conf_dict["col_name"],
            F.when(
                F.col("optimize_freq") < F.lit(self.max_optimize_freq),
                F.lit(True)
            ).otherwise(F.lit(False))
        ).withColumn(
            conf_dict["col_name"] + self.reason_col_suffix, F.lit(conf_dict["legend"])
        )

    def _never_vacuumed(self, stats_rec: DataFrame) -> DataFrame:
        conf_dict = self.recomendations_dict["not_vacuumed"]
        return stats_rec.withColumn(
            conf_dict["col_name"],
            F.when(
                F.col("max_vacuum_timestamp").isNull() | F.isnan("max_vacuum_timestamp"),
                F.lit(True)
            ).otherwise(F.lit(False))
        ).withColumn(
            conf_dict["col_name"] + self.reason_col_suffix, F.lit(conf_dict["legend"])
        )

    def _not_vacuumed_last_days(self, stats_rec: DataFrame) -> DataFrame:
        conf_dict = self.recomendations_dict["not_vacuumed_last_days"]
        return stats_rec.withColumn(
            "vacuum_lag",
            F.date_diff(F.lit(datetime.today()), F.col("max_vacuum_timestamp"))
        ).withColumn(
            conf_dict["col_name"],
            F.when(
                F.col("vacuum_lag") > F.lit(self.min_days_not_vacuumed),
                F.lit(True)
            ).otherwise(F.lit(False))
        ).withColumn(
            conf_dict["col_name"] + self.reason_col_suffix, F.lit(conf_dict["legend"])
        )

    def _vacuumed_too_frequently(self, stats_rec: DataFrame) -> DataFrame:
        conf_dict = self.recomendations_dict["vacuumed_too_freq"]
        return stats_rec.withColumn(
            "vacuum_freq",
            F.when(
                F.col("max_vacuum_timestamp").isNotNull() & ~F.isnan(F.col("max_vacuum_timestamp")) &
                F.col("2nd_vacuum_timestamp").isNotNull() & ~F.isnan(F.col("2nd_vacuum_timestamp")),
                F.date_diff(F.col("max_vacuum_timestamp"), F.col("2nd_vacuum_timestamp"))
            )
        ).withColumn(
            conf_dict["col_name"],
            F.when(
                F.col("vacuum_freq") < F.lit(self.max_vacuum_freq),
                F.lit(True)
            ).otherwise(F.lit(False))
        ).withColumn(
            conf_dict["col_name"] + self.reason_col_suffix, F.lit(conf_dict["legend"])
        )

    def _analyze_these_tables(self, stats_rec: DataFrame) -> DataFrame:
        conf_dict = self.recomendations_dict["to_analyze"]
        return stats_rec.withColumn(
            conf_dict["col_name"],
            F.when(
                F.col("max_optimize_timestamp").isNotNull() & ~F.isnan(F.col("max_optimize_timestamp")) &
                F.col("p50_file_size").isNotNull() & (F.col("number_of_files") > F.lit(1)) &
                (F.col("p50_file_size").astype("int") < F.lit(self.small_file_threshold)),
                F.lit(True)
            ).otherwise(F.lit(False))
        ).withColumn(
            conf_dict["col_name"] + self.reason_col_suffix, F.lit(conf_dict["legend"])
        )

    def _zorder_not_effective(self, stats_rec: DataFrame) -> DataFrame:
        conf_dict = self.recomendations_dict["zorder_not_effective"]
        return stats_rec.withColumn(
            "z_order_by_clean",
            F.when(
                F.col("max_optimize_timestamp").isNull() | F.isnan(F.col("max_optimize_timestamp")) |
                F.col("p50_file_size").isNull() | (F.col("z_order_by") == "[]"),
                None
            ).otherwise(
                F.regexp_replace(
                    F.regexp_replace(
                        F.regexp_replace(
                            F.col("z_order_by"), "\\[", ""
                        ), "\\]", ""
                    ), '"', ""
                )
            )
        ).withColumn(
            "z_order_by_array", F.split(F.col("z_order_by_clean"), ",")
        ).withColumn(
            conf_dict["col_name"],
            F.when(
                (F.size(F.col("z_order_by_array")) > 0) &
                (F.col("number_of_files") < F.lit(self.min_number_of_files_for_zorder)),
                F.lit(True)
            ).otherwise(F.lit(False))
        ).withColumn(
            conf_dict["col_name"] + self.reason_col_suffix, F.lit(conf_dict["legend"])
        )

    def display(self) -> None:
        """Executes the Delta housekeeping analysis and displays a sample of results"""
        return self.apply().display()

    def apply(self) -> DataFrame:
        """Displays recommendations in a DataFrame format"""
        return self.generate_recommendations()

    def generate_recommendations(self) -> DataFrame:
        """
        Generates Delta Housekeeping recommendations as a list of dictionaries (internal use + unit tests only)
        A dict per recommendation where:
        - The key is the legend of the recommendation
        - The value is a pandas df with the affected tables
        """
        if self._stats_rec is None:
            stats_rec = self._need_optimize(self._stats_df)
            stats_rec = self._never_vacuumed(stats_rec)
            stats_rec = self._not_optimized_last_days(stats_rec)
            stats_rec = self._not_vacuumed_last_days(stats_rec)
            stats_rec = self._optimized_too_frequently(stats_rec)
            stats_rec = self._vacuumed_too_frequently(stats_rec)
            stats_rec = self._optimize_not_needed(stats_rec)
            stats_rec = self._analyze_these_tables(stats_rec)
            stats_rec = self._zorder_not_effective(stats_rec)
            self._stats_rec = stats_rec

        return self._stats_rec

    def _explain(self) -> Iterable[dict]:
        stats = self.generate_recommendations()
        out = []
        for _, item in self.recomendations_dict.items():
            legend = None
            col_name = None
            for k, v in item.items():
                if k == "legend":
                    legend = v
                elif k == "col_name":
                    col_name = v

            out.append({
                legend: stats.filter(F.col(col_name) & F.col(col_name + self.reason_col_suffix).rlike(legend))
            })

        return out

    def explain(self) -> None:
        from databricks.sdk.runtime import display

        for item in self._explain():
            for legend, df in item.items():
                display(legend)
                display(df)

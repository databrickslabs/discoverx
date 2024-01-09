from typing import Iterable
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
        min_days_not_optimized: int = 7,
        min_days_not_vacuumed: int = 31,
        max_optimize_freq: int = 2,
        max_vacuum_freq: int = 2,
        small_file_threshold: int = 32*1024*1024,  # i.e. 32 MB
        min_number_of_files_for_zorder: int = 8,
        stats: pd.DataFrame = None,  # for testability only
    ) -> None:
        if stats is None:
            self._mapped_pd_dfs = mapped_pd_dfs
            stats = pd.concat(self._mapped_pd_dfs)
        self._stats: pd.DataFrame = stats
        
        if spark is None:
            spark = SparkSession.builder.getOrCreate()
        self._spark = spark
        
        self.min_table_size_optimize = min_table_size_optimize
        self.min_days_not_optimized = min_days_not_optimized
        self.min_days_not_vacuumed = min_days_not_vacuumed
        self.max_optimize_freq = max_optimize_freq
        self.max_vacuum_freq = max_vacuum_freq
        self.small_file_threshold = small_file_threshold
        self.min_number_of_files_for_zorder = min_number_of_files_for_zorder
        self.tables_not_optimized_legend = "Tables that are never OPTIMIZED and would benefit from it"
        self.tables_not_vacuumed_legend = "Tables that are never VACUUM'ed"
        self.tables_not_optimized_last_days = "Tables that are not OPTIMIZED often enough"
        self.tables_not_vacuumed_last_days = "Tables that are not VACUUM'ed often enough"
        self.tables_optimized_too_freq = "Tables that are OPTIMIZED too often"
        self.tables_vacuumed_too_freq = "Tables that are VACUUM'ed too often"
        self.tables_do_not_need_optimize = "Tables that are too small to be OPTIMIZED"
        self.tables_to_analyze = "Tables that need more analysis (small_files)"
        self.tables_zorder_not_effective = "Tables for which ZORDER is not being effective"

    def _need_optimize(self) -> pd.DataFrame:
        stats = self._stats.copy()
        stats = stats.loc[stats.max_optimize_timestamp.isnull() & stats.bytes.notnull()]
        return (
            stats.loc[(stats.bytes.astype(int) > self.min_table_size_optimize)]
        )

    def _optimize_not_needed(self) -> pd.DataFrame:
        stats = self._stats.copy()
        stats = stats.loc[stats.max_optimize_timestamp.isnull() & stats.bytes.notnull()]
        return (
            stats.loc[stats.max_optimize_timestamp.notnull() & (stats.bytes.astype(int) > self.min_table_size_optimize)]
        )

    def _not_optimized_last_days(self) -> pd.DataFrame:
        stats = self._stats.copy()
        stats['max_optimize_timestamp'] = pd.to_datetime(stats['max_optimize_timestamp'], utc=True)
        stats['optimize_lag'] = (
            datetime.now(timezone.utc) - stats['max_optimize_timestamp']
        ).dt.days
        return (
            stats[stats['optimize_lag'] < self.min_days_not_optimized]
        )

    def _optimized_too_frequently(self) -> pd.DataFrame:
        stats = self._stats.copy()
        stats['max_optimize_timestamp'] = pd.to_datetime(stats['max_optimize_timestamp'])
        stats['2nd_optimize_timestamp'] = pd.to_datetime(stats['2nd_optimize_timestamp'])
        stats['optimize_lag'] = (stats['max_optimize_timestamp'] - stats['2nd_optimize_timestamp']).dt.days
        return (
            stats[stats['optimize_lag'] < self.max_optimize_freq]
        )

    def _never_vacuumed(self) -> pd.DataFrame:
        stats = self._stats
        return (
            stats.loc[stats.max_vacuum_timestamp.isnull()]
        )

    def _not_vacuumed_last_days(self) -> pd.DataFrame:
        stats = self._stats.copy()
        stats['max_vacuum_timestamp'] = pd.to_datetime(stats['max_vacuum_timestamp'], utc=True)
        stats['vacuum_lag'] = (
            datetime.now(timezone.utc) - stats['max_vacuum_timestamp']
        ).dt.days
        return (
            stats[stats['vacuum_lag'] < self.min_days_not_vacuumed]
        )

    def _vacuumed_too_frequently(self) -> pd.DataFrame:
        stats = self._stats.copy()
        stats['max_vacuum_timestamp'] = pd.to_datetime(stats['max_vacuum_timestamp'])
        stats['2nd_vacuum_timestamp'] = pd.to_datetime(stats['2nd_vacuum_timestamp'])
        stats['vacuum_lag'] = (stats['max_vacuum_timestamp'] - stats['2nd_vacuum_timestamp']).dt.days
        return (
            stats[stats['vacuum_lag'] < self.max_vacuum_freq]
        )

    def _analyze_these_tables(self) -> pd.DataFrame:
        stats = self._stats.copy()
        stats = stats.loc[stats['max_optimize_timestamp'].notnull() &
                          stats['p50_file_size'].notnull() &
                          (stats['number_of_files'] > 1)]
        stats = stats.loc[(stats['p50_file_size'].astype(int) < self.small_file_threshold)]
        return (
            stats.sort_values(by=['database', 'tableName', 'number_of_files'], ascending=[True, True, False])
        )

    def _zorder_not_effective(self) -> pd.DataFrame:
        stats = self._stats.copy()
        stats = stats.loc[stats['max_optimize_timestamp'].notnull() &
                          stats['p50_file_size'].notnull()]

        # clean up z_order_by column and split into array
        stats['z_order_by_clean'] = stats['z_order_by'].apply(
            lambda x: None if x == "[]" else x.replace('[', '').replace(']', '').replace('"', ''))
        stats['z_order_by_array'] = stats['z_order_by_clean'].str.split(',')

        # filter rows with zorder columns and number_of_files is less than threshold
        stats = stats[stats['z_order_by_array'].str.len() > 0]
        stats = stats[stats['number_of_files'].astype(int) < self.min_number_of_files_for_zorder]
        return (
            stats
        )

    def stats(self) -> DataFrame:
        """Ouputs the stats per table"""
        return self._spark.createDataFrame(self._stats)

    def display(self) -> None:
        """Executes the Delta housekeeping analysis and displays a sample of results"""
        return self.apply().display()

    def apply(self) -> DataFrame:
        """Displays recommendations in a DataFrame format"""
        out = None
        for recomm in self.generate_recommendations():
            for legend, df in recomm.items():
                out_df = self._spark.createDataFrame(df).withColumn("recommendation", F.lit(legend))
                if out is None:
                    out = out_df
                else:
                    out = out.unionByName(out_df, allowMissingColumns=True)
        return out

    def generate_recommendations(self) -> Iterable[dict]:
        """
        Generates Delta Housekeeping recommendations as a list of dictionaries (internal use + unit tests only)
        A dict per recommendation where:
        - The key is the legend of the recommendation
        - The value is a pandas df with the affected tables
        """
        out = []
        for df, legend in zip([
            self._need_optimize(),
            self._never_vacuumed(),
            self._not_optimized_last_days(),
            self._not_vacuumed_last_days(),
            self._optimized_too_frequently(),
            self._vacuumed_too_frequently(),
            self._optimize_not_needed(),
            self._analyze_these_tables(),
            self._zorder_not_effective(),
        ], [
            self.tables_not_optimized_legend,
            self.tables_not_vacuumed_legend,
            self.tables_not_optimized_last_days,
            self.tables_not_vacuumed_last_days,
            self.tables_optimized_too_freq,
            self.tables_vacuumed_too_freq,
            self.tables_do_not_need_optimize,
            self.tables_to_analyze,
            self.tables_zorder_not_effective,
        ]):
            if not df.empty:
                out.append({legend: df})
        return out

    def explain(self) -> None:
        # TODO better formatting!
        from databricks.sdk.runtime import display


        for recomm in self.generate_recommendations():
            for legend, df in recomm.items():
                display(legend)
                display(df)

from typing import Iterable
from functools import reduce
from datetime import datetime
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
    ) -> DataFrame:
        """
        processes the DESCRIBE HISTORY result of potentially several tables in different schemas/catalogs
        Provides
        - table stats (size and number of files)
        - timestamp for last & second last OPTIMIZE
        - stats of OPTIMIZE (including ZORDER)
        - timestamp for last & second last VACUUM

        TODO reconsider if it is better outside of the class
        """
        if not "operation" in describe_history_df.columns:
            return describe_detail_df

        # window over operation
        operation_order = (
            describe_history_df
            .filter(F.col("operation").isin(["OPTIMIZE", "VACUUM END"]))
            .withColumn("operation_order", F.row_number().over(
                Window.partitionBy(["catalog", "database", "tableName", "operation"]).orderBy(F.col("timestamp").desc())
            ))
        )
        # max & 2nd timestamp of OPTIMIZE into output
        out = describe_detail_df.join(
            operation_order
            .filter((F.col("operation") == "OPTIMIZE") & (F.col("operation_order") == 1))
            .select("catalog", "database", "tableName", "timestamp")
            .withColumnRenamed("timestamp", "max_optimize_timestamp"),
            how="outer", on=["catalog", "database", "tableName"]
        )
        out = out.join(
            operation_order
            .filter((F.col("operation") == "OPTIMIZE") & (F.col("operation_order") == 2))
            .select("catalog", "database", "tableName", "timestamp")
            .withColumnRenamed("timestamp", "2nd_optimize_timestamp"),
            how="outer", on=["catalog", "database", "tableName"]
        )
        # max timestamp of VACUUM into output
        out = out.join(
            operation_order
            .filter((F.col("operation") == "VACUUM END") & (F.col("operation_order") == 1))
            .select("catalog", "database", "tableName", "timestamp")
            .withColumnRenamed("timestamp", "max_vacuum_timestamp"),
            how="outer", on=["catalog", "database", "tableName"]
        )
        out = out.join(
            operation_order
            .filter((F.col("operation") == "VACUUM END") & (F.col("operation_order") == 2))
            .select("catalog", "database", "tableName", "timestamp")
            .withColumnRenamed("timestamp", "2nd_vacuum_timestamp"),
            how="outer", on=["catalog", "database", "tableName"]
        )
        # summary of table metrics
        table_metrics_1 = (
            operation_order.filter((F.col("operation") == "OPTIMIZE") & (F.col("operation_order") == 1))
            .select([
                F.col("catalog"),
                F.col("database"),
                F.col("tableName"),
                F.col("min_file_size"),
                F.col("p50_file_size"),
                F.col("max_file_size"),
                F.col("z_order_by"),
            ])
        )

        # write to output
        out = out.join(
            table_metrics_1,
            how="outer", on=["catalog", "database", "tableName"]
        )

        return out

    def scan(
        self,
        table_info_list: Iterable[TableInfo],
        housekeeping_table_name: str = "lorenzorubi.default.housekeeping_summary_v2",  # TODO remove
        do_save_as_table: bool = True,
    ) -> pd.DataFrame:
        """
        Scans a table_info / table_info_list to fetch Delta stats
        - DESCRIBE DETAIL
        - DESCRIBE HISTORY
        """
        dd_list = []
        statements = []
        errors = []

        if not isinstance(table_info_list, Iterable):
            table_info_list = [table_info_list]

        for table_info in table_info_list:
            try:
                # runs a describe detail per table, figures out if exception
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
                dd_list.append(dd)

                # prepares a DESCRIBE HISTORY statement per table (will be run outside of the loop)
                statements.append(f"""
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
                """)
            except Exception as e:
                errors.append(self._spark.createDataFrame(
                    [(table_info.catalog, table_info.schema, table_info.table, str(e))],
                    ["catalog", "database", "tableName", "error"]
                ))

        # statement to UNION all DESCRIBE HISTORY together
        statement = " UNION ".join(statements)

        dh = self._spark.createDataFrame([], self.empty_schema)
        if statements:
            dh = self._process_describe_history(
                reduce(
                    lambda left, right: left.union(right),
                    dd_list
                ),
                self._spark.sql(statement),
            )

        errors_df = self._spark.createDataFrame([], self.empty_schema)
        if errors:
            errors_df = reduce(
                lambda left, right: left.union(right),
                errors
            )

        out = dh.unionByName(errors_df, allowMissingColumns=True)

        if do_save_as_table:
            (
                out
                .write
                .format("delta")
                .mode("append")
                .option("mergeSchema", "true")
                .saveAsTable(housekeeping_table_name)
            )

        return out.toPandas()


class DeltaHousekeepingActions:
    def __init__(
        self,
        # delta_housekeeping: DeltaHousekeeping,
        mapped_pd_dfs: Iterable[pd.DataFrame],
        # spark: SparkSession = None,
        min_table_size_optimize: int = 128*1024*1024,  # i.e. 128 MB
        min_days_not_optimized: int = 7,
        min_days_not_vacuumed: int = 31,
        max_optimize_freq: int = 2,
        max_vacuum_freq: int = 2,
        small_file_threshold: int = 32*1024*1024,  # i.e. 32 MB
        min_number_of_files_for_zorder: int = 8,
        stats: pd.DataFrame = None,  # for testability only
    ) -> None:
        # self._delta_housekeeping = delta_housekeeping
        if stats is None:
            self._mapped_pd_dfs = mapped_pd_dfs
            stats = pd.concat(self._mapped_pd_dfs)
        self._stats: pd.DataFrame = stats
        # if spark is None:
        #     spark = SparkSession.builder.getOrCreate()
        # self._spark = spark
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

    def stats(self) -> pd.DataFrame:
        return self._stats

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
        stats['max_optimize_timestamp'] = pd.to_datetime(stats['max_optimize_timestamp'])
        stats['optimize_lag'] = (
            datetime.utcnow().replace(tzinfo=stats.dtypes["max_optimize_timestamp"].tz) - stats['max_optimize_timestamp']
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
        stats['max_vacuum_timestamp'] = pd.to_datetime(stats['max_vacuum_timestamp'])
        stats['vacuum_lag'] = (
            datetime.utcnow().replace(tzinfo=stats.dtypes["max_vacuum_timestamp"].tz) - stats['max_vacuum_timestamp']
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
        stats = stats.loc[(stats['p50_file_size'] < self.small_file_threshold)]
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

    def apply(self):
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

    def to_html(self):
        # TODO better formatting!
        from bs4 import BeautifulSoup

        res = self.apply()

        soup = BeautifulSoup(features='xml')
        body = soup.new_tag('body')
        soup.insert(0, body)
        for r in res:
            for k,v in r.items():
                title_s = soup.new_tag('title')
                title_s.string = k
                body.insert(0, v.to_html())
                body.insert(0, title_s)

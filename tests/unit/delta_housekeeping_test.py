import pytest
import pandas as pd
from discoverx.delta_housekeeping import DeltaHousekeeping
from pathlib import Path
import pyspark.sql.functions as F


def _resolve_file_path(request, relative_path):
    module_path = Path(request.module.__file__)
    test_file_path = module_path.parent / relative_path
    return pd.read_csv(
        str(test_file_path.resolve()),
        dtype={
            "max_optimize_timestamp": "str",
            "2nd_optimize_timestamp": "str",
            "max_vacuum_timestamp": "str",
            "2nd_vacuum_timestamp": "str",
        }
    )


@pytest.fixture()
def dh_click_sales(request):
    return _resolve_file_path(request, "data/delta_housekeeping/dh_click_sales.csv")


@pytest.fixture()
def dd_click_sales(request):
    return _resolve_file_path(request, "data/delta_housekeeping/dd_click_sales.csv")


@pytest.fixture()
def expected_pdh_click_sales(request):
    return _resolve_file_path(request, "data/delta_housekeeping/expected_pdh_click_sales.csv")


@pytest.fixture()
def dh_housekeeping_summary(request):
    return _resolve_file_path(request, "data/delta_housekeeping/dh_housekeeping_summary.csv")


@pytest.fixture()
def dd_housekeeping_summary(request):
    return _resolve_file_path(request, "data/delta_housekeeping/dd_housekeeping_summary.csv")


@pytest.fixture()
def expected_pdh_housekeeping_summary(request):
    return _resolve_file_path(request, "data/delta_housekeeping/expected_pdh_housekeeping_summary.csv")


def test_process_describe_history_no_optimize(spark, dh_click_sales, dd_click_sales, expected_pdh_click_sales):
    dh = DeltaHousekeeping(spark)
    describe_detail_df = spark.createDataFrame(dd_click_sales)
    describe_history_df = spark.createDataFrame(dh_click_sales)
    out = dh._process_describe_history(describe_detail_df, describe_history_df)
    pd.testing.assert_frame_equal(
        out.reset_index(),
        expected_pdh_click_sales.reset_index(),
    )


def test_process_describe_history_no_vacuum(
    spark, dh_housekeeping_summary, dd_housekeeping_summary, expected_pdh_housekeeping_summary
):
    dh = DeltaHousekeeping(spark)
    describe_detail_df = spark.createDataFrame(dd_housekeeping_summary)
    describe_history_df = spark.createDataFrame(dh_housekeeping_summary)
    out = dh._process_describe_history(describe_detail_df, describe_history_df)
    pd.testing.assert_frame_equal(
        out.reset_index(),
        expected_pdh_housekeeping_summary.reset_index(),
    )


def test_process_describe_history_no_operation(spark, dd_click_sales):
    dh = DeltaHousekeeping(spark)
    describe_detail_df = spark.createDataFrame(dd_click_sales)
    describe_history_df = spark.createDataFrame([], "string")
    out = dh._process_describe_history(describe_detail_df, describe_history_df)
    # output should be equal to DESCRIBE DETAIL
    pd.testing.assert_frame_equal(
        out.reset_index(),
        dd_click_sales.reset_index(),
    )


def test_process_describe_history_empty_history(spark, dd_click_sales, dh_click_sales):
    dh = DeltaHousekeeping(spark)
    describe_detail_df = spark.createDataFrame(dd_click_sales)
    describe_history_df = spark.createDataFrame(dh_click_sales)
    describe_history_df = describe_history_df.withColumn("operation", F.lit("NOOP"))
    out = dh._process_describe_history(describe_detail_df, describe_history_df)
    # output should be equal to DESCRIBE DETAIL
    pd.testing.assert_frame_equal(
        out.reset_index(),
        dd_click_sales.reset_index(),
    )
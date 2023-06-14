import pandas as pd
from pandas.testing import assert_frame_equal
import pytest
import numpy as np

from discoverx.dx import DX
from discoverx.dx import Scanner
from discoverx.scanner import ScanResult
from discoverx.rules import Rules
from discoverx import logging

logger = logging.Logging()


def test_classifier(spark):

    dummy_scanner = Scanner(spark, Rules())
    df_scan_result = pd.DataFrame(
        {
            "table_catalog": [None, None, None, None, None, None, None, None, None],
            "table_schema": [
                "default",
                "default",
                "default",
                "default",
                "default",
                "default",
                "default",
                "default",
                "default",
            ],
            "table_name": ["tb_1", "tb_1", "tb_1", "tb_1", "tb_1", "tb_1", "tb_1", "tb_1", "tb_1"],
            "column_name": ["ip", "ip", "ip", "mac", "mac", "mac", "description", "description", "description"],
            "class_name": ["ip_v4", "ip_v6", "mac", "ip_v4", "ip_v6", "mac", "ip_v4", "ip_v6", "mac"],
            "score": [1.0, 0.0, 0.0, 0.0, 0.0, 0.97, 0.0, 0.0, 0.0],
        }
    )
    dummy_scanner.scan_result = ScanResult(df_scan_result)
    dx = DX(spark=spark, classification_table_name="_discoverx.classes")
    dx.scanner = dummy_scanner
    dx._classify(classification_threshold=0.95)
    dx.classifier.compute_classification_result()
    assert_frame_equal(
        dx.classifier.classification_result.reset_index(drop=True),
        pd.DataFrame(
            {
                "table_catalog": [None, None],
                "table_schema": ["default", "default"],
                "table_name": ["tb_1", "tb_1"],
                "column_name": ["ip", "mac"],
                "Current Classes": [[], []],
                "Detected Classes": [["ip_v4"], ["mac"]],
                "Classes to be published": [["ip_v4"], ["mac"]],
                "Classes changed": [True, True],
            }
        ),
    )

    assert dx.classifier.n_classified_columns == 2
    assert dx.classifier.rule_match_str == "            <li>1 ip_v4 columns</li>\n            <li>1 mac columns</li>"
    try:
        _ = dx.classifier.summary_html
    except Exception as display_rules_error:
        pytest.fail(f"Displaying rules failed with {display_rules_error}")


def test_merging_scan_results(spark, mock_current_time):

    # start with adding one classified ip column to the classification table
    dummy_scanner = Scanner(spark, Rules())
    df_scan_result = pd.DataFrame(
        {
            "table_catalog": [None, None, None, None, None, None],
            "table_schema": ["default", "default", "default", "default", "default", "default"],
            "table_name": ["tb_1", "tb_1", "tb_1", "tb_1", "tb_1", "tb_1"],
            "column_name": ["ip", "ip", "mac", "mac", "description", "description"],
            "class_name": ["ip_v4", "ip_v6", "ip_v4", "ip_v6", "ip_v4", "ip_v6"],
            "score": [1.0, 0.0, 0.0, 0.0, 0.0, 0.0],
        }
    )
    dummy_scanner.scan_result = ScanResult(df_scan_result)
    dx = DX(spark=spark, classification_table_name="_discoverx.classes")
    dx.scanner = dummy_scanner
    dx._classify(classification_threshold=0.95)
    dx.publish()

    expected_df = pd.DataFrame(
        {
            "table_catalog": [None],
            "table_schema": ["default"],
            "table_name": ["tb_1"],
            "column_name": ["ip"],
            "class_name": ["ip_v4"],
            "effective_timestamp": [pd.Timestamp(2023, 1, 1, 0)],
            "current": [True],
            "end_timestamp": [pd.NaT],
        }
    )

    assert_frame_equal(spark.sql("SELECT * FROM _discoverx.classes").toPandas(), expected_df)

    # if a second scan/classification re-classifies the same column, i.e.
    # we get the same result the classification should remain unchanged.
    dx2 = DX(spark=spark, classification_table_name="_discoverx.classes")
    dx2.scanner = dummy_scanner
    dx2._classify(classification_threshold=0.95)
    dx2.publish()

    expected_df = pd.DataFrame(
        {
            "table_catalog": [None],
            "table_schema": ["default"],
            "table_name": ["tb_1"],
            "column_name": ["ip"],
            "class_name": ["ip_v4"],
            "effective_timestamp": [pd.Timestamp(2023, 1, 1, 0)],
            "current": [True],
            "end_timestamp": [pd.NaT],
        }
    )

    assert_frame_equal(spark.sql("SELECT * FROM _discoverx.classes").toPandas(), expected_df)

    # we now assume that a second column has been classified. It should be
    # added to the classification table
    df_scan_result3 = pd.DataFrame(
        {
            "table_catalog": [None, None, None, None, None, None, None, None],
            "table_schema": ["default", "default", "default", "default", "default", "default", "default", "default"],
            "table_name": ["tb_1", "tb_1", "tb_1", "tb_1", "tb_1", "tb_1", "tb_1", "tb_1"],
            "column_name": ["ip", "ip", "ip6", "ip6", "mac", "mac", "description", "description"],
            "class_name": ["ip_v4", "ip_v6", "ip_v4", "ip_v6", "ip_v4", "ip_v6", "ip_v4", "ip_v6"],
            "score": [1.0, 0.0, 0.0, 0.97, 0.0, 0.0, 0.0, 0.0],
        }
    )

    dx3 = DX(spark=spark, classification_table_name="_discoverx.classes")
    dummy_scanner.scan_result = ScanResult(df_scan_result3)
    dx3.scanner = dummy_scanner
    dx3._classify(classification_threshold=0.95)
    dx3.publish()

    current_time = pd.Timestamp(2023, 1, 1, 0)
    expected3_df = pd.DataFrame(
        {
            "table_catalog": [None, None],
            "table_schema": ["default", "default"],
            "table_name": ["tb_1", "tb_1"],
            "column_name": ["ip", "ip6"],
            "class_name": ["ip_v4", "ip_v6"],
            "effective_timestamp": [current_time, current_time],
            "current": [True, True],
            "end_timestamp": [pd.NaT, pd.NaT],
        }
    ).sort_values(by=["table_catalog", "table_schema", "table_name", "column_name"])

    assert_frame_equal(
        spark.sql("SELECT * FROM _discoverx.classes")
        .toPandas()
        .sort_values(by=["table_catalog", "table_schema", "table_name", "column_name"])
        .reset_index(drop=True),
        expected3_df.reset_index(drop=True),
    )

    # assume ip column is not classified during a subsequent scan - we should not remove the existing class
    df_scan_result5 = pd.DataFrame(
        {
            "table_catalog": [None, None, None, None, None, None, None, None],
            "table_schema": ["default", "default", "default", "default", "default", "default", "default", "default"],
            "table_name": ["tb_1", "tb_1", "tb_1", "tb_1", "tb_1", "tb_1", "tb_1", "tb_1"],
            "column_name": ["ip", "ip", "ip6", "ip6", "mac", "mac", "description", "description"],
            "class_name": ["ip_v4", "ip_v6", "ip_v4", "ip_v6", "ip_v4", "ip_v6", "ip_v4", "ip_v6"],
            "score": [0.7, 0.0, 0.0, 0.97, 0.0, 0.0, 0.0, 0.0],
        }
    )

    dx5 = DX(spark=spark, classification_table_name="_discoverx.classes")
    dummy_scanner.scan_result = ScanResult(df_scan_result5)
    dx5.scanner = dummy_scanner
    dx5._classify(classification_threshold=0.95)
    dx5.publish()

    expected5_df = pd.DataFrame(
        {
            "table_catalog": [None, None],
            "table_schema": ["default", "default"],
            "table_name": ["tb_1", "tb_1"],
            "column_name": ["ip", "ip6"],
            "class_name": ["ip_v4", "ip_v6"],
            "effective_timestamp": [current_time, current_time],
            "current": [True, True],
            "end_timestamp": [pd.NaT, pd.NaT],
        }
    ).sort_values(by=["table_catalog", "table_schema", "table_name", "column_name"])

    assert_frame_equal(
        spark.sql("SELECT * FROM _discoverx.classes")
        .toPandas()
        .sort_values(by=["table_catalog", "table_schema", "table_name", "column_name"])
        .reset_index(drop=True),
        expected5_df.reset_index(drop=True),
    )

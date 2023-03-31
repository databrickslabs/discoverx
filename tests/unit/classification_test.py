import pandas as pd
from pandas.testing import assert_frame_equal
import pytest
import numpy as np

from discoverx.dx import DX
from discoverx.dx import Scanner
from discoverx.scanner import ScanResult
from discoverx.rules import Rules
from discoverx import logging
from discoverx.inspection import InspectionTool

logger = logging.Logging()


def test_classifier(spark):

    dummy_scanner = Scanner(spark, Rules())
    df_scan_result = pd.DataFrame(
        {
            "catalog": [None, None, None, None, None, None, None, None, None],
            "database": [
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
            "table": ["tb_1", "tb_1", "tb_1", "tb_1", "tb_1", "tb_1", "tb_1", "tb_1", "tb_1"],
            "column": ["ip", "ip", "ip", "mac", "mac", "mac", "description", "description", "description"],
            "rule_name": ["ip_v4", "ip_v6", "mac", "ip_v4", "ip_v6", "mac", "ip_v4", "ip_v6", "mac"],
            "frequency": [1.0, 0.0, 0.0, 0.0, 0.0, 0.97, 0.0, 0.0, 0.0],
        }
    )
    dummy_scanner.scan_result = ScanResult(df_scan_result)
    dx = DX(spark=spark, classification_table_name="_discoverx.tags")
    dx.scanner = dummy_scanner
    dx.classify(column_type_classification_threshold=0.95)
    assert_frame_equal(
        dx.classifier.classification_result.reset_index(drop=True),
        pd.DataFrame(
            {
                "table_catalog": [None, None],
                "table_schema": ["default", "default"],
                "table_name": ["tb_1", "tb_1"],
                "column_name": ["ip", "mac"],
                "Current Tags": [[], []],
                "Detected Tags": [["ip_v4"], ["mac"]],
                "Tags to be published": [["ip_v4"], ["mac"]],
                "Tags changed": [True, True],
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
            "catalog": [None, None, None, None, None, None],
            "database": ["default", "default", "default", "default", "default", "default"],
            "table": ["tb_1", "tb_1", "tb_1", "tb_1", "tb_1", "tb_1"],
            "column": ["ip", "ip", "mac", "mac", "description", "description"],
            "rule_name": ["ip_v4", "ip_v6", "ip_v4", "ip_v6", "ip_v4", "ip_v6"],
            "frequency": [1.0, 0.0, 0.0, 0.0, 0.0, 0.0],
        }
    )
    dummy_scanner.scan_result = ScanResult(df_scan_result)
    dx = DX(spark=spark, classification_table_name="_discoverx.tags")
    dx.scanner = dummy_scanner
    dx.classify(column_type_classification_threshold=0.95)
    dx.publish()

    expected_df = pd.DataFrame(
        {
            "table_catalog": [None],
            "table_schema": ["default"],
            "table_name": ["tb_1"],
            "column_name": ["ip"],
            "tag_name": ["ip_v4"],
            "effective_timestamp": [pd.Timestamp(2023, 1, 1, 0)],
            "current": [True],
            "end_timestamp": [pd.NaT],
        }
    )

    assert_frame_equal(spark.sql("SELECT * FROM _discoverx.tags").toPandas(), expected_df)

    # if a second scan/classification re-classifies the same column, i.e.
    # we get the same result the classification should remain unchanged.
    dx2 = DX(spark=spark, classification_table_name="_discoverx.tags")
    dx2.scanner = dummy_scanner
    dx2.classify(column_type_classification_threshold=0.95)
    dx2.publish()

    expected_df = pd.DataFrame(
        {
            "table_catalog": [None],
            "table_schema": ["default"],
            "table_name": ["tb_1"],
            "column_name": ["ip"],
            "tag_name": ["ip_v4"],
            "effective_timestamp": [pd.Timestamp(2023, 1, 1, 0)],
            "current": [True],
            "end_timestamp": [pd.NaT],
        }
    )

    assert_frame_equal(spark.sql("SELECT * FROM _discoverx.tags").toPandas(), expected_df)

    # we now assume that a second column has been classified. It should be
    # added to the classification table
    df_scan_result3 = pd.DataFrame(
        {
            "catalog": [None, None, None, None, None, None, None, None],
            "database": ["default", "default", "default", "default", "default", "default", "default", "default"],
            "table": ["tb_1", "tb_1", "tb_1", "tb_1", "tb_1", "tb_1", "tb_1", "tb_1"],
            "column": ["ip", "ip", "ip6", "ip6", "mac", "mac", "description", "description"],
            "rule_name": ["ip_v4", "ip_v6", "ip_v4", "ip_v6", "ip_v4", "ip_v6", "ip_v4", "ip_v6"],
            "frequency": [1.0, 0.0, 0.0, 0.97, 0.0, 0.0, 0.0, 0.0],
        }
    )

    dx3 = DX(spark=spark, classification_table_name="_discoverx.tags")
    dummy_scanner.scan_result = ScanResult(df_scan_result3)
    dx3.scanner = dummy_scanner
    dx3.classify(column_type_classification_threshold=0.95)
    dx3.publish()

    current_time = pd.Timestamp(2023, 1, 1, 0)
    expected3_df = pd.DataFrame(
        {
            "table_catalog": [None, None],
            "table_schema": ["default", "default"],
            "table_name": ["tb_1", "tb_1"],
            "column_name": ["ip", "ip6"],
            "tag_name": ["ip_v4", "ip_v6"],
            "effective_timestamp": [current_time, current_time],
            "current": [True, True],
            "end_timestamp": [pd.NaT, pd.NaT],
        }
    ).sort_values(by=["table_catalog", "table_schema", "table_name", "column_name"])

    assert_frame_equal(
        spark.sql("SELECT * FROM _discoverx.tags")
        .toPandas()
        .sort_values(by=["table_catalog", "table_schema", "table_name", "column_name"])
        .reset_index(drop=True),
        expected3_df.reset_index(drop=True),
    )

    # assume ip column is not classified during a subsequent scan - we should not remove the existing tag
    df_scan_result5 = pd.DataFrame(
        {
            "catalog": [None, None, None, None, None, None, None, None],
            "database": ["default", "default", "default", "default", "default", "default", "default", "default"],
            "table": ["tb_1", "tb_1", "tb_1", "tb_1", "tb_1", "tb_1", "tb_1", "tb_1"],
            "column": ["ip", "ip", "ip6", "ip6", "mac", "mac", "description", "description"],
            "rule_name": ["ip_v4", "ip_v6", "ip_v4", "ip_v6", "ip_v4", "ip_v6", "ip_v4", "ip_v6"],
            "frequency": [0.7, 0.0, 0.0, 0.97, 0.0, 0.0, 0.0, 0.0],
        }
    )

    dx5 = DX(spark=spark, classification_table_name="_discoverx.tags")
    dummy_scanner.scan_result = ScanResult(df_scan_result5)
    dx5.scanner = dummy_scanner
    dx5.classify(column_type_classification_threshold=0.95)
    dx5.publish()

    expected5_df = pd.DataFrame(
        {
            "table_catalog": [None, None],
            "table_schema": ["default", "default"],
            "table_name": ["tb_1", "tb_1"],
            "column_name": ["ip", "ip6"],
            "tag_name": ["ip_v4", "ip_v6"],
            "effective_timestamp": [current_time, current_time],
            "current": [True, True],
            "end_timestamp": [pd.NaT, pd.NaT],
        }
    ).sort_values(by=["table_catalog", "table_schema", "table_name", "column_name"])

    assert_frame_equal(
        spark.sql("SELECT * FROM _discoverx.tags")
        .toPandas()
        .sort_values(by=["table_catalog", "table_schema", "table_name", "column_name"])
        .reset_index(drop=True),
        expected5_df.reset_index(drop=True),
    )

    # test new detected column and manual changes during inspect. Manually remove ip_v4, add pii tag to ip_v6
    # uc tags (mocked)
    df_scan_result6 = pd.DataFrame(
        {
            "catalog": [None, None, None, None, None, None, None, None, None, None, None, None],
            "database": [
                "default",
                "default",
                "default",
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
            "table": ["tb_1", "tb_1", "tb_1", "tb_1", "tb_1", "tb_1", "tb_2", "tb_2", "tb_2", "tb_1", "tb_1", "tb_1"],
            "column": [
                "ip",
                "ip",
                "ip",
                "ip6",
                "ip6",
                "ip6",
                "mac",
                "mac",
                "mac",
                "description",
                "description",
                "description",
            ],
            "rule_name": [
                "ip_v4",
                "ip_v6",
                "mac",
                "ip_v4",
                "ip_v6",
                "mac",
                "ip_v4",
                "ip_v6",
                "mac",
                "ip_v4",
                "ip_v6",
                "mac",
            ],
            "frequency": [0.96, 0.0, 0.0, 0.0, 0.98, 0.0, 0.0, 0.0, 1.0, 0.0, 0.0, 0.0],
        }
    )

    dx6 = DX(spark=spark, classification_table_name="_discoverx.tags")
    dummy_scanner.scan_result = ScanResult(df_scan_result6)
    dx6.scanner = dummy_scanner
    dx6.classify(column_type_classification_threshold=0.95)
    # simulate manual changes in InteractionTool - set ip6 to active again
    dx6.classifier.inspection_tool = InspectionTool(dx6.classifier.classification_result, dx6.classifier.publish)
    dx6.classifier.inspection_tool.inspected_table = dx6.classifier.classification_result
    dx6.classifier.inspection_tool.inspected_table.at[0, "Tags to be published"] = []
    dx6.classifier.inspection_tool.inspected_table.at[0, "Tags changed"] = True
    dx6.classifier.inspection_tool.inspected_table.at[1, "Tags to be published"] = ['ip_v6', 'pii']
    dx6.classifier.inspection_tool.inspected_table.at[1, "Tags changed"] = True
    dx6.publish(publish_uc_tags=True)

    expected6_df = pd.DataFrame(
        {
            "table_catalog": [None, None, None, None],
            "table_schema": ["default", "default", "default", "default"],
            "table_name": ["tb_1", "tb_1", "tb_1", "tb_2"],
            "column_name": ["ip", "ip6", "ip6", "mac"],
            "tag_name": ["ip_v4", "ip_v6", "pii", "mac"],
            "effective_timestamp": [current_time, current_time, current_time, current_time],
            "current": [False, True, True, True],
            "end_timestamp": [current_time, pd.NaT, pd.NaT, pd.NaT],
        }
    ).sort_values(by=["table_catalog", "table_schema", "table_name", "column_name"])

    assert_frame_equal(
        spark.sql("SELECT * FROM _discoverx.tags")
        .toPandas()
        .sort_values(by=["table_catalog", "table_schema", "table_name", "column_name"])
        .reset_index(drop=True),
        expected6_df.reset_index(drop=True),
    )

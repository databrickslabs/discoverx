import pandas as pd
from pandas.testing import assert_frame_equal
import pytest
from delta.tables import DeltaTable

from discoverx.dx import DX
from discoverx.dx import Scanner
from discoverx.scanner import ScanResult
from discoverx.dx import Classifier
from discoverx.rules import Rules
from discoverx import logging
from discoverx.classification import func
from discoverx.inspection import InspectionTool

logger = logging.Logging()


@pytest.fixture(scope="module")
def monkeymodule():
    """
    Required for monkeypatching with module scope.
    For more info see
    https://stackoverflow.com/questions/53963822/python-monkeypatch-setattr-with-pytest-fixture-at-module-scope
    """
    with pytest.MonkeyPatch.context() as mp:
        yield mp


@pytest.fixture(autouse=True, scope="module")
def mock_uc_functionality(spark, monkeymodule):
    # apply the monkeypatch for the columns_table_name
    monkeymodule.setattr(Scanner, "COLUMNS_TABLE_NAME", "default.columns_mock")

    # mock classifier method _get_classification_table_from_delta as we don't
    # have catalogs in open source spark
    def get_classification_table_mock(self):
        (schema, table) = self.classification_table_name.split(".")
        self.spark.sql(f"CREATE DATABASE IF NOT EXISTS {schema}")
        self.spark.sql(
            f"""
                CREATE TABLE IF NOT EXISTS {schema + '.' + table} (table_catalog string, table_schema string, table_name string, column_name string, rule_name string, tag_status string, effective_timestamp timestamp, current boolean, end_timestamp timestamp) USING DELTA
                """
        )
        return DeltaTable.forName(self.spark, self.classification_table_name)

    monkeymodule.setattr(Classifier, "_get_classification_table_from_delta", get_classification_table_mock)

    # mock UC's tag functionality
    def set_uc_tags(self, series):
        if (series.action == "set") & (series.tag_status == "active"):
            logger.debug(
                f"Set tag {series.rule_name} for column {series.column_name} of table {series.table_catalog}.{series.table_schema}.{series.table_name}"
            )
        if (series.action == "unset") | (series.tag_status == "inactive"):
            logger.debug(
                f"Unset tag {series.rule_name} for column {series.column_name} of table {series.table_catalog}.{series.table_schema}.{series.table_name}"
            )

    monkeymodule.setattr(Classifier, "_set_tag_uc", set_uc_tags)

    yield

    spark.sql("DROP TABLE IF EXISTS _discoverx.tags")
    spark.sql("DROP SCHEMA IF EXISTS _discoverx")


@pytest.fixture(scope="module")
def mock_current_time(spark, monkeymodule):
    def set_time():
        return func.to_timestamp(func.lit("2023-01-01 00:00:00"))

    monkeymodule.setattr(func, "current_timestamp", set_time)


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
    dx.classify(classification_threshold=0.95)
    assert_frame_equal(
        dx.classifier.classified_result.reset_index(drop=True),
        pd.DataFrame(
            {
                "table_catalog": [None, None],
                "table_schema": ["default", "default"],
                "table_name": ["tb_1", "tb_1"],
                "column_name": ["ip", "mac"],
                "rule_name": ["ip_v4", "mac"],
                "frequency": [1.0, 0.97],
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
    dx.classify(classification_threshold=0.95)
    dx.publish()

    expected_df = pd.DataFrame(
        {
            "table_catalog": [None],
            "table_schema": ["default"],
            "table_name": ["tb_1"],
            "column_name": ["ip"],
            "rule_name": ["ip_v4"],
            "tag_status": ["active"],
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
    dx2.classify(classification_threshold=0.95)
    dx2.publish()

    expected_df = pd.DataFrame(
        {
            "table_catalog": [None],
            "table_schema": ["default"],
            "table_name": ["tb_1"],
            "column_name": ["ip"],
            "rule_name": ["ip_v4"],
            "tag_status": ["active"],
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
    dx3.classify(classification_threshold=0.95)
    dx3.publish()

    current_time = pd.Timestamp(2023, 1, 1, 0)
    expected3_df = pd.DataFrame(
        {
            "table_catalog": [None, None],
            "table_schema": ["default", "default"],
            "table_name": ["tb_1", "tb_1"],
            "column_name": ["ip", "ip6"],
            "rule_name": ["ip_v4", "ip_v6"],
            "tag_status": ["active", "active"],
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

    # as a next step we assume that the ip6 column classification has been
    # deactivated

    # simulate manual change of tag_status
    spark.sql("UPDATE _discoverx.tags SET tag_status = 'inactive' WHERE column_name = 'ip6'")
    dx4 = DX(spark=spark, classification_table_name="_discoverx.tags")
    dx4.scanner = dummy_scanner
    dx4.classify(classification_threshold=0.95)
    dx4.publish()

    expected4_df = pd.DataFrame(
        {
            "table_catalog": [None, None],
            "table_schema": ["default", "default"],
            "table_name": ["tb_1", "tb_1"],
            "column_name": ["ip", "ip6"],
            "rule_name": ["ip_v4", "ip_v6"],
            "tag_status": ["active", "inactive"],
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
        expected4_df.reset_index(drop=True),
    )

    # assume ip column is not classified during a subsequent scan
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
    dx5.classify(classification_threshold=0.95)
    dx5.publish()

    expected5_df = pd.DataFrame(
        {
            "table_catalog": [None, None],
            "table_schema": ["default", "default"],
            "table_name": ["tb_1", "tb_1"],
            "column_name": ["ip", "ip6"],
            "rule_name": ["ip_v4", "ip_v6"],
            "tag_status": ["active", "inactive"],
            "effective_timestamp": [current_time, current_time],
            "current": [False, True],
            "end_timestamp": [current_time, pd.NaT],
        }
    ).sort_values(by=["table_catalog", "table_schema", "table_name", "column_name"])

    assert_frame_equal(
        spark.sql("SELECT * FROM _discoverx.tags")
        .toPandas()
        .sort_values(by=["table_catalog", "table_schema", "table_name", "column_name"])
        .reset_index(drop=True),
        expected5_df.reset_index(drop=True),
    )

    # re-detect ip, re-activate ip6 and new mac and test publishing
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
    dx6.classify(classification_threshold=0.95)
    # simulate manual changes in InteractionTool - set ip6 to active again
    dx6.classifier._get_staged_updates()
    dx6.classifier.inspection_tool = InspectionTool(dx6.classifier.staged_updates_pdf)
    dx6.classifier.inspection_tool.staged_updates_pdf.iloc[2, 6] = "active"
    dx6.classifier.inspection_tool.staged_updates_pdf.iloc[2, 5] = "set"
    dx6.publish(publish_uc_tags=True)

    expected6_df = pd.DataFrame(
        {
            "table_catalog": [None, None, None, None, None],
            "table_schema": ["default", "default", "default", "default", "default"],
            "table_name": ["tb_1", "tb_1", "tb_1", "tb_1", "tb_2"],
            "column_name": ["ip", "ip", "ip6", "ip6", "mac"],
            "rule_name": ["ip_v4", "ip_v4", "ip_v6", "ip_v6", "mac"],
            "tag_status": ["active", "active", "active", "inactive", "active"],
            "effective_timestamp": [current_time, current_time, current_time, current_time, current_time],
            "current": [True, False, True, False, True],
            "end_timestamp": [pd.NaT, current_time, pd.NaT, current_time, pd.NaT],
        }
    ).sort_values(by=["table_catalog", "table_schema", "table_name", "column_name"])

    assert_frame_equal(
        spark.sql("SELECT * FROM _discoverx.tags")
        .toPandas()
        .sort_values(by=["table_catalog", "table_schema", "table_name", "column_name"])
        .reset_index(drop=True),
        expected6_df.reset_index(drop=True),
    )

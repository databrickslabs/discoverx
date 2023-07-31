import pytest
from unittest.mock import Mock, patch
from pyspark.sql import SparkSession
from discoverx.explorer import DataExplorer, InfoFetcher, TableInfo


# # Sample test data
# sample_table_info = TableInfo("catalog1", "schema1", "table1", [])
@pytest.fixture()
def info_fetcher(spark):
    return InfoFetcher(spark=spark, columns_table_name="default.columns_mock")


@pytest.fixture()
def sample_table_info():
    return TableInfo("catalog1", "schema1", "table1", [])


def test_validate_from_components():
    with pytest.raises(ValueError):
        DataExplorer.validate_from_components("invalid_format")
    assert DataExplorer.validate_from_components("catalog1.schema1.table1") == ("catalog1", "schema1", "table1")


def test_build_sql(sample_table_info):
    info_fetcher = InfoFetcher(spark=Mock(), columns_table_name="default.columns_mock")
    data_explorer = DataExplorer("catalog1.schema1.table1", spark=Mock(), info_fetcher=info_fetcher)
    sql_template = "SELECT * FROM {full_table_name}"
    expected_sql = "SELECT * FROM catalog1.schema1.table1"
    assert data_explorer._build_sql(sql_template, sample_table_info) == expected_sql


def test_run_sql(spark, info_fetcher):
    data_explorer = DataExplorer("*.*.tb_1", spark=spark, info_fetcher=info_fetcher)

    result = (
        data_explorer.with_sql("SELECT 1 AS a FROM {full_table_name}")
        .to_dataframe()
        .groupBy("table_name")
        .count()
        .collect()
    )
    assert len(result) == 1


def test_get_sql_commands():
    info_fetcher = InfoFetcher(spark=Mock(), columns_table_name="default.columns_mock")
    data_explorer = DataExplorer("catalog1.schema1.table1", spark=Mock(), info_fetcher=info_fetcher)

    # Mocking get_tables_info to return a sample table list
    info_fetcher.get_tables_info = Mock(return_value=[sample_table_info])

    # Mocking _build_sql to return a sample SQL query
    data_explorer._build_sql = Mock(return_value="SELECT * FROM catalog1.schema1.table1")

    expected_sql_commands = [("SELECT * FROM catalog1.schema1.table1", sample_table_info)]
    assert data_explorer._get_sql_commands() == expected_sql_commands


def test_explain(capfd, spark, info_fetcher):
    data_explorer = DataExplorer("*.*.tb_*", spark=spark, info_fetcher=info_fetcher)

    data_explorer.having_columns("ip.v2").with_sql("SELECT `ip.v2` FROM {full_table_name}").explain()

    captured = capfd.readouterr()
    assert "SELECT `ip.v2` FROM " in captured.out
    # assert "tb_1" in captured.out

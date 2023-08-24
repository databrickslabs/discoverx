import pytest
from unittest.mock import Mock, patch
from pyspark.sql import SparkSession
from discoverx.explorer import DataExplorer, DataExplorerActions, InfoFetcher, TableInfo


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
    sql_template = "SELECT * FROM {full_table_name}"
    expected_sql = "SELECT * FROM catalog1.schema1.table1"
    assert DataExplorerActions._build_sql(sql_template, sample_table_info) == expected_sql


def test_run_sql(spark, info_fetcher):
    data_explorer = DataExplorer("*.*.tb_1", spark, info_fetcher)
    data_explorer._sql_query_template = "SELECT 1 AS a FROM {full_table_name}"

    result = (
        DataExplorerActions(data_explorer=data_explorer, spark=spark, info_fetcher=info_fetcher)
        .to_union_dataframe()
        .groupBy("table_name")
        .count()
        .collect()
    )
    assert len(result) == 1


def test_execute(spark, info_fetcher, capfd):
    data_explorer = DataExplorer("*.*.tb_1", spark, info_fetcher)
    data_explorer._sql_query_template = "SELECT 12345 AS a FROM {full_table_name}"

    result = DataExplorerActions(data_explorer=data_explorer, spark=spark, info_fetcher=info_fetcher).execute()
    captured = capfd.readouterr()
    assert "12345" in captured.out


def test_explain(capfd, spark, info_fetcher):
    data_explorer = DataExplorer("*.*.tb_*", spark, info_fetcher).having_columns("ip.v2").with_concurrency(2)

    data_explorer._sql_query_template = "SELECT `something` FROM {full_table_name}"

    DataExplorerActions(data_explorer, spark, info_fetcher).explain()

    captured = capfd.readouterr()
    assert "SELECT `something` FROM " in captured.out
    assert "ip.v2" in captured.out

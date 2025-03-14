import pandas
import pytest
from discoverx.explorer import DataExplorer, DataExplorerActions, InfoFetcher, TableInfo


@pytest.fixture()
def info_fetcher(spark):
    return InfoFetcher(spark=spark, information_schema="default")


@pytest.fixture()
def sample_table_info():
    return TableInfo("catalog1", "schema1", "table1", [], None)


def test_validate_from_components():
    with pytest.raises(ValueError):
        DataExplorer.validate_from_components("invalid_format")
    assert DataExplorer.validate_from_components("catalog1.schema1.table1") == (
        "catalog1",
        "schema1",
        "table1",
    )


def test_build_sql(sample_table_info):
    sql_template = "SELECT * FROM {full_table_name}"
    expected_sql = "SELECT * FROM catalog1.schema1.table1"
    assert DataExplorerActions._build_sql(sql_template, sample_table_info) == expected_sql


def test_run_sql(spark, info_fetcher):
    data_explorer = DataExplorer("*.*.tb_1", spark, info_fetcher)
    data_explorer._sql_query_template = "SELECT 1 AS a FROM {full_table_name}"

    result = (
        DataExplorerActions(data_explorer=data_explorer, spark=spark, info_fetcher=info_fetcher)
        .apply()
        .groupBy("table_name")
        .count()
        .collect()
    )
    assert len(result) == 1


def test_execute(spark, info_fetcher, capfd):
    data_explorer = DataExplorer("*.*.tb_1", spark, info_fetcher)
    data_explorer._sql_query_template = "SELECT 12345 AS a FROM {full_table_name}"

    result = DataExplorerActions(data_explorer=data_explorer, spark=spark, info_fetcher=info_fetcher).display()
    captured = capfd.readouterr()
    assert "12345" in captured.out


def test_explain(capfd, spark, info_fetcher):
    data_explorer = DataExplorer("*.*.tb_*", spark, info_fetcher).having_columns("ip.v2").with_concurrency(2)

    data_explorer._sql_query_template = "SELECT `something` FROM {full_table_name}"

    DataExplorerActions(data_explorer, spark, info_fetcher).explain()

    captured = capfd.readouterr()
    assert "SELECT `something` FROM " in captured.out
    assert "ip.v2" in captured.out


def test_map(spark, info_fetcher):
    data_explorer = DataExplorer("*.default.tb_1", spark, info_fetcher)
    result = data_explorer.map(lambda table_info: table_info)
    assert len(result) == 1
    assert result[0].table == "tb_1"
    assert result[0].schema == "default"
    assert result[0].catalog is None
    assert result[0].tags is None


def test_map_with_tags(spark, info_fetcher):
    data_explorer = DataExplorer("*.default.tb_1", spark, info_fetcher).with_tags()
    result = data_explorer.map(lambda table_info: table_info)
    assert len(result) == 1
    assert result[0].table == "tb_1"
    assert result[0].schema == "default"
    assert result[0].catalog is None
    assert len(result[0].tags.table_tags) == 1


def test_map_with_source_data_formats(spark, info_fetcher):
    data_explorer = DataExplorer("*.default.tb_1", spark, info_fetcher).with_data_source_formats(
        data_source_formats=["DELTA"]
    )
    result = data_explorer.map(lambda table_info: table_info)
    assert len(result) == 1
    assert result[0].table == "tb_1"
    assert result[0].schema == "default"
    assert result[0].catalog is None

    data_explorer = DataExplorer("*.default.tb_1", spark, info_fetcher).with_data_source_formats(
        data_source_formats=["CSV"]
    )
    with pytest.raises(ValueError):
        data_explorer.map(lambda table_info: table_info)


def test_no_tables_matching_filter(spark, info_fetcher):
    data_explorer = DataExplorer("some_catalog.default.non_existent_table", spark, info_fetcher)
    with pytest.raises(ValueError):
        data_explorer.map(lambda table_info: table_info)


def test_delta_housekeeeping_call(spark, info_fetcher):
    data_explorer = DataExplorer("*.default.*", spark, info_fetcher)
    result: pandas.DataFrame = data_explorer.delta_housekeeping()._stats_df.toPandas()
    print(result['tableName'].count())
    assert result['tableName'].count() == 3
    for res in result['tableName'].tolist():
        assert res in ["tb_all_types", "tb_1", "tb_2"]
    for col in result.columns:
        assert col in ["catalog", "database", "tableName", "error"]

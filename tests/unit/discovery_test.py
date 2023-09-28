import pytest
from discoverx.explorer import DataExplorer, InfoFetcher


@pytest.fixture()
def info_fetcher(spark):
    return InfoFetcher(spark=spark, columns_table_name="default.columns_mock")


@pytest.fixture(name="discover_ip")
def scan_ip_in_tb1(spark, info_fetcher):
    data_explorer = DataExplorer("*.*.tb_1", spark, info_fetcher)
    discover = data_explorer.scan(rules="ip_*")
    return discover


def test_discover_scan_msql(discover_ip):
    result = discover_ip._msql("SELECT [ip_v4] as ip FROM *.*.*").collect()
    assert {row.ip for row in result} == {"1.2.3.4", "3.4.5.60"}

    # test what-if
    try:
        _ = discover_ip._msql("SELECT [ip_v4] as ip FROM *.*.*", what_if=True)
    except Exception as e:
        pytest.fail(f"Test failed with exception {e}")


def test_discover_search(discover_ip):
    # search a specific term and auto-detect matching classes/rules
    result = discover_ip.search("1.2.3.4").collect()
    assert result[0].table_name == "tb_1"
    assert result[0].search_result.ip_v4.column_name == "ip"

    # specify catalog, schema and table
    result_classes_namespace = discover_ip.search("1.2.3.4", by_class="ip_v4", from_tables="*.default.tb_*")
    assert {row.search_result.ip_v4.value for row in result_classes_namespace.collect()} == {"1.2.3.4"}

    with pytest.raises(ValueError) as no_search_term_error:
        discover_ip.search(None)
    assert no_search_term_error.value.args[0] == "search_term has not been provided."

    with pytest.raises(ValueError) as no_inferred_class_error:
        discover_ip.search("###")
    assert (
        no_inferred_class_error.value.args[0]
        == "Could not infer any class for the given search term. Please specify the by_class parameter."
    )

    with pytest.raises(ValueError) as single_bool:
        discover_ip.search("", by_class=True)
    assert single_bool.value.args[0] == "The provided by_class True must be of string type."


def test_discover_select_by_class(discover_ip):
    # search a specific term and auto-detect matching classes/rules
    result = discover_ip.select_by_classes(from_tables="*.default.tb_*", by_classes="ip_v4").collect()
    assert result[0].table_name == "tb_1"
    assert result[0].classified_columns.ip_v4.column_name == "ip"

    result = discover_ip.select_by_classes(from_tables="*.default.tb_*", by_classes=["ip_v4"]).collect()
    assert result[0].table_name == "tb_1"
    assert result[0].classified_columns.ip_v4.column_name == "ip"

    with pytest.raises(ValueError):
        discover_ip.select_by_classes(from_tables="*.default.tb_*")

    with pytest.raises(ValueError):
        discover_ip.select_by_classes(from_tables="*.default.tb_*", by_classes=[1, 3, "ip"])

    with pytest.raises(ValueError):
        discover_ip.select_by_classes(from_tables="*.default.tb_*", by_classes=True)

    with pytest.raises(ValueError):
        discover_ip.select_by_classes(from_tables="invalid from", by_classes="email")


def test_discover_delete_by_class(spark, discover_ip):
    # search a specific term and auto-detect matching classes/rules
    discover_ip.delete_by_class(from_tables="*.default.tb_*", by_class="ip_v4", values="9.9.9.9")
    assert {row.ip for row in spark.sql("select * from tb_1").collect()} == {"1.2.3.4", "3.4.5.60"}

    discover_ip.delete_by_class(from_tables="*.default.tb_*", by_class="ip_v4", values="1.2.3.4", yes_i_am_sure=True)
    assert {row.ip for row in spark.sql("select * from tb_1").collect()} == {"3.4.5.60"}

    with pytest.raises(ValueError):
        discover_ip.delete_by_class(from_tables="*.default.tb_*", by_class="x")

    with pytest.raises(ValueError):
        discover_ip.delete_by_class(from_tables="*.default.tb_*", values="x")

    with pytest.raises(ValueError):
        discover_ip.delete_by_class(from_tables="*.default.tb_*", by_class=["ip"], values="x")

    with pytest.raises(ValueError):
        discover_ip.delete_by_class(from_tables="*.default.tb_*", by_class=True, values="x")

    with pytest.raises(ValueError):
        discover_ip.delete_by_class(from_tables="invalid from", by_class="email", values="x")


def test_discover_scan_result(discover_ip):
    assert not discover_ip.scan_result.empty

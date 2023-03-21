import pytest

from discoverx.dx import DX
from discoverx.dx import Scanner


def test_dx_instantiation(spark):

    dx = DX(spark=spark)
    assert dx.column_type_classification_threshold == 0.95

    # The validation should fail if threshold is outside of [0,1]
    with pytest.raises(ValueError) as e_threshold_error_plus:
        dx = DX(column_type_classification_threshold=1.4, spark=spark)

    with pytest.raises(ValueError) as e_threshold_error_minus:
        dx = DX(column_type_classification_threshold=-1.0, spark=spark)

    # simple test for displaying rules
    try:
        dx.display_rules()
    except Exception as display_rules_error:
        pytest.fail(f"Displaying rules failed with {display_rules_error}")


def test_msql(spark, monkeypatch):

    # apply the monkeypatch for the columns_table_name
    monkeypatch.setattr(Scanner, "COLUMNS_TABLE_NAME", "default.columns_mock")

    dx = DX(spark=spark)
    dx.scan(tables="tb_1", rules="ip_*")
    result = dx.msql("SELECT [ip_v4] as ip FROM *.*.*")

    assert {row.ip for row in result.collect()} == {"1.2.3.4", "3.4.5.60"}


def test_search(spark, monkeypatch):
    # apply the monkeypatch for the columns_table_name
    monkeypatch.setattr(Scanner, "COLUMNS_TABLE_NAME", "default.columns_mock")
    dx = DX(spark=spark)
    dx.scan(tables="tb_1", rules="ip_*")

    # search a specific term and auto-detect matching tags/rules
    result = dx.search("1.2.3.4")
    assert result.collect()[0].table_name == 'tb_1'

    # search all records for specific tag
    result_tags_only = dx.search(search_tags='ip_v4')
    assert {row.ip_v4 for row in result_tags_only.collect()} == {"1.2.3.4", "3.4.5.60"}

    # specify catalog, database and table
    result_tags_namespace = dx.search(search_tags='ip_v4', catalog="*", database="default", table="tb_*")
    assert {row.ip_v4 for row in result_tags_namespace.collect()} == {"1.2.3.4", "3.4.5.60"}

    # search specific term for list of specified tags
    result_term_tag = dx.search(search_term="3.4.5.60", search_tags=['ip_v4'])
    assert result_term_tag.collect()[0].table_name == 'tb_1'
    assert result_term_tag.collect()[0].ip_v4 == "3.4.5.60"

    with pytest.raises(ValueError) as no_tags_no_terms_error:
        dx.search()
    assert no_tags_no_terms_error.value.args[0] == "Neither search_term nor search_tags have been provided. At least one of them need to be specified."

    with pytest.raises(ValueError) as list_with_ints:
        dx.search(search_tags=[1, 3, 'ip'])
    assert list_with_ints.value.args[0] == "The provided search_tags [1, 3, 'ip'] have the wrong type. Please provide either a str or List[str]."

    with pytest.raises(ValueError) as single_bool:
        dx.search(search_tags=True)
    assert single_bool.value.args[0] == "The provided search_tags True have the wrong type. Please provide either a str or List[str]."


def test_msql_what_if(spark, monkeypatch):

    # apply the monkeypatch for the columns_table_name
    monkeypatch.setattr(Scanner, "COLUMNS_TABLE_NAME", "default.columns_mock")

    dx = DX(spark=spark)
    dx.scan(tables="tb_1", rules="ip_*")
    try:
        result = dx.msql("SELECT [ip_v4] as ip FROM *.*.*", what_if=True)
    except Exception as e:
        pytest.fail(f"Test failed with exception {e}")

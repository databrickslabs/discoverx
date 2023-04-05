import pytest

from discoverx.dx import DX
from discoverx import logging

logger = logging.Logging()


@pytest.fixture(scope="module", name="dx_ip")
def scan_ip_in_tb1(spark, mock_uc_functionality):
    dx = DX(spark=spark, classification_table_name="_discoverx.tags")
    dx.scan(tables="tb_1", rules="ip_*")
    dx.publish()
    yield dx

    spark.sql("DROP TABLE IF EXISTS _discoverx.tags")


def test_dx_instantiation(spark):

    dx = DX(spark=spark)
    assert dx.classification_threshold == 0.95

    # The validation should fail if threshold is outside of [0,1]
    with pytest.raises(ValueError) as e_threshold_error_plus:
        dx = DX(classification_threshold=1.4, spark=spark)

    with pytest.raises(ValueError) as e_threshold_error_minus:
        dx = DX(classification_threshold=-1.0, spark=spark)

    # simple test for displaying rules
    try:
        dx.display_rules()
    except Exception as display_rules_error:
        pytest.fail(f"Displaying rules failed with {display_rules_error}")


def test_scan_and_msql(spark, dx_ip):
    """
    End-2-end test involving both scanning and searching
    """

    result = dx_ip._msql("SELECT [ip_v4] as ip FROM *.*.*").collect()

    assert {row.ip for row in result} == {"1.2.3.4", "3.4.5.60"}

    # test what-if
    try:
        _ = dx_ip._msql("SELECT [ip_v4] as ip FROM *.*.*", what_if=True)
    except Exception as e:
        pytest.fail(f"Test failed with exception {e}")

def test_search(spark, dx_ip):

    # search a specific term and auto-detect matching tags/rules
    result = dx_ip.search("1.2.3.4").collect()
    assert result[0].table == 'tb_1'
    assert result[0].search_result.ip_v4.column == 'ip'

    # search all records for specific tag
    result_tags_only = dx_ip.search(by_tags='ip_v4')
    assert {row.search_result.ip_v4.value for row in result_tags_only.collect()} == {"1.2.3.4", "3.4.5.60"}

    # specify catalog, database and table
    result_tags_namespace = dx_ip.search(by_tags='ip_v4', catalog="*", database="default", table="tb_*")
    assert {row.search_result.ip_v4.value for row in result_tags_namespace.collect()} == {"1.2.3.4", "3.4.5.60"}

    # search specific term for list of specified tags
    result_term_tag = dx_ip.search(search_term="3.4.5.60", by_tags=['ip_v4']).collect()
    assert result_term_tag[0].table == 'tb_1'
    assert result_term_tag[0].search_result.ip_v4.value == "3.4.5.60"

    with pytest.raises(ValueError) as no_tags_no_terms_error:
        dx_ip.search()
    assert no_tags_no_terms_error.value.args[0] == "Neither search_term nor by_tags have been provided. At least one of them need to be specified."

    with pytest.raises(ValueError) as list_with_ints:
        dx_ip.search(by_tags=[1, 3, 'ip'])
    assert list_with_ints.value.args[0] == "The provided by_tags [1, 3, 'ip'] have the wrong type. Please provide either a str or List[str]."

    with pytest.raises(ValueError) as single_bool:
        dx_ip.search(by_tags=True)
    assert single_bool.value.args[0] == "The provided by_tags True have the wrong type. Please provide either a str or List[str]."


def test_select_by_tag(spark, dx_ip):

    # search a specific term and auto-detect matching tags/rules
    result = dx_ip.select_by_tags(from_tables="*.default.tb_*", by_tags="ip_v4").collect()
    assert result[0].table == 'tb_1'
    assert result[0].tagged_columns.ip_v4.column == 'ip'

    result = dx_ip.select_by_tags(from_tables="*.default.tb_*", by_tags=["ip_v4"]).collect()
    assert result[0].table == 'tb_1'
    assert result[0].tagged_columns.ip_v4.column == 'ip'

    with pytest.raises(ValueError):
        dx_ip.select_by_tags(from_tables="*.default.tb_*")
    
    with pytest.raises(ValueError):
        dx_ip.select_by_tags(from_tables="*.default.tb_*", by_tags=[1, 3, 'ip'])
    
    with pytest.raises(ValueError):
        dx_ip.select_by_tags(from_tables="*.default.tb_*", by_tags=True)

    with pytest.raises(ValueError):
        dx_ip.select_by_tags(from_tables="invalid from", by_tags="email")
    
# @pytest.mark.skip(reason="Delete is only working with v2 tables. Needs investigation")
def test_delete_by_tag(spark, dx_ip):

    # search a specific term and auto-detect matching tags/rules
    result = dx_ip.delete_by_tag(from_tables="*.default.tb_*", by_tag="ip_v4", values="9.9.9.9")
    assert result is None # Nothing should be executed

    result = dx_ip.delete_by_tag(from_tables="*.default.tb_*", by_tag="ip_v4", values="9.9.9.9", yes_i_am_sure=True).collect()
    assert result[0].table == 'tb_1'

    with pytest.raises(ValueError):
        dx_ip.delete_by_tag(from_tables="*.default.tb_*", by_tag="x")

    with pytest.raises(ValueError):
        dx_ip.delete_by_tag(from_tables="*.default.tb_*", values="x")
    
    with pytest.raises(ValueError):
        dx_ip.delete_by_tag(from_tables="*.default.tb_*", by_tag=['ip'], values="x")
    
    with pytest.raises(ValueError):
        dx_ip.delete_by_tag(from_tables="*.default.tb_*", by_tag=True, values="x")

    with pytest.raises(ValueError):
        dx_ip.delete_by_tag(from_tables="invalid from", by_tag="email", values="x")
    

# test multiple tags
def test_search_multiple(spark, mock_uc_functionality):
    dx = DX(spark=spark, classification_table_name="_discoverx.tags")
    dx.scan(tables="tb_1", rules="*")
    dx.publish()

    # search a specific term and auto-detect matching tags/rules
    result = dx.search(by_tags=["ip_v4", "mac"])
    assert result.collect()[0].table == 'tb_1'
    assert result.collect()[0].search_result.ip_v4.column == 'ip'
    assert result.collect()[0].search_result.mac.column == 'mac'

    spark.sql("DROP TABLE IF EXISTS _discoverx.tags")

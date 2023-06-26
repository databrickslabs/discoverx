import pytest
from discoverx.dx import DX
from discoverx import logging

logger = logging.Logging()


@pytest.fixture(scope="module", name="dx_ip")
def scan_ip_in_tb1(spark, mock_uc_functionality):
    dx = DX(spark=spark, classification_table_name="_discoverx.classes")
    dx.scan(from_tables="*.*.tb_1", rules="ip_*")
    dx.save()
    yield dx

    spark.sql("DROP TABLE IF EXISTS _discoverx.classes")


def test_can_read_columns_table(spark):
    dx = DX(spark=spark, classification_table_name="_discoverx.classes")
    dx.COLUMNS_TABLE_NAME = "db.non_existent_table"
    dx.intro()
    assert dx._can_read_columns_table() == False


def test_scan_without_classification_table(spark, mock_uc_functionality):
    dx = DX(spark=spark, classification_table_name="_discoverx.non_existent_classes_table")
    dx.scan(from_tables="*.*.tb_1", rules="ip_*")

    assert len(dx.scan_result()) > 0


def test_scan_withno_results(spark, mock_uc_functionality):
    dx = DX(spark=spark, classification_table_name="_discoverx.non_existent_classes_table")
    dx.scan(from_tables="*.*.tb_1", rules="credit_card_number")

    assert len(dx.scan_result()) > 0


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


def test_search(spark, dx_ip: DX):

    # search a specific term and auto-detect matching classes/rules
    result = dx_ip.search("1.2.3.4").collect()
    assert result[0].table == "tb_1"
    assert result[0].search_result.ip_v4.column == "ip"

    # specify catalog, schema and table
    result_classes_namespace = dx_ip.search("1.2.3.4", by_class="ip_v4", from_tables="*.default.tb_*")
    assert {row.search_result.ip_v4.value for row in result_classes_namespace.collect()} == {"1.2.3.4"}

    with pytest.raises(ValueError) as no_search_term_error:
        dx_ip.search(None)
    assert no_search_term_error.value.args[0] == "search_term has not been provided."

    with pytest.raises(ValueError) as no_inferred_class_error:
        dx_ip.search("###")
    assert (
        no_inferred_class_error.value.args[0]
        == "Could not infer any class for the given search term. Please specify the by_class parameter."
    )

    with pytest.raises(ValueError) as single_bool:
        dx_ip.search("", by_class=True)
    assert single_bool.value.args[0] == "The provided by_class True must be of string type."


def test_select_by_class(spark, dx_ip):

    # search a specific term and auto-detect matching classes/rules
    result = dx_ip.select_by_classes(from_tables="*.default.tb_*", by_classes="ip_v4").collect()
    assert result[0].table == "tb_1"
    assert result[0].classified_columns.ip_v4.column == "ip"

    result = dx_ip.select_by_classes(from_tables="*.default.tb_*", by_classes=["ip_v4"]).collect()
    assert result[0].table == "tb_1"
    assert result[0].classified_columns.ip_v4.column == "ip"

    with pytest.raises(ValueError):
        dx_ip.select_by_classes(from_tables="*.default.tb_*")

    with pytest.raises(ValueError):
        dx_ip.select_by_classes(from_tables="*.default.tb_*", by_classes=[1, 3, "ip"])

    with pytest.raises(ValueError):
        dx_ip.select_by_classes(from_tables="*.default.tb_*", by_classes=True)

    with pytest.raises(ValueError):
        dx_ip.select_by_classes(from_tables="invalid from", by_classes="email")


# @pytest.mark.skip(reason="Delete is only working with v2 tables. Needs investigation")
def test_delete_by_class(spark, dx_ip):

    # search a specific term and auto-detect matching classes/rules
    result = dx_ip.delete_by_class(from_tables="*.default.tb_*", by_class="ip_v4", values="9.9.9.9")
    assert result is None  # Nothing should be executed

    result = dx_ip.delete_by_class(from_tables="*.default.tb_*", by_class="ip_v4", values="9.9.9.9", yes_i_am_sure=True)
    assert result["table"][0] == "tb_1"

    with pytest.raises(ValueError):
        dx_ip.delete_by_class(from_tables="*.default.tb_*", by_class="x")

    with pytest.raises(ValueError):
        dx_ip.delete_by_class(from_tables="*.default.tb_*", values="x")

    with pytest.raises(ValueError):
        dx_ip.delete_by_class(from_tables="*.default.tb_*", by_class=["ip"], values="x")

    with pytest.raises(ValueError):
        dx_ip.delete_by_class(from_tables="*.default.tb_*", by_class=True, values="x")

    with pytest.raises(ValueError):
        dx_ip.delete_by_class(from_tables="invalid from", by_class="email", values="x")


def test_scan_result(dx_ip):
    assert not dx_ip.scan_result().empty


def test_scan_results_before_scan_should_fail(spark):
    dx = DX(spark=spark)
    with pytest.raises(Exception):
        dx.scan_result()

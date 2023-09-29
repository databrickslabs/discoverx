import pandas as pd
import pytest
from discoverx.dx import DX
from discoverx import logging
from pyspark.sql.functions import col

logger = logging.Logging()


@pytest.fixture(scope="module", name="dx_ip")
def scan_ip_in_tb1(spark, mock_uc_functionality):
    dx = DX(spark=spark)
    dx.scan(from_tables="*.*.tb_1", rules="ip_*")
    return dx


def test_can_read_columns_table(spark):
    dx = DX(spark=spark)
    dx.COLUMNS_TABLE_NAME = "db.non_existent_table"
    dx.intro()
    assert dx._can_read_columns_table() == False


def test_scan_special_caracters_col_name(spark, mock_uc_functionality):
    dx = DX(spark=spark)
    dx.scan(from_tables="*.*.tb_2", rules="ip_*")

    assert len(dx.scan_result) > 0

    result = dx.search("1.2.3.5", from_tables="*.*.tb_2").collect()
    assert len(result) > 0


def test_scan_without_classification_table(spark, mock_uc_functionality):
    dx = DX(spark=spark)
    dx.scan(from_tables="*.*.tb_1", rules="ip_*")

    assert len(dx.scan_result) > 0


def test_scan_withno_results(spark, mock_uc_functionality):
    dx = DX(spark=spark)
    dx.scan(from_tables="*.*.tb_1", rules="credit_card_number")

    assert len(dx.scan_result) > 0


def test_dx_instantiation(spark):
    dx = DX(spark=spark)

    # simple test for displaying rules
    try:
        dx.display_rules()
    except Exception as display_rules_error:
        pytest.fail(f"Displaying rules failed with {display_rules_error}")


def test_sql_template(spark):
    dx = DX(spark=spark)
    result = (
        dx.from_tables("*.*.*")
        .having_columns("id")
        .with_sql("SELECT 1 AS a FROM {full_table_name} WHERE id = 1")
        .apply()
        .count()
    )

    assert result > 0


def test_sql_template_fails_for_incorrect_sql(spark):
    dx = DX(spark=spark)

    with pytest.raises(Exception) as no_search_term_error:
        dx.from_tables("*.*.*").with_sql("Not-a-SQL-query").apply()
    assert no_search_term_error.value.args[0] == "No SQL statements were successfully executed."


def test_unpivot_string_columns(spark):
    dx = DX(spark=spark)

    df = dx.from_tables("*.*.*").unpivot_string_columns().apply()

    assert df.filter(col("column_name") == "ip.v2").count() > 1


def test_unpivot_string_columns_with_sampling(spark):
    dx = DX(spark=spark)

    df = dx.from_tables("*.*.*").unpivot_string_columns(sample_size=1).apply()

    assert df.filter(col("column_name") == "ip.v2").count() == 1


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
    assert result[0].table_name == "tb_1"
    assert result[0].search_result.ip_v4.column_name == "ip"

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
    assert result[0].table_name == "tb_1"
    assert result[0].classified_columns.ip_v4.column_name == "ip"

    result = dx_ip.select_by_classes(from_tables="*.default.tb_*", by_classes=["ip_v4"]).collect()
    assert result[0].table_name == "tb_1"
    assert result[0].classified_columns.ip_v4.column_name == "ip"

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
    dx_ip.delete_by_class(from_tables="*.default.tb_*", by_class="ip_v4", values="9.9.9.9")
    assert {row.ip for row in spark.sql("select * from tb_1").collect()} == {"1.2.3.4", "3.4.5.60"}

    dx_ip.delete_by_class(from_tables="*.default.tb_*", by_class="ip_v4", values="1.2.3.4", yes_i_am_sure=True)
    assert {row.ip for row in spark.sql("select * from tb_1").collect()} == {"3.4.5.60"}

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
    assert not dx_ip.scan_result.empty


def test_scan_results_before_scan_should_fail(spark):
    dx = DX(spark=spark)
    with pytest.raises(Exception):
        dx.scan_result()


def test_save_and_load_scan_result(spark, dx_ip):
    scan_result_table = "_discoverx.classes_save_test"
    dx_ip.save(full_table_name=scan_result_table)
    result = (
        spark.sql(f"select * from {scan_result_table}")
        .toPandas()
        .drop("effective_timestamp", axis=1)
        .sort_values(by=["column_name", "class_name"])
    )
    expected = pd.DataFrame(
        [
            ["None", "default", "tb_1", "description", "ip_v4", 0.0],
            ["None", "default", "tb_1", "description", "ip_v6", 0.0],
            ["None", "default", "tb_1", "ip", "ip_v4", 1.0],
            ["None", "default", "tb_1", "ip", "ip_v6", 0.0],
            ["None", "default", "tb_1", "mac", "ip_v4", 0.0],
            ["None", "default", "tb_1", "mac", "ip_v6", 0.0],
        ],
        columns=["table_catalog", "table_schema", "table_name", "column_name", "class_name", "score"],
    )
    assert result.reset_index(drop=True).equals(expected)

    # now load the saved results
    dx = DX(spark=spark)
    dx.load(full_table_name=scan_result_table)
    assert dx.scan_result.sort_values(by=["column_name", "class_name"]).reset_index(drop=True).equals(expected)

    with pytest.raises(Exception):
        dx.load(full_table_name="xxx")

    # now test merge with updates
    updated_df = pd.DataFrame(
        [
            ["None", "default", "tb_1", "description", "ip_v4", 0.0],
            ["None", "default", "tb_1", "description", "ip_v6", 0.0],
            ["None", "default", "tb_1", "ip", "ip_v4", 1.0],
            ["None", "default", "tb_1", "ip", "ip_v6", 2.0],
            ["None", "default", "tb_1", "mac", "ip_v4", 3.0],
            ["None", "default", "tb_1", "mac", "ip_v6", 0.0],
        ],
        columns=["table_catalog", "table_schema", "table_name", "column_name", "class_name", "score"],
    )
    dx_ip.scan_result.update(updated_df)
    dx_ip.save(full_table_name=scan_result_table)
    updated_result = (
        spark.sql(f"select * from {scan_result_table}")
        .toPandas()
        .drop("effective_timestamp", axis=1)
        .sort_values(by=["column_name", "class_name"])
    )
    assert updated_result.reset_index(drop=True).equals(updated_df)

    spark.sql(f"DROP TABLE IF EXISTS {scan_result_table}")

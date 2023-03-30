# pylint: disable=missing-function-docstring, missing-module-docstring

from dataclasses import dataclass
import pandas as pd
import pytest
from discoverx.common.helper import strip_margin
from discoverx.scanner import ColumnInfo, TableInfo
from discoverx.msql import Msql, SQLRow
from discoverx.scanner import ScanResult

@dataclass
class MockScanner:
    scan_result: ScanResult


@pytest.fixture(scope="module")
def classification_df(spark) -> pd.DataFrame:
    return pd.DataFrame([
        ["c", "db", "tb1", "email_1", "dx_email", True, "active"],
        ["c", "db", "tb1", "email_2", "dx_email", True, "active"],
        ["c", "db", "tb1", "date", "dx_date_partition", True, "active"],
        ["c", "db", "tb2", "email_3", "dx_email", True, "active"],
        ["c", "db", "tb2", "date", "dx_date_partition", True, "active"],
        ["c", "db2", "tb3", "email_4", "dx_email", True, "active"],
        ["c", "db", "tb1", "description", "any_number", True, "active"],  # any_number not in the tag list
        ["m_c", "db", "tb1", "email_3", "dx_email", True, "active"],  # catalog does not match
        ["c", "m_db", "tb1", "email_4", "dx_email", True, "active"],  # database does not match
        ["c", "db", "m_tb1", "email_5", "dx_email", True, "active"],  # table does not match
    ], columns=["catalog", "database", "table", "column", "rule_name", "current", "tag_status"])

columns = [
    ColumnInfo("id", "number", None, ["id"]),
    ColumnInfo("email_1", "string", None, ["dx_email", "dx_pii"]),
    ColumnInfo("email_2", "string", None, ["dx_email"]),
    ColumnInfo("date", "string", 1, ["dx_date_partition"]),
]
table_info = TableInfo("catalog", "prod_db1", "tb1", columns)

def test_msql_extracts_command():
    assert Msql("SELECT [dx_pii] AS pii FROM *.*.*").command == "SELECT"
    assert Msql("select [dx_pii] AS pii FROM *.*.*").command == "SELECT"
    assert Msql("   SELECT * FROM *.*.*").command == "SELECT"
    assert Msql("DELETE FROM *.*.*").command == "DELETE"
    assert Msql("delete FROM *.*.*").command == "DELETE"
    assert Msql("  DELETE FROM *.*.*").command == "DELETE"

def test_msql_validates_command():
    with pytest.raises(ValueError):
        Msql("INSERT INTO *.*.*")
    with pytest.raises(ValueError):
        Msql("DROP  ")
    with pytest.raises(ValueError):
        Msql("ALTER  ")
    with pytest.raises(ValueError):
        Msql("UPDATE  ")
    with pytest.raises(ValueError):
        Msql("CREATE  ")
    with pytest.raises(ValueError):
        Msql("anythingelse  ")

def test_msql_replace_from_clausole():
    msql = "SELECT [dx_pii] AS dx_pii FROM *.*.*"

    expected = SQLRow(
        "catalog",
        "prod_db1",
        "tb1",
        "SELECT email_1 AS dx_pii FROM catalog.prod_db1.tb1"
        )

    actual = Msql(msql).compile_msql(table_info)
    assert len(actual) == 1
    assert actual[0] == expected

def test_msql_select_single_tag():
    msql = "SELECT [dx_pii] AS pii FROM catalog.prod_db1.tb1"

    expected = SQLRow(
        "catalog",
        "prod_db1",
        "tb1",
        "SELECT email_1 AS pii FROM catalog.prod_db1.tb1"
    )

    actual = Msql(msql).compile_msql(table_info)
    assert len(actual) == 1
    assert actual[0] == expected

def test_msql_select_repeated_tag():
    msql = "SELECT [dx_email] AS email FROM catalog.prod_db1.tb1"

    actual = Msql(msql).compile_msql(table_info)
    assert len(actual) == 2
    assert actual[0] == SQLRow("catalog", "prod_db1", "tb1", "SELECT email_1 AS email FROM catalog.prod_db1.tb1")
    assert actual[1] == SQLRow("catalog", "prod_db1", "tb1", "SELECT email_2 AS email FROM catalog.prod_db1.tb1")

def test_msql_select_multi_tag():
    msql = """
    SELECT [dx_date_partition] AS dt, [dx_pii] AS pii, count([dx_pii]) AS cnt
    FROM catalog.prod_db1.tb1
    GROUP BY [dx_date_partition], [dx_pii]
    """

    expected = SQLRow(
        "catalog",
        "prod_db1",
        "tb1",
        strip_margin("""
            SELECT date AS dt, email_1 AS pii, count(email_1) AS cnt
            FROM catalog.prod_db1.tb1
            GROUP BY date, email_1
        """)
        )

    actual = Msql(msql).compile_msql(table_info)
    assert len(actual) == 1
    assert actual[0] == expected

def test_msql_select_multi_and_repeated_tag():
    msql = "SELECT [dx_email] AS email, [dx_date_partition] AS d FROM catalog.prod_db1.tb1 WHERE [dx_email] = 'a@b.c'"

    actual = Msql(msql).compile_msql(table_info)
    assert len(actual) == 2
    assert actual[0] == SQLRow("catalog", "prod_db1", "tb1", "SELECT email_1 AS email, date AS d FROM catalog.prod_db1.tb1 WHERE email_1 = 'a@b.c'")
    assert actual[1] == SQLRow("catalog", "prod_db1", "tb1", "SELECT email_2 AS email, date AS d FROM catalog.prod_db1.tb1 WHERE email_2 = 'a@b.c'")

def test_msql_build_select_multi_and_repeated_tag(spark, classification_df):
    msql = "SELECT [dx_email] AS email, [dx_date_partition] AS d FROM c.d*.t* WHERE [dx_email] = 'a@b.c'"
    expected_1 = SQLRow(
        "c",
        "db",
        "tb1",
        strip_margin("""
                SELECT email_1 AS email, date AS d FROM c.db.tb1 WHERE email_1 = 'a@b.c'
            """))

    expected_2 = SQLRow(
        "c",
        "db",
        "tb1",
        strip_margin("""
                SELECT email_2 AS email, date AS d FROM c.db.tb1 WHERE email_2 = 'a@b.c'
            """))

    expected_3 = SQLRow(
        "c",
        "db",
        "tb2",
        strip_margin("""
                SELECT email_3 AS email, date AS d FROM c.db.tb2 WHERE email_3 = 'a@b.c'
            """))

    actual = Msql(msql).build(classification_df)
    assert len(actual) == 3
    assert actual[0] == expected_1
    assert actual[1] == expected_2
    assert actual[2] == expected_3

def test_msql_build_delete_multi_and_repeated_tag(spark, classification_df):
    msql = "DELETE FROM c.d*.t* WHERE [dx_email] = 'a@b.c'"
    actual = Msql(msql).build(classification_df)

    assert len(actual) == 4
    assert actual[0] == SQLRow("c", "db", "tb1", "DELETE FROM c.db.tb1 WHERE email_1 = 'a@b.c'")
    assert actual[1] == SQLRow("c", "db", "tb1", "DELETE FROM c.db.tb1 WHERE email_2 = 'a@b.c'")
    assert actual[2] == SQLRow("c", "db", "tb2", "DELETE FROM c.db.tb2 WHERE email_3 = 'a@b.c'")
    assert actual[3] == SQLRow("c", "db2", "tb3", "DELETE FROM c.db2.tb3 WHERE email_4 = 'a@b.c'")

def test_msql_delete_command():
    msql = "DELETE FROM *.*.* WHERE [dx_email] = 'a@b.c'"

    actual = Msql(msql).compile_msql(table_info)
    assert len(actual) == 2
    assert actual[0] == SQLRow("catalog", "prod_db1", "tb1", "DELETE FROM catalog.prod_db1.tb1 WHERE email_1 = 'a@b.c'")
    assert actual[1] == SQLRow("catalog", "prod_db1", "tb1", "DELETE FROM catalog.prod_db1.tb1 WHERE email_2 = 'a@b.c'")

def test_execute_sql_rows(spark):
    msql = Msql("SELECT description FROM *.*.* ")
    sql_rows = [
        SQLRow(None, "default", "tb_1", "SELECT description FROM default.tb_1"),
    ]
    df = msql.execute_sql_rows(sqls=sql_rows, spark=spark)
    assert df.count() == 2

def test_execute_sql_rows_should_not_fail(spark):
    msql = Msql("SELECT description FROM *.*.* ")
    sql_rows = [
        SQLRow(None, "default", "tb_1", "SELECT description FROM default.tb_1"),
        SQLRow(None, "default", "non_existent_table", "SELECT description FROM default.non_existent_table"),
    ]
    df = msql.execute_sql_rows(sqls=sql_rows, spark=spark)
    assert df.count() == 2

# TODO: pyspark.sql.utils.AnalysisException: DELETE is only supported with v2 tables.
# def test_execute_delete_should_add_sql_column(spark):
#     msql = Msql("DELETE FROM *.*.* WHERE ip = '0.0.0.0'")
#     sql_rows = [
#         SQLRow(None, "default", "tb_1", "DELETE FROM default.tb_1 WHERE ip = '0.0.0.0'")
#     ]
#     df = msql.execute_sql_rows(sqls=sql_rows, spark=spark).select("sql")
#     assert df.count() == 1

def test_execute_sql_should_fail_for_no_successful_queries(spark):
    msql = Msql("SELECT description FROM *.*.* ")
    sql_rows = [
        SQLRow(None, "default", "tb_1", "SELECT non_existent_column FROM default.tb_1"), # Column does not exist
        SQLRow(None, "default", "non_existent_table_2", "SELECT description FROM default.non_existent_table_2"),
    ]
    with pytest.raises(ValueError):
        df = msql.execute_sql_rows(sqls=sql_rows, spark=spark)


# def test_msql_replace_tag_fails_for_missing_alias_in_select():
#     msql = "SELECT [dx_pii] FROM x.y WHERE [dx_pii] = ''"
#     with pytest.raises(ValueError):
#         SqlBuilder()._replace_tag(msql, 'dx_pii', 'email_1')

def test_validate_from_components():
    assert (Msql.validate_from_components("c.d.t") == ("c", "d", "t"))
    assert (Msql.validate_from_components("*.*.*") == ("*", "*", "*"))

    with pytest.raises(ValueError):
        Msql.validate_from_components("c.d")

    with pytest.raises(ValueError):
        Msql.validate_from_components(" c.d.t")

    with pytest.raises(ValueError):
        Msql.validate_from_components("c.d.t ")

    with pytest.raises(ValueError):
        Msql.validate_from_components("c. d.t")


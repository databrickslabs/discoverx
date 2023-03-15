# pylint: disable=missing-function-docstring, missing-module-docstring

import pandas as pd
import pytest
from discoverx.common.helper import strip_margin
from discoverx.config import ColumnInfo, TableInfo
from discoverx.msql import Msql


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
    msql = "SELECT [dx_pii] AS pii FROM *.*.*"

    expected = """
    SELECT email_1 AS pii FROM catalog.prod_db1.tb1
    """

    actual = Msql(msql).compile_msql(table_info)
    assert actual == strip_margin(expected)

def test_msql_select_single_tag():
    msql = "SELECT [dx_pii] AS pii FROM catalog.prod_db1.tb1"

    expected = """
    SELECT email_1 AS pii FROM catalog.prod_db1.tb1
    """

    actual = Msql(msql).compile_msql(table_info)
    assert actual == strip_margin(expected)

def test_msql_select_literal_keys():
    msql = "SELECT {catalog_name}, {database_name}, {table_name} FROM *.*.*"

    expected = """
    SELECT 'catalog' AS catalog_name, 'prod_db1' AS database_name, 'tb1' AS table_name FROM catalog.prod_db1.tb1
    """

    actual = Msql(msql).compile_msql(table_info)
    assert actual == strip_margin(expected)

def test_msql_select_repeated_tag():
    msql = "SELECT [dx_email] AS email FROM catalog.prod_db1.tb1"

    expected = """
    SELECT email_1 AS email FROM catalog.prod_db1.tb1
    UNION ALL
    SELECT email_2 AS email FROM catalog.prod_db1.tb1
    """

    actual = Msql(msql).compile_msql(table_info)
    assert actual == strip_margin(expected)

def test_msql_select_multi_tag():
    msql = """
    SELECT [dx_date_partition] AS dt, [dx_pii] AS pii, count([dx_pii]) AS cnt
    FROM catalog.prod_db1.tb1
    GROUP BY [dx_date_partition], [dx_pii]
    """

    expected = """
    SELECT date AS dt, email_1 AS pii, count(email_1) AS cnt
    FROM catalog.prod_db1.tb1
    GROUP BY date, email_1
    """

    actual = Msql(msql).compile_msql(table_info)
    assert actual == strip_margin(expected)

def test_msql_select_multi_and_repeated_tag():
    msql = "SELECT [dx_email] AS email, [dx_date_partition] AS d FROM catalog.prod_db1.tb1 WHERE [dx_email] = 'a@b.c'"

    expected = """
    SELECT email_1 AS email, date AS d FROM catalog.prod_db1.tb1 WHERE email_1 = 'a@b.c'
    UNION ALL
    SELECT email_2 AS email, date AS d FROM catalog.prod_db1.tb1 WHERE email_2 = 'a@b.c'
    """

    actual = Msql(msql).compile_msql(table_info)
    assert actual == strip_margin(expected)

def test_msql_build_select_multi_and_repeated_tag():
    msql = "SELECT [dx_email] AS email, [dx_date_partition] AS d FROM c.d*.t* WHERE [dx_email] = 'a@b.c'"
    df = pd.DataFrame([
        ["c", "db", "tb1", "email_1", "dx_email", 0.99],
        ["c", "db", "tb1", "email_2", "dx_email", 1.0],
        ["c", "db", "tb1", "date", "dx_date_partition", 1],
        # The next rows should be ignored
        ["c", "db", "tb1", "some_col", "dx_email", 0.5], # Threshold too low
        ["c", "db", "tb1", "description", "any_number", 0.99], # any_number not in the tag list
        ["m_c", "db", "tb1", "email_3", "dx_email", 0.99], # catalog does not match
        ["c", "m_db", "tb1", "email_4", "dx_email", 0.99], # database does not match
        ["c", "db", "m_tb1", "email_5", "dx_email", 0.99], # table does not match
    ], columns = ["catalog", "database", "table", "column", "rule_name", "frequency"])

    expected = """
    SELECT email_1 AS email, date AS d FROM c.db.tb1 WHERE email_1 = 'a@b.c'
    UNION ALL
    SELECT email_2 AS email, date AS d FROM c.db.tb1 WHERE email_2 = 'a@b.c'
    """

    actual = Msql(msql).build(df, 0.95)
    assert actual == strip_margin(expected)

def test_msql_delete_command():
    msql = "DELETE FROM *.*.* WHERE [dx_email] = 'a@b.c'"

    expected = """
    DELETE FROM catalog.prod_db1.tb1 WHERE email_1 = 'a@b.c';
    DELETE FROM catalog.prod_db1.tb1 WHERE email_2 = 'a@b.c'
    """

    actual = Msql(msql).compile_msql(table_info)
    assert actual == strip_margin(expected)

# def test_msql_replace_tag_fails_for_missing_alias_in_select():
#     msql = "SELECT [dx_pii] FROM x.y WHERE [dx_pii] = ''" 
#     with pytest.raises(ValueError):
#         SqlBuilder()._replace_tag(msql, 'dx_pii', 'email_1')
        
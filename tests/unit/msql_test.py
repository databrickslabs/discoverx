# pylint: disable=missing-function-docstring, missing-module-docstring

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

# def test_msql_replace_tag_fails_for_missing_alias_in_select():
#     msql = "SELECT [dx_pii] FROM x.y WHERE [dx_pii] = ''" 
#     with pytest.raises(ValueError):
#         SqlBuilder()._replace_tag(msql, 'dx_pii', 'email_1')
        
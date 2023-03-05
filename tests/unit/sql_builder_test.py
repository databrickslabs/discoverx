# pylint: disable=missing-function-docstring, missing-module-docstring
import logging

from pyspark.sql import SparkSession
import pytest
from discoverx.common.helper import strip_margin

from discoverx.config import ColumnInfo, TableInfo
from discoverx.rules import Rule
from discoverx.sql_builder import SqlBuilder


def test_generate_sql():
    columns = [ColumnInfo("id", "number", False, []), ColumnInfo("name", "string", False, [])]
    table_info = TableInfo("meta", "db", "tb", columns)
    rules = [Rule(name="any_word", type="regex", description="Any word", definition=r"\w")]


    expected = r"""SELECT
    'meta' as catalog,
    'db' as database,
    'tb' as table,
    column,
    rule_name,
    (sum(value) / count(value)) as frequency
FROM
(
    SELECT column, stack(1, 'any_word', `any_word`) as (rule_name, value)
    FROM
    (
        SELECT
        column,
        INT(regexp_like(value, '\\w')) AS any_word
        FROM (
            SELECT
                stack(1, 'name', `name`) AS (column, value)
            FROM meta.db.tb
            TABLESAMPLE (100 ROWS)
        )
    )
)
GROUP BY catalog, database, table, column, rule_name"""

    actual = SqlBuilder().rule_matching_sql(table_info, rules, 100)

    logging.info("Generated SQL is: \n%s", actual)

    assert actual == expected


def test_generate_sql_multiple_rules():
    columns = [ColumnInfo("id", "number", False, []), ColumnInfo("name", "string", False, [])]
    table_info = TableInfo("meta", "db", "tb", columns)
    rules = [
        Rule(name="any_word", type="regex", description="Any word", definition=r"\w."),
        Rule(name="any_number", type="regex", description="Any number", definition=r"\d."),
    ]

    expected = r"""SELECT
    'meta' as catalog,
    'db' as database,
    'tb' as table,
    column,
    rule_name,
    (sum(value) / count(value)) as frequency
FROM
(
    SELECT column, stack(2, 'any_word', `any_word`, 'any_number', `any_number`) as (rule_name, value)
    FROM
    (
        SELECT
        column,
        INT(regexp_like(value, '\\w.')) AS any_word,
        INT(regexp_like(value, '\\d.')) AS any_number
        FROM (
            SELECT
                stack(1, 'name', `name`) AS (column, value)
            FROM meta.db.tb
            TABLESAMPLE (100 ROWS)
        )
    )
)
GROUP BY catalog, database, table, column, rule_name"""

    actual = SqlBuilder().rule_matching_sql(table_info, rules, 100)

    logging.info("Generated SQL is: \n%s", actual)

    assert actual == expected


def test_sql_runs(spark: SparkSession):
    columns = [
        ColumnInfo("id", "number", None, []),
        ColumnInfo("ip", "string", None, []),
        ColumnInfo("description", "string", None, []),
    ]
    table_info = TableInfo(None, "default", "tb_1", columns)
    rules = [
        Rule(name="any_word", type="regex", description="Any word", definition=r"\w+"),
        Rule(name="any_number", type="regex", description="Any number", definition=r"\d+"),
    ]

    actual = SqlBuilder().rule_matching_sql(table_info, rules, 100)

    logging.info("Generated SQL is: \n%s", actual)

    expected = spark.sql(actual).collect()

    print(expected)

columns = [
    ColumnInfo("id", "number", None, ["id"]),
    ColumnInfo("email_1", "string", None, ["dx_email", "dx_pii"]),
    ColumnInfo("email_2", "string", None, ["dx_email"]),
    ColumnInfo("date", "string", 1, ["dx_date_partition"]),
]
table_info = TableInfo("catalog", "prod_db1", "tb1", columns)

def test_msql_select_single_tag():
    msql = "SELECT [dx_pii] AS pii FROM catalog.prod_db1.tb1"

    expected = """
    SELECT email_1 AS pii FROM catalog.prod_db1.tb1
    """

    actual = SqlBuilder().compile_msql(msql, table_info)
    assert actual == strip_margin(expected)

def test_msql_select_repeated_tag():
    msql = "SELECT [dx_email] AS email FROM catalog.prod_db1.tb1"

    expected = """
    SELECT email_1 AS email FROM catalog.prod_db1.tb1
    UNION ALL
    SELECT email_2 AS email FROM catalog.prod_db1.tb1
    """

    actual = SqlBuilder().compile_msql(msql, table_info)
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

    actual = SqlBuilder().compile_msql(msql, table_info)
    assert actual == strip_margin(expected)


def test_msql_replace_tag():
    msql = "SELECT [dx_pii] AS pii FROM x.y WHERE [dx_pii] = 'abc@def.co'"

    expected = """
    SELECT email_1 AS pii FROM x.y WHERE email_1 = 'abc@def.co'
    """

    actual = SqlBuilder()._replace_tag(msql, 'dx_pii', 'email_1')
    assert actual == strip_margin(expected)

# def test_msql_replace_tag_fails_for_missing_alias_in_select():
#     msql = "SELECT [dx_pii] FROM x.y WHERE [dx_pii] = ''" 
#     with pytest.raises(ValueError):
#         SqlBuilder()._replace_tag(msql, 'dx_pii', 'email_1')
        
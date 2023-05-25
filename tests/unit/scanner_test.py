import logging
import pandas as pd
from pyspark.sql import SparkSession
import pytest

from discoverx.scanner import Scanner, ColumnInfo, TableInfo
from discoverx.rules import Rule, Rules


def test_get_table_list(spark):
    expected = [
        TableInfo(
            "hive_metastore",
            "default",
            "tb_all_types",
            [
                ColumnInfo("str_col", "STRING", None, []),
                ColumnInfo("int_col", "INT", None, []),
                ColumnInfo("double_col", "DOUBLE", None, []),
                ColumnInfo("timestamp_col", "TIMESTAMP", None, []),
                ColumnInfo("bool_col", "BOOLEAN", None, []),
                ColumnInfo("long_col", "LONG", None, []),
                ColumnInfo("null_col", "NULL", None, []),
                ColumnInfo("decimal_col", "DECIMAL", None, []),
                ColumnInfo("float_col", "FLOAT", None, []),
                ColumnInfo("map_col", "MAP", None, []),
                ColumnInfo("short_col", "SHORT", None, []),
                ColumnInfo("array_col", "ARRAY", None, []),
                ColumnInfo("date_col", "DATE", None, []),
                ColumnInfo("byte_col", "BYTE", None, []),
                ColumnInfo("struct_col", "STRUCT", None, []),
                ColumnInfo("binary_col", "BINARY", None, []),
                ColumnInfo("char_col", "CHAR", None, []),
                ColumnInfo("udt_col", "USER_DEFINED_TYPE", None, []),
                ColumnInfo("interval_col", "INTERVAL", None, []),
                ColumnInfo("str_part_col", "STRING", 1, []),
            ],
        )
    ]

    rules = Rules()
    MockedScanner = Scanner
    MockedScanner.COLUMNS_TABLE_NAME = "default.columns_mock"
    scanner = MockedScanner(
        spark, rules=rules, catalogs="*", databases="*", tables="*_all_types", rule_filter="*", sample_size=100
    )
    actual = scanner._get_list_of_tables()

    assert len(actual) == 1
    assert actual == expected


# test generating sql for single and multiple rules (using parametrized pytests)
expectedsingle = r"""SELECT
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
        INT(regexp_like(value, '\\w')) AS `any_word`
        FROM (
            SELECT
                stack(1, 'name', `name`) AS (column, value)
            FROM meta.db.tb
            TABLESAMPLE (100 ROWS)
        )
    )
)
GROUP BY catalog, database, table, column, rule_name"""

expectedmulti = r"""SELECT
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
        INT(regexp_like(value, '\\w.')) AS `any_word`,
        INT(regexp_like(value, '\\d.')) AS `any_number`
        FROM (
            SELECT
                stack(1, 'name', `name`) AS (column, value)
            FROM meta.db.tb
            TABLESAMPLE (100 ROWS)
        )
    )
)
GROUP BY catalog, database, table, column, rule_name"""


@pytest.mark.parametrize(
    "rules_input, expected",
    [
        ([Rule(name="any_word", type="regex", description="Any word", definition=r"\w")], expectedsingle),
        (
            [
                Rule(name="any_word", type="regex", description="Any word", definition=r"\w."),
                Rule(name="any_number", type="regex", description="Any number", definition=r"\d."),
            ],
            expectedmulti,
        ),
    ],
)
def test_generate_sql(spark, rules_input, expected):
    columns = [ColumnInfo("id", "number", False, []), ColumnInfo("name", "string", False, [])]
    table_info = TableInfo("meta", "db", "tb", columns)
    rules = rules_input

    rules = Rules(custom_rules=rules)
    MockedScanner = Scanner
    MockedScanner.COLUMNS_TABLE_NAME = "default.columns_mock"
    scanner = MockedScanner(spark, rules=rules, rule_filter="any_*", sample_size=100)
    actual = scanner._rule_matching_sql(table_info)
    logging.info("Generated SQL is: \n%s", actual)

    assert actual == expected


def test_sql_runs(spark):
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

    rules = Rules(custom_rules=rules)
    MockedScanner = Scanner
    MockedScanner.COLUMNS_TABLE_NAME = "default.columns_mock"
    scanner = MockedScanner(spark, rules=rules, rule_filter="any_*", sample_size=100)
    actual = scanner._rule_matching_sql(table_info)

    logging.info("Generated SQL is: \n%s", actual)

    expected = spark.sql(actual).collect()

    print(expected)


def test_scan_custom_rules(spark: SparkSession):
    expected = pd.DataFrame(
        [
            ["None", "default", "tb_1", "ip", "any_word", 0.0],
            ["None", "default", "tb_1", "ip", "any_number", 0.0],
            ["None", "default", "tb_1", "mac", "any_word", 0.0],
            ["None", "default", "tb_1", "mac", "any_number", 0.0],
            ["None", "default", "tb_1", "description", "any_word", 0.5],
            ["None", "default", "tb_1", "description", "any_number", 0.0],
        ],
        columns=["catalog", "database", "table", "column", "rule_name", "frequency"],
    )

    columns = [
        ColumnInfo("id", "number", False, []),
        ColumnInfo("ip", "string", False, []),
        ColumnInfo("description", "string", False, []),
    ]
    table_list = [TableInfo(None, "default", "tb_1", columns)]
    rules = [
        Rule(name="any_word", type="regex", description="Any word", definition=r"^\w*$"),
        Rule(name="any_number", type="regex", description="Any number", definition=r"^\d*$"),
    ]

    rules = Rules(custom_rules=rules)
    MockedScanner = Scanner
    MockedScanner.COLUMNS_TABLE_NAME = "default.columns_mock"
    scanner = MockedScanner(spark, rules=rules, tables="tb_1", rule_filter="any_*", sample_size=100)
    scanner.scan()

    logging.info("Scan result is: \n%s", scanner.scan_result.df)

    assert scanner.scan_result.df.equals(expected)


def test_scan(spark: SparkSession):
    expected = pd.DataFrame(
        [
            ["None", "default", "tb_1", "ip", "ip_v4", 1.0],
            ["None", "default", "tb_1", "ip", "ip_v6", 0.0],
            ["None", "default", "tb_1", "mac", "ip_v4", 0.0],
            ["None", "default", "tb_1", "mac", "ip_v6", 0.0],
            ["None", "default", "tb_1", "description", "ip_v4", 0.0],
            ["None", "default", "tb_1", "description", "ip_v6", 0.0],
        ],
        columns=["catalog", "database", "table", "column", "rule_name", "frequency"],
    )

    rules = Rules()
    MockedScanner = Scanner
    MockedScanner.COLUMNS_TABLE_NAME = "default.columns_mock"
    scanner = MockedScanner(spark, rules=rules, tables="tb_*", rule_filter="ip_*")
    scanner.scan()

    assert scanner.scan_result.df.equals(expected)

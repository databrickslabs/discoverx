from discoverx.sql.analyze_content import *
from discoverx.data_models import *

def test_generate_sql():

    columns = [
        ColumnInfo("id", "number", False),
        ColumnInfo("name", "string", False)
    ]
    table_info = TableInfo("meta", "db", "tb", columns)
    rules = [
        Rule("any_word", "regex", "Any word", "\w")
    ]

    expected = """SELECT
    'meta' as metastore,
    'db' as database,
    `tb` as table,
    column,
    rule_name,
    (sum(value) / count(value)) as frequency
FROM
(
    SELECT column, stack(1, 'any_word', any_word) as (rule_name, value)
    FROM
    (
        SELECT
        column,
        INT(regexp_like(value, '\w')) AS any_word
        FROM (
        SELECT
            stack(1, 'name', name) AS (column, value)
        FROM db.tb
        TABLESAMPLE (100 ROWS)
        )
    )
)"""
    actual = rule_matching_sql(table_info, rules, 100)

    print(actual)

    assert actual == expected


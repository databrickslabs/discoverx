from discoverx.data_models import *
from discoverx.common import trim_sql

def rule_matching_sql(
                        table_info: TableInfo,
                        rules: list[Rule],
                        sample_size: int = 1000):

    expressions = [r for r in rules if r.type == "regex"]
    cols = [c for c in table_info.columns if c.data_type == "string"]

    matchingColumns = [f"INT(regexp_like(value, '{r.definition}')) AS {r.name}" for r in expressions]
    matchingString = ",\n        ".join(matchingColumns)

    unpivotExpressions = ", ".join([f"'{r.name}', {r.name}" for r in expressions])
    unpivotColumns = ", ".join([f"'{c.name}', {c.name}" for c in cols])

    sql = f"""
        SELECT 
            '{table_info.metastore}' as metastore,
            '{table_info.database}' as database,
            `{table_info.table}` as table, 
            column,
            rule_name,
            (sum(value) / count(value)) as frequency
        FROM
        (
            SELECT column, stack({len(expressions)}, {unpivotExpressions}) as (rule_name, value)
            FROM 
            (
                SELECT
                column,
                {matchingString}
                FROM (
                SELECT
                    stack({len(cols)}, {unpivotColumns}) AS (column, value)
                FROM {table_info.database}.{table_info.table}
                TABLESAMPLE ({sample_size} ROWS)
                )
            )
        )
    """
    
    return trim_sql(sql)


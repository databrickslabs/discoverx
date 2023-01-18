from discoverx.config import *
from discoverx.common import trim_sql

class SqlBuilder:
    
    def rule_matching_sql(self, 
                        table_info: TableInfo,
                        rules: list[Rule],
                        sample_size: int = 1000):

        expressions = [r for r in rules if r.type == "regex"]
        cols = [c for c in table_info.columns if c.data_type == "string"]

        matching_columns = [f"INT(regexp_like(value, '{r.definition}')) AS {r.name}" for r in expressions]
        matching_string = ",\n                    ".join(matching_columns)

        unpivot_expressions = ", ".join([f"'{r.name}', {r.name}" for r in expressions])
        unpivot_columns = ", ".join([f"'{c.name}', {c.name}" for c in cols])

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
                SELECT column, stack({len(expressions)}, {unpivot_expressions}) as (rule_name, value)
                FROM 
                (
                    SELECT
                    column,
                    {matching_string}
                    FROM (
                    SELECT
                        stack({len(cols)}, {unpivot_columns}) AS (column, value)
                    FROM {table_info.database}.{table_info.table}
                    TABLESAMPLE ({sample_size} ROWS)
                    )
                )
            )
        """
        
        return trim_sql(sql)


